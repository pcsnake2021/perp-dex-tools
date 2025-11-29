"""
Lighter exchange client implementation.
"""

import os
import asyncio
import time
import logging
import traceback
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
from typing import Dict, Any, List, Optional, Tuple

from .base import BaseExchangeClient, OrderResult, OrderInfo, query_retry
from helpers.logger import TradingLogger

# Import official Lighter SDK for API client
import lighter
from lighter import SignerClient, ApiClient, Configuration

# Import custom WebSocket implementation
from .lighter_custom_websocket import LighterCustomWebSocketManager

# Suppress Lighter SDK debug logs
logging.getLogger('lighter').setLevel(logging.WARNING)
# Also suppress root logger DEBUG messages that might be coming from Lighter SDK
root_logger = logging.getLogger()
if root_logger.level == logging.DEBUG:
    root_logger.setLevel(logging.WARNING)


class LighterClient(BaseExchangeClient):
    """Lighter exchange client implementation."""

    def __init__(self, config: Dict[str, Any]):
        """Initialize Lighter client."""
        super().__init__(config)

        # Lighter credentials from environment
        self.api_key_private_key = os.getenv('API_KEY_PRIVATE_KEY')
        self.account_index = int(os.getenv('LIGHTER_ACCOUNT_INDEX', '0'))
        self.api_key_index = int(os.getenv('LIGHTER_API_KEY_INDEX', '0'))
        self.base_url = "https://mainnet.zklighter.elliot.ai"

        if not self.api_key_private_key:
            raise ValueError("API_KEY_PRIVATE_KEY must be set in environment variables")

        # Initialize logger
        self.logger = TradingLogger(exchange="lighter", ticker=self.config.ticker, log_to_console=False)
        self._order_update_handler = None

        # Initialize Lighter client (will be done in connect)
        self.lighter_client = None

        # Initialize API client (will be done in connect)
        self.api_client = None

        # Market configuration
        self.base_amount_multiplier = None
        self.price_multiplier = None
        self.min_order_quantity = None
        self.min_order_notional = None
        self.orders_cache = {}
        self.current_order_client_id = None
        self.current_order = None

    def _validate_config(self) -> None:
        """Validate Lighter configuration."""
        required_env_vars = ['API_KEY_PRIVATE_KEY', 'LIGHTER_ACCOUNT_INDEX', 'LIGHTER_API_KEY_INDEX']
        missing_vars = [var for var in required_env_vars if not os.getenv(var)]
        if missing_vars:
            raise ValueError(f"Missing required environment variables: {missing_vars}")

    async def _get_market_config(self, ticker: str) -> Tuple[int, int, int]:
        """Get market configuration for a ticker using official SDK."""
        try:
            # Use shared API client
            order_api = lighter.OrderApi(self.api_client)

            # Get order books to find market info
            order_books = await order_api.order_books()

            for market in order_books.order_books:
                if market.symbol == ticker:
                    market_id = market.market_id
                    base_multiplier = pow(10, market.supported_size_decimals)
                    price_multiplier = pow(10, market.supported_price_decimals)

                    # Capture additional market constraints if available
                    self.min_order_quantity = self._extract_market_numeric_attr(
                        market,
                        [
                            'minimum_quantity', 'minimum_size', 'min_size', 'min_quantity',
                            'min_order_size', 'minimum_order_size', 'minimal_size',
                            'min_trade_size', 'minimum_trade_size', 'minimum_base_quantity',
                            'minimum_base_amount', 'base_minimum', 'base_min',
                            'min_base_amount'
                        ]
                    )
                    self.min_order_notional = self._extract_market_numeric_attr(
                        market,
                        [
                            'minimum_value', 'min_value', 'minimum_quote', 'min_quote',
                            'minimum_notional', 'min_notional', 'minimum_quote_value',
                            'minimum_quote_amount', 'quote_minimum', 'quote_min',
                            'min_quote_amount'
                        ]
                    )

                    if self.min_order_quantity is None:
                        self.logger.log(
                            f"Lighter: No explicit minimum quantity found for {ticker}. "
                            "Proceeding with configured quantity.", "WARNING"
                        )
                    if self.min_order_notional is None:
                        self.logger.log(
                            f"Lighter: No explicit minimum notional found for {ticker}. "
                            "Assuming no additional notional constraint.", "WARNING"
                        )

                    interesting_attrs = {}
                    interesting_keywords = ['min', 'max', 'supported', 'step', 'limit']
                    for attr in dir(market):
                        if attr.startswith('_'):
                            continue
                        if any(keyword in attr.lower() for keyword in interesting_keywords):
                            value = getattr(market, attr, None)
                            if callable(value):
                                continue
                            interesting_attrs[attr] = value

                    if interesting_attrs:
                        attrs_preview = str(interesting_attrs)
                        if len(attrs_preview) > 1000:
                            attrs_preview = attrs_preview[:1000] + "..."
                        self.logger.log(
                            f"Lighter market attribute snapshot ({ticker}): {attrs_preview}",
                            "INFO"
                        )

                    self.logger.log(
                        f"Lighter market constraints ({ticker}) -> "
                        f"min_qty={self.min_order_quantity}, min_notional={self.min_order_notional}, "
                        f"size_decimals={market.supported_size_decimals}, "
                        f"price_decimals={market.supported_price_decimals}",
                        "INFO"
                    )

                    # Store market info for later use
                    self.config.market_info = market

                    self.logger.log(
                        f"Market config for {ticker}: ID={market_id}, "
                        f"Base multiplier={base_multiplier}, Price multiplier={price_multiplier}",
                        "INFO"
                    )
                    return market_id, base_multiplier, price_multiplier

            raise Exception(f"Ticker {ticker} not found in available markets")

        except Exception as e:
            self.logger.log(f"Error getting market config: {e}", "ERROR")
            raise

    async def _initialize_lighter_client(self):
        """Initialize the Lighter client using official SDK."""
        if self.lighter_client is None:
            try:
                self.lighter_client = SignerClient(
                    url=self.base_url,
                    private_key=self.api_key_private_key,
                    account_index=self.account_index,
                    api_key_index=self.api_key_index,
                )

                # Check client
                err = self.lighter_client.check_client()
                if err is not None:
                    raise Exception(f"CheckClient error: {err}")

                self.logger.log("Lighter client initialized successfully", "INFO")
            except Exception as e:
                self.logger.log(f"Failed to initialize Lighter client: {e}", "ERROR")
                raise
        return self.lighter_client

    async def connect(self) -> None:
        """Connect to Lighter."""
        try:
            # Initialize shared API client
            if self.api_client is None:
                self.api_client = ApiClient(configuration=Configuration(host=self.base_url))

            # Initialize Lighter client
            await self._initialize_lighter_client()

            # Add market config to config for WebSocket manager
            self.config.market_index = self.config.contract_id
            self.config.account_index = self.account_index
            self.config.lighter_client = self.lighter_client

            # Initialize WebSocket manager (using custom implementation)
            self.ws_manager = LighterCustomWebSocketManager(
                config=self.config,
                order_update_callback=self._handle_websocket_order_update
            )

            # Set logger for WebSocket manager
            self.ws_manager.set_logger(self.logger)

            # Start WebSocket connection in background task
            asyncio.create_task(self.ws_manager.connect())
            # Wait a moment for connection to establish
            await asyncio.sleep(2)

        except Exception as e:
            self.logger.log(f"Error connecting to Lighter: {e}", "ERROR")
            raise

    async def disconnect(self) -> None:
        """Disconnect from Lighter."""
        try:
            if hasattr(self, 'ws_manager') and self.ws_manager:
                await self.ws_manager.disconnect()

            # Close shared API client
            if self.api_client:
                await self.api_client.close()
                self.api_client = None
        except Exception as e:
            self.logger.log(f"Error during Lighter disconnect: {e}", "ERROR")

    def get_exchange_name(self) -> str:
        """Get the exchange name."""
        return "lighter"

    def setup_order_update_handler(self, handler) -> None:
        """Setup order update handler for WebSocket."""
        self._order_update_handler = handler

    def _handle_websocket_order_update(self, order_data_list: List[Dict[str, Any]]):
        """Handle order updates from WebSocket."""
        for order_data in order_data_list:
            if order_data['market_index'] != self.config.contract_id:
                continue

            side = 'sell' if order_data['is_ask'] else 'buy'
            if side == self.config.close_order_side:
                order_type = "CLOSE"
            else:
                order_type = "OPEN"

            order_id = order_data['order_index']
            status = order_data['status'].upper()
            filled_size = Decimal(order_data['filled_base_amount'])
            size = Decimal(order_data['initial_base_amount'])
            price = Decimal(order_data['price'])
            remaining_size = Decimal(order_data['remaining_base_amount'])

            if order_id in self.orders_cache.keys():
                if (self.orders_cache[order_id]['status'] == 'OPEN' and
                        status == 'OPEN' and
                        filled_size == self.orders_cache[order_id]['filled_size']):
                    continue
                elif status in ['FILLED', 'CANCELED']:
                    del self.orders_cache[order_id]
                else:
                    self.orders_cache[order_id]['status'] = status
                    self.orders_cache[order_id]['filled_size'] = filled_size
            elif status == 'OPEN':
                self.orders_cache[order_id] = {'status': status, 'filled_size': filled_size}

            if status == 'OPEN' and filled_size > 0:
                status = 'PARTIALLY_FILLED'

            if status == 'OPEN':
                self.logger.log(f"[{order_type}] [{order_id}] {status} "
                                f"{size} @ {price}", "INFO")
            else:
                self.logger.log(f"[{order_type}] [{order_id}] {status} "
                                f"{filled_size} @ {price}", "INFO")

            if order_data['client_order_index'] == self.current_order_client_id or order_type == 'OPEN':
                current_order = OrderInfo(
                    order_id=order_id,
                    side=side,
                    size=size,
                    price=price,
                    status=status,
                    filled_size=filled_size,
                    remaining_size=remaining_size,
                    cancel_reason=''
                )
                self.current_order = current_order

            if status in ['FILLED', 'CANCELED']:
                self.logger.log_transaction(order_id, side, filled_size, price, status)

    @query_retry(default_return=(0, 0))
    async def fetch_bbo_prices(self, contract_id: str) -> Tuple[Decimal, Decimal]:
        """Get orderbook using official SDK."""
        # Use WebSocket data if available
        if (hasattr(self, 'ws_manager') and
                self.ws_manager.best_bid and self.ws_manager.best_ask):
            best_bid = Decimal(str(self.ws_manager.best_bid))
            best_ask = Decimal(str(self.ws_manager.best_ask))

            if best_bid <= 0 or best_ask <= 0 or best_bid >= best_ask:
                self.logger.log("Invalid bid/ask prices", "ERROR")
                raise ValueError("Invalid bid/ask prices")
        else:
            self.logger.log("Unable to get bid/ask prices from WebSocket.", "ERROR")
            raise ValueError("WebSocket not running. No bid/ask prices available")

        return best_bid, best_ask

    async def _submit_order_with_retry(self, order_params: Dict[str, Any]) -> OrderResult:
        """Submit an order with Lighter using official SDK."""
        # Ensure client is initialized
        if self.lighter_client is None:
            # This is a sync method, so we need to handle this differently
            # For now, raise an error if client is not initialized
            raise ValueError("Lighter client not initialized. Call connect() first.")

        # Create order using official SDK
        create_order, tx_hash, error = await self.lighter_client.create_order(**order_params)
        if error is not None:
            return OrderResult(
                success=False, order_id=str(order_params['client_order_index']),
                error_message=f"Order creation error: {error}")

        else:
            return OrderResult(success=True, order_id=str(order_params['client_order_index']))

    async def place_limit_order(self, contract_id: str, quantity: Decimal, price: Decimal,
                                side: str) -> OrderResult:
        """Place a post only order with Lighter using official SDK."""
        # Ensure client is initialized
        if self.lighter_client is None:
            await self._initialize_lighter_client()

        # Ensure market config has been loaded
        if self.base_amount_multiplier is None or self.price_multiplier is None:
            raise ValueError("Market configuration missing. Call get_contract_attributes() first.")

        # Normalize quantity to the supported precision
        quantity = self._normalize_quantity(quantity)

        # Determine order side and price
        if side.lower() == 'buy':
            is_ask = False
        elif side.lower() == 'sell':
            is_ask = True
        else:
            raise Exception(f"Invalid side: {side}")

        # Generate unique client order index
        client_order_index = int(time.time() * 1000) % 1000000  # Simple unique ID
        self.current_order_client_id = client_order_index

        # Ensure price respects tick size
        price = self.round_to_tick(price)

        # Ensure order notional meets exchange requirements
        quantity = self._ensure_min_notional(quantity, price)

        base_mult = Decimal(self.base_amount_multiplier)
        price_mult = Decimal(self.price_multiplier)

        base_amount = (quantity * base_mult).to_integral_value(rounding=ROUND_HALF_UP)
        price_amount = (price * price_mult).to_integral_value(rounding=ROUND_HALF_UP)

        if base_amount <= 0:
            raise ValueError(f"Quantity {quantity} is below minimum tradable size on Lighter.")
        if price_amount <= 0:
            raise ValueError(f"Price {price} is invalid for current tick size.")

        self.logger.log(
            f"Lighter order payload -> side: {side}, qty: {quantity} (normalized), "
            f"price: {price}, base_amount: {base_amount}, price_amount: {price_amount}, "
            f"base_mult: {self.base_amount_multiplier}, price_mult: {self.price_multiplier}",
            "INFO"
        )

        # Create order parameters
        order_params = {
            'market_index': self.config.contract_id,
            'client_order_index': client_order_index,
            'base_amount': int(base_amount),
            'price': int(price_amount),
            'is_ask': is_ask,
            'order_type': self.lighter_client.ORDER_TYPE_LIMIT,
            'time_in_force': self.lighter_client.ORDER_TIME_IN_FORCE_GOOD_TILL_TIME,
            'reduce_only': False,
            'trigger_price': 0,
        }

        order_result = await self._submit_order_with_retry(order_params)
        if order_result.success:
            order_result.size = quantity
            order_result.price = price
            order_result.side = side
            order_result.status = 'OPEN'
        return order_result

    async def place_open_order(self, contract_id: str, quantity: Decimal, direction: str) -> OrderResult:
        """Place an open order with Lighter using official SDK."""

        self.current_order = None
        self.current_order_client_id = None
        order_price = await self.get_order_price(direction)

        limit_result = await self.place_limit_order(contract_id, quantity, order_price, direction)
        if not limit_result.success:
            raise Exception(f"[OPEN] Error placing order: {limit_result.error_message}")
        normalized_quantity = getattr(limit_result, 'size', quantity)
        effective_price = getattr(limit_result, 'price', order_price)

        start_time = time.time()
        order_status = 'OPEN'

        # While waiting for order to be filled
        while time.time() - start_time < 10 and order_status != 'FILLED':
            await asyncio.sleep(0.1)
            if self.current_order is not None:
                order_status = self.current_order.status

        order_id = self.current_order.order_id if self.current_order else limit_result.order_id
        current_status = self.current_order.status if self.current_order else 'OPEN'

        return OrderResult(
            success=True,
            order_id=order_id,
            side=direction,
            size=normalized_quantity,
            price=effective_price,
            status=current_status
        )

    async def _get_active_close_orders(self, contract_id: str) -> int:
        """Get active close orders for a contract using official SDK."""
        active_orders = await self.get_active_orders(contract_id)
        active_close_orders = 0
        for order in active_orders:
            if order.side == self.config.close_order_side:
                active_close_orders += 1
        return active_close_orders

    async def place_close_order(self, contract_id: str, quantity: Decimal, price: Decimal, side: str) -> OrderResult:
        """Place a close order with Lighter using official SDK."""
        self.current_order = None
        self.current_order_client_id = None
        limit_result = await self.place_limit_order(contract_id, quantity, price, side)

        # wait for 5 seconds to ensure order is placed
        await asyncio.sleep(5)
        if limit_result.success:
            normalized_quantity = getattr(limit_result, 'size', quantity)
            return OrderResult(
                success=True,
                order_id=limit_result.order_id,
                side=side,
                size=normalized_quantity,
                price=getattr(limit_result, 'price', price),
                status='OPEN'
            )
        else:
            raise Exception(f"[CLOSE] Error placing order: {limit_result.error_message}")
    
    async def get_order_price(self, side: str = '') -> Decimal:
        """Get the price of an order with Lighter using official SDK."""
        # Get current market prices
        best_bid, best_ask = await self.fetch_bbo_prices(self.config.contract_id)
        if best_bid <= 0 or best_ask <= 0 or best_bid >= best_ask:
            self.logger.log("Invalid bid/ask prices", "ERROR")
            raise ValueError("Invalid bid/ask prices")

        order_price = (best_bid + best_ask) / 2

        active_orders = await self.get_active_orders(self.config.contract_id)
        close_orders = [order for order in active_orders if order.side == self.config.close_order_side]
        for order in close_orders:
            if side == 'buy':
                order_price = min(order_price, order.price - self.config.tick_size)
            else:
                order_price = max(order_price, order.price + self.config.tick_size)

        return order_price

    async def cancel_order(self, order_id: str) -> OrderResult:
        """Cancel an order with Lighter."""
        # Ensure client is initialized
        if self.lighter_client is None:
            await self._initialize_lighter_client()

        # Cancel order using official SDK
        cancel_order, tx_hash, error = await self.lighter_client.cancel_order(
            market_index=self.config.contract_id,
            order_index=int(order_id)  # Assuming order_id is the order index
        )

        if error is not None:
            return OrderResult(success=False, error_message=f"Cancel order error: {error}")

        if tx_hash:
            return OrderResult(success=True)
        else:
            return OrderResult(success=False, error_message='Failed to send cancellation transaction')

    async def get_order_info(self, order_id: str) -> Optional[OrderInfo]:
        """Get order information from Lighter using official SDK."""
        try:
            # Use shared API client to get account info
            account_api = lighter.AccountApi(self.api_client)

            # Get account orders
            account_data = await account_api.account(by="index", value=str(self.account_index))

            # Look for the specific order in account positions
            for position in account_data.positions:
                if position.symbol == self.config.ticker:
                    position_amt = abs(float(position.position))
                    if position_amt > 0.001:  # Only include significant positions
                        return OrderInfo(
                            order_id=order_id,
                            side="buy" if float(position.position) > 0 else "sell",
                            size=Decimal(str(position_amt)),
                            price=Decimal(str(position.avg_price)),
                            status="FILLED",  # Positions are filled orders
                            filled_size=Decimal(str(position_amt)),
                            remaining_size=Decimal('0')
                        )

            return None

        except Exception as e:
            self.logger.log(f"Error getting order info: {e}", "ERROR")
            return None

    @query_retry(reraise=True)
    async def _fetch_orders_with_retry(self) -> List[Dict[str, Any]]:
        """Get orders using official SDK."""
        # Ensure client is initialized
        if self.lighter_client is None:
            await self._initialize_lighter_client()

        # Generate auth token for API call
        auth_token, error = self.lighter_client.create_auth_token_with_expiry()
        if error is not None:
            self.logger.log(f"Error creating auth token: {error}", "ERROR")
            raise ValueError(f"Error creating auth token: {error}")

        # Use OrderApi to get active orders
        order_api = lighter.OrderApi(self.api_client)

        # Get active orders for the specific market
        orders_response = await order_api.account_active_orders(
            account_index=self.account_index,
            market_id=self.config.contract_id,
            auth=auth_token
        )

        if not orders_response:
            self.logger.log("Failed to get orders", "ERROR")
            raise ValueError("Failed to get orders")

        return orders_response.orders

    async def get_active_orders(self, contract_id: str) -> List[OrderInfo]:
        """Get active orders for a contract using official SDK."""
        order_list = await self._fetch_orders_with_retry()

        # Filter orders for the specific market
        contract_orders = []
        for order in order_list:
            # Convert Lighter Order to OrderInfo
            side = "sell" if order.is_ask else "buy"
            size = Decimal(str(order.remaining_base_amount))
            price = Decimal(str(order.price))

            # Only include orders with remaining size > 0
            if size > 0:
                contract_orders.append(OrderInfo(
                    order_id=str(order.order_index),
                    side=side,
                    size=size,
                    price=price,
                    status=order.status.upper(),
                    filled_size=Decimal(str(order.filled_base_amount)),
                    remaining_size=Decimal(str(order.remaining_base_amount))
                ))

        return contract_orders

    @query_retry(reraise=True)
    async def _fetch_positions_with_retry(self) -> List[Dict[str, Any]]:
        """Get positions using official SDK."""
        # Use shared API client
        account_api = lighter.AccountApi(self.api_client)

        # Get account info
        account_data = await account_api.account(by="index", value=str(self.account_index))

        if not account_data or not account_data.accounts:
            self.logger.log("Failed to get positions", "ERROR")
            raise ValueError("Failed to get positions")

        return account_data.accounts[0].positions

    async def get_account_positions(self) -> Decimal:
        """Get account positions using official SDK."""
        # Get account info which includes positions
        positions = await self._fetch_positions_with_retry()

        # Find position for current market
        for position in positions:
            if position.market_id == self.config.contract_id:
                return Decimal(position.position)

        return Decimal(0)

    def _normalize_quantity(self, quantity: Decimal) -> Decimal:
        """Ensure order quantity matches Lighter's size precision."""
        if quantity <= 0:
            raise ValueError("Quantity must be greater than zero.")

        if self.base_amount_multiplier is None:
            raise ValueError("Base amount multiplier is not initialized.")

        increment = Decimal('1') / Decimal(self.base_amount_multiplier)
        steps = (quantity / increment).to_integral_value(rounding=ROUND_HALF_UP)
        normalized = steps * increment

        if normalized <= 0:
            normalized = increment

        if self.min_order_quantity:
            min_increment_steps = (self.min_order_quantity / increment).to_integral_value(rounding=ROUND_HALF_UP)
            min_aligned_qty = max(self.min_order_quantity, min_increment_steps * increment)
            if normalized < min_aligned_qty:
                normalized = min_aligned_qty

        return normalized

    def _ensure_min_notional(self, quantity: Decimal, price: Decimal) -> Decimal:
        """Ensure the order notional meets Lighter's minimum requirement."""
        if not self.min_order_notional or price <= 0:
            return quantity

        notional = quantity * price
        if notional >= self.min_order_notional:
            return quantity

        increment = Decimal('1') / Decimal(self.base_amount_multiplier)
        required_qty = self.min_order_notional / price
        steps = (required_qty / increment).to_integral_value(rounding=ROUND_HALF_UP)
        adjusted_qty = steps * increment

        if adjusted_qty <= quantity:
            adjusted_qty = quantity + increment

        self.logger.log(
            f"Lighter quantity adjusted for min notional: price={price}, "
            f"original_qty={quantity}, adjusted_qty={adjusted_qty}, "
            f"min_notional={self.min_order_notional}",
            "INFO"
        )

        return adjusted_qty

    def _extract_market_numeric_attr(self, market_obj: Any, candidates: List[str]) -> Optional[Decimal]:
        """Try to extract a numeric attribute from market metadata."""
        for candidate in candidates:
            value = getattr(market_obj, candidate, None)
            if value is None:
                continue
            try:
                return Decimal(str(value))
            except (InvalidOperation, ValueError, TypeError):
                continue
        return None

    def _extract_balance_value(self, source: Any) -> Optional[Decimal]:
        """Extract a numeric balance value from different data structures."""
        if source is None:
            return None
        
        balance_fields = [
            'account_value', 'accountValue',
            'equity', 'total_equity', 'totalEquity',
            'balance', 'total_balance', 'totalBalance',
            'collateral', 'total_collateral', 'totalCollateral',
            'available_collateral', 'availableCollateral',
            'margin_balance', 'marginBalance',
            'available_balance', 'availableBalance',
            'wallet_balance', 'walletBalance',
            'free_collateral', 'freeCollateral'
        ]
        
        for field in balance_fields:
            value = None
            if isinstance(source, dict):
                value = source.get(field)
            else:
                value = getattr(source, field, None)
            
            if value is None:
                continue
            
            try:
                decimal_value = Decimal(str(value))
                return decimal_value
            except (InvalidOperation, ValueError, TypeError):
                continue
        
        return None

    @query_retry(default_return=None)
    async def get_account_balance(self) -> Optional[Decimal]:
        """Get account balance/margin."""
        try:
            if self.api_client is None:
                self.api_client = ApiClient(configuration=Configuration(host=self.base_url))
            
            account_api = lighter.AccountApi(self.api_client)
            account_data = await account_api.account(by="index", value=str(self.account_index))
            if not account_data:
                self.logger.log("Lighter: Account API returned no data", "WARNING")
                return None
            
            # account_data may itself contain balance fields
            balance = self._extract_balance_value(account_data)
            if balance is not None:
                return balance
            
            # Many responses include an accounts list; inspect entries for balance fields
            accounts = getattr(account_data, 'accounts', None)
            if accounts:
                for entry in accounts:
                    balance = self._extract_balance_value(entry)
                    if balance is not None:
                        return balance
            
            # Some models expose collateral summaries under `collateral_accounts`
            collateral_accounts = getattr(account_data, 'collateral_accounts', None)
            if collateral_accounts:
                for entry in collateral_accounts:
                    balance = self._extract_balance_value(entry)
                    if balance is not None:
                        return balance
            
            self.logger.log("Lighter: Could not find balance field in account data", "WARNING")
            return None
        except Exception as e:
            self.logger.log(f"Failed to get account balance: {e}", "WARNING")
            return None

    async def cancel_all_orders(self, contract_id: str) -> bool:
        """Cancel all orders for a contract."""
        try:
            # Get all active orders
            active_orders = await self.get_active_orders(contract_id)
            
            # Cancel each order individually
            success_count = 0
            for order in active_orders:
                try:
                    result = await self.cancel_order(order.order_id)
                    if result.success:
                        success_count += 1
                        self.logger.log(f"Canceled order: {order.order_id}", "INFO")
                    else:
                        self.logger.log(f"Failed to cancel order {order.order_id}: {result.error_message}", "WARNING")
                except Exception as e:
                    self.logger.log(f"Error canceling order {order.order_id}: {e}", "ERROR")
            
            if success_count == len(active_orders):
                return True
            elif success_count > 0:
                self.logger.log(f"Partially canceled orders: {success_count}/{len(active_orders)}", "WARNING")
                return True  # Still return True if at least some were canceled
            else:
                return False
        except Exception as e:
            self.logger.log(f"Failed to cancel all orders: {e}", "ERROR")
            return False

    async def get_contract_attributes(self) -> Tuple[str, Decimal]:
        """Get contract ID for a ticker."""
        ticker = self.config.ticker
        if len(ticker) == 0:
            self.logger.log("Ticker is empty", "ERROR")
            raise ValueError("Ticker is empty")

        if self.api_client is None:
            self.api_client = ApiClient(configuration=Configuration(host=self.base_url))

        # Populate multipliers, min quantity/notional, and base metadata
        market_id, base_multiplier, price_multiplier = await self._get_market_config(ticker)

        order_api = lighter.OrderApi(self.api_client)
        market_summary = await order_api.order_book_details(market_id=market_id)
        order_book_details = market_summary.order_book_details[0]
        # Set contract_id to market name (Lighter uses market IDs as identifiers)
        self.config.contract_id = market_id
        self.base_amount_multiplier = Decimal(base_multiplier)
        self.price_multiplier = Decimal(price_multiplier)

        try:
            self.config.tick_size = Decimal("1") / (Decimal("10") ** order_book_details.price_decimals)
        except Exception:
            self.logger.log("Failed to get tick size", "ERROR")
            raise ValueError("Failed to get tick size")

        return self.config.contract_id, self.config.tick_size
