"""
Modular Trading Bot - Supports multiple exchanges
"""

import os
import time
import asyncio
import traceback
from dataclasses import dataclass
from decimal import Decimal
from typing import Optional

from exchanges import ExchangeFactory
from helpers import TradingLogger
from helpers.lark_bot import LarkBot
from helpers.telegram_bot import TelegramBot


@dataclass
class TradingConfig:
    """Configuration class for trading parameters."""
    ticker: str
    contract_id: str
    quantity: Decimal
    take_profit: Decimal
    tick_size: Decimal
    direction: str
    max_orders: int
    wait_time: int
    exchange: str
    grid_step: Decimal
    stop_price: Decimal
    pause_price: Decimal
    boost_mode: bool

    @property
    def close_order_side(self) -> str:
        """Get the close order side based on bot direction."""
        return 'buy' if self.direction == "sell" else 'sell'


@dataclass
class OrderMonitor:
    """Thread-safe order monitoring state."""
    order_id: Optional[str] = None
    filled: bool = False
    filled_price: Optional[Decimal] = None
    filled_qty: Decimal = 0.0

    def reset(self):
        """Reset the monitor state."""
        self.order_id = None
        self.filled = False
        self.filled_price = None
        self.filled_qty = 0.0


class TradingBot:
    """Modular Trading Bot - Main trading logic supporting multiple exchanges."""

    def __init__(self, config: TradingConfig):
        self.config = config
        self.logger = TradingLogger(config.exchange, config.ticker, log_to_console=True)

        # Create exchange client
        try:
            self.exchange_client = ExchangeFactory.create_exchange(
                config.exchange,
                config
            )
        except ValueError as e:
            raise ValueError(f"Failed to create exchange client: {e}")

        # Trading state
        self.active_close_orders = []
        self.last_close_orders = 0
        self.last_open_order_time = 0
        self.last_log_time = 0
        self.current_order_status = None
        self.order_filled_event = asyncio.Event()
        self.order_canceled_event = asyncio.Event()
        self.shutdown_requested = False
        self.loop = None
        
        # Position mismatch tracking
        self.mismatch_start_time = None  # Timestamp when mismatch was first detected
        self.mismatch_detected = False  # Current mismatch status
        self.position_mismatch_disconnected = False  # Whether we disconnected due to mismatch

        # Register order callback
        self._setup_websocket_handlers()

    async def graceful_shutdown(self, reason: str = "Unknown"):
        """Perform graceful shutdown of the trading bot."""
        self.logger.log(f"Starting graceful shutdown: {reason}", "INFO")
        self.shutdown_requested = True

        try:
            # Disconnect from exchange
            await self.exchange_client.disconnect()
            self.logger.log("Graceful shutdown completed", "INFO")

        except Exception as e:
            self.logger.log(f"Error during graceful shutdown: {e}", "ERROR")

    def _setup_websocket_handlers(self):
        """Setup WebSocket handlers for order updates."""
        def order_update_handler(message):
            """Handle order updates from WebSocket."""
            try:
                # Check if this is for our contract
                if message.get('contract_id') != self.config.contract_id:
                    return

                order_id = message.get('order_id')
                status = message.get('status')
                side = message.get('side', '')
                order_type = message.get('order_type', '')
                filled_size = Decimal(message.get('filled_size'))
                if order_type == "OPEN":
                    self.current_order_status = status

                if status == 'FILLED':
                    if order_type == "OPEN":
                        self.order_filled_amount = filled_size
                        # Ensure thread-safe interaction with asyncio event loop
                        if self.loop is not None:
                            self.loop.call_soon_threadsafe(self.order_filled_event.set)
                        else:
                            # Fallback (should not happen after run() starts)
                            self.order_filled_event.set()

                    self.logger.log(f"[{order_type}] [{order_id}] {status} "
                                    f"{message.get('size')} @ {message.get('price')}", "INFO")
                    self.logger.log_transaction(order_id, side, message.get('size'), message.get('price'), status)
                elif status == "CANCELED":
                    if order_type == "OPEN":
                        self.order_filled_amount = filled_size
                        if self.loop is not None:
                            self.loop.call_soon_threadsafe(self.order_canceled_event.set)
                        else:
                            self.order_canceled_event.set()

                        if self.order_filled_amount > 0:
                            self.logger.log_transaction(order_id, side, self.order_filled_amount, message.get('price'), status)
                            
                    # PATCH
                    if self.config.exchange == "extended":
                        self.logger.log(f"[{order_type}] [{order_id}] {status} "
                                        f"{Decimal(message.get('size')) - filled_size} @ {message.get('price')}", "INFO")
                    else:
                        self.logger.log(f"[{order_type}] [{order_id}] {status} "
                                        f"{message.get('size')} @ {message.get('price')}", "INFO")
                elif status == "PARTIALLY_FILLED":
                    self.logger.log(f"[{order_type}] [{order_id}] {status} "
                                    f"{filled_size} @ {message.get('price')}", "INFO")
                else:
                    self.logger.log(f"[{order_type}] [{order_id}] {status} "
                                    f"{message.get('size')} @ {message.get('price')}", "INFO")

            except Exception as e:
                self.logger.log(f"Error handling order update: {e}", "ERROR")
                self.logger.log(f"Traceback: {traceback.format_exc()}", "ERROR")

        # Setup order update handler
        self.exchange_client.setup_order_update_handler(order_update_handler)

    def _calculate_wait_time(self) -> Decimal:
        """Calculate wait time between orders."""
        cool_down_time = self.config.wait_time

        if len(self.active_close_orders) < self.last_close_orders:
            self.last_close_orders = len(self.active_close_orders)
            return 0

        self.last_close_orders = len(self.active_close_orders)
        if len(self.active_close_orders) >= self.config.max_orders:
            return 1

        if len(self.active_close_orders) / self.config.max_orders >= 2/3:
            cool_down_time = 2 * self.config.wait_time
        elif len(self.active_close_orders) / self.config.max_orders >= 1/3:
            cool_down_time = self.config.wait_time
        elif len(self.active_close_orders) / self.config.max_orders >= 1/6:
            cool_down_time = self.config.wait_time / 2
        else:
            cool_down_time = self.config.wait_time / 4

        # if the program detects active_close_orders during startup, it is necessary to consider cooldown_time
        if self.last_open_order_time == 0 and len(self.active_close_orders) > 0:
            self.last_open_order_time = time.time()

        if time.time() - self.last_open_order_time > cool_down_time:
            return 0
        else:
            return 1

    async def _place_and_monitor_open_order(self) -> bool:
        """Place an order and monitor its execution."""
        try:
            # Reset state before placing order
            self.order_filled_event.clear()
            self.current_order_status = 'OPEN'
            self.order_filled_amount = 0.0

            # Place the order
            order_result = await self.exchange_client.place_open_order(
                self.config.contract_id,
                self.config.quantity,
                self.config.direction
            )

            if not order_result.success:
                return False

            if order_result.status == 'FILLED':
                return await self._handle_order_result(order_result)
            elif not self.order_filled_event.is_set():
                try:
                    await asyncio.wait_for(self.order_filled_event.wait(), timeout=10)
                except asyncio.TimeoutError:
                    pass

            # Handle order result
            return await self._handle_order_result(order_result)

        except Exception as e:
            self.logger.log(f"Error placing order: {e}", "ERROR")
            self.logger.log(f"Traceback: {traceback.format_exc()}", "ERROR")
            return False

    async def _handle_order_result(self, order_result) -> bool:
        """Handle the result of an order placement."""
        order_id = order_result.order_id
        filled_price = order_result.price

        if self.order_filled_event.is_set() or order_result.status == 'FILLED':
            if self.config.boost_mode:
                close_order_result = await self.exchange_client.place_market_order(
                    self.config.contract_id,
                    self.config.quantity,
                    self.config.close_order_side
                )
            else:
                self.last_open_order_time = time.time()
                # Place close order
                close_side = self.config.close_order_side
                if close_side == 'sell':
                    close_price = filled_price * (1 + self.config.take_profit/100)
                else:
                    close_price = filled_price * (1 - self.config.take_profit/100)

                close_order_result = await self.exchange_client.place_close_order(
                    self.config.contract_id,
                    self.config.quantity,
                    close_price,
                    close_side
                )
                if self.config.exchange == "lighter":
                    await asyncio.sleep(1)

                if not close_order_result.success:
                    self.logger.log(f"[CLOSE] Failed to place close order: {close_order_result.error_message}", "ERROR")
                    raise Exception(f"[CLOSE] Failed to place close order: {close_order_result.error_message}")

                return True

        else:
            new_order_price = await self.exchange_client.get_order_price(self.config.direction)

            def should_wait(direction: str, new_order_price: Decimal, order_result_price: Decimal) -> bool:
                if direction == "buy":
                    return new_order_price <= order_result_price
                elif direction == "sell":
                    return new_order_price >= order_result_price
                return False

            if self.config.exchange == "lighter":
                current_order_status = self.exchange_client.current_order.status
            else:
                order_info = await self.exchange_client.get_order_info(order_id)
                current_order_status = order_info.status

            while (
                should_wait(self.config.direction, new_order_price, order_result.price)
                and current_order_status == "OPEN"
            ):
                self.logger.log(f"[OPEN] [{order_id}] Waiting for order to be filled @ {order_result.price}", "INFO")
                await asyncio.sleep(5)
                if self.config.exchange == "lighter":
                    current_order_status = self.exchange_client.current_order.status
                else:
                    order_info = await self.exchange_client.get_order_info(order_id)
                    if order_info is not None:
                        current_order_status = order_info.status
                new_order_price = await self.exchange_client.get_order_price(self.config.direction)

            self.order_canceled_event.clear()
            # Cancel the order if it's still open
            self.logger.log(f"[OPEN] [{order_id}] Cancelling order and placing a new order", "INFO")
            if self.config.exchange == "lighter":
                cancel_result = await self.exchange_client.cancel_order(order_id)
                start_time = time.time()
                while (time.time() - start_time < 10 and self.exchange_client.current_order.status != 'CANCELED' and
                        self.exchange_client.current_order.status != 'FILLED'):
                    await asyncio.sleep(0.1)

                if self.exchange_client.current_order.status not in ['CANCELED', 'FILLED']:
                    raise Exception(f"[OPEN] Error cancelling order: {self.exchange_client.current_order.status}")
                else:
                    self.order_filled_amount = self.exchange_client.current_order.filled_size
            else:
                try:
                    cancel_result = await self.exchange_client.cancel_order(order_id)
                    if not cancel_result.success:
                        self.order_canceled_event.set()
                        self.logger.log(f"[CLOSE] Failed to cancel order {order_id}: {cancel_result.error_message}", "WARNING")
                    else:
                        self.current_order_status = "CANCELED"

                except Exception as e:
                    self.order_canceled_event.set()
                    self.logger.log(f"[CLOSE] Error canceling order {order_id}: {e}", "ERROR")

                if self.config.exchange == "backpack" or self.config.exchange == "extended":
                    self.order_filled_amount = cancel_result.filled_size
                else:
                    # Wait for cancel event or timeout
                    if not self.order_canceled_event.is_set():
                        try:
                            await asyncio.wait_for(self.order_canceled_event.wait(), timeout=5)
                        except asyncio.TimeoutError:
                            order_info = await self.exchange_client.get_order_info(order_id)
                            self.order_filled_amount = order_info.filled_size

            if self.order_filled_amount > 0:
                close_side = self.config.close_order_side
                if self.config.boost_mode:
                    close_order_result = await self.exchange_client.place_close_order(
                        self.config.contract_id,
                        self.order_filled_amount,
                        filled_price,
                        close_side
                    )
                else:
                    if close_side == 'sell':
                        close_price = filled_price * (1 + self.config.take_profit/100)
                    else:
                        close_price = filled_price * (1 - self.config.take_profit/100)

                    close_order_result = await self.exchange_client.place_close_order(
                        self.config.contract_id,
                        self.order_filled_amount,
                        close_price,
                        close_side
                    )
                    if self.config.exchange == "lighter":
                        await asyncio.sleep(1)

                self.last_open_order_time = time.time()
                if not close_order_result.success:
                    self.logger.log(f"[CLOSE] Failed to place close order: {close_order_result.error_message}", "ERROR")

            return True

        return False

    async def _log_status_periodically(self):
        """Log status information periodically, including positions."""
        # Always return a tuple, even if not time to check yet
        if time.time() - self.last_log_time > 60 or self.last_log_time == 0:
            print("--------------------------------")
            try:
                # Reconnect if disconnected to check status
                if self.position_mismatch_disconnected:
                    try:
                        await self.exchange_client.connect()
                        await asyncio.sleep(2)
                    except Exception as e:
                        self.logger.log(f"Failed to reconnect for status check: {e}", "WARNING")
                        return True, Decimal(0), Decimal(0)  # Assume still mismatched
                
                # Get active orders
                active_orders = await self.exchange_client.get_active_orders(self.config.contract_id)

                # Filter close orders
                self.active_close_orders = []
                for order in active_orders:
                    if order.side == self.config.close_order_side:
                        self.active_close_orders.append({
                            'id': order.order_id,
                            'price': order.price,
                            'size': order.size
                        })

                # Get positions
                position_amt = await self.exchange_client.get_account_positions()
                position_amt = abs(position_amt)
                
                # Disconnect again if we reconnected just for checking
                if self.position_mismatch_disconnected:
                    try:
                        await self.exchange_client.disconnect()
                    except Exception:
                        pass

                # Calculate active closing amount
                active_close_amount = sum(
                    Decimal(order.get('size', 0))
                    for order in self.active_close_orders
                    if isinstance(order, dict)
                )

                self.logger.log(f"Current Position: {position_amt} | Active closing amount: {active_close_amount} | "
                                f"Order quantity: {len(self.active_close_orders)}")
                self.last_log_time = time.time()
                
                # Check for position mismatch
                if abs(position_amt - active_close_amount) > (2 * self.config.quantity):
                    # First time detecting mismatch
                    if not self.mismatch_detected:
                        self.mismatch_start_time = time.time()
                        self.mismatch_detected = True
                        error_message = f"\n\nERROR: [{self.config.exchange.upper()}_{self.config.ticker.upper()}] "
                        error_message += "Position mismatch detected\n"
                        error_message += "###### ERROR ###### ERROR ###### ERROR ###### ERROR #####\n"
                        error_message += "Please manually rebalance your position and take-profit orders\n"
                        error_message += "请手动平衡当前仓位和正在关闭的仓位\n"
                        error_message += f"current position: {position_amt} | active closing amount: {active_close_amount} | "f"Order quantity: {len(self.active_close_orders)}\n"
                        error_message += "系统将在10分钟后自动尝试修复\n"
                        error_message += "###### ERROR ###### ERROR ###### ERROR ###### ERROR #####\n"
                        self.logger.log(error_message, "ERROR")
                        await self.send_notification(error_message.lstrip())
                    else:
                        # Mismatch still exists, check if 10 minutes have passed
                        mismatch_duration = time.time() - self.mismatch_start_time
                        if mismatch_duration >= 600:  # 10 minutes = 600 seconds
                            self.logger.log(f"Position mismatch duration: {mismatch_duration:.0f} seconds, attempting auto-fix...", "INFO")
                            await self._auto_fix_position_mismatch(position_amt, active_close_amount)
                        else:
                            remaining_time = 600 - mismatch_duration
                            self.logger.log(f"Position mismatch ongoing. Auto-fix in {remaining_time:.0f} seconds. "
                                          f"Current: position={position_amt}, close_orders={active_close_amount}", "WARNING")
                    
                    return True, position_amt, active_close_amount
                else:
                    # Mismatch resolved
                    if self.mismatch_detected:
                        self.logger.log("Position mismatch resolved!", "INFO")
                        await self.send_notification(f"Position mismatch resolved for {self.config.exchange.upper()}_{self.config.ticker.upper()}")
                    self.mismatch_detected = False
                    self.mismatch_start_time = None
                    return False, position_amt, active_close_amount

            except Exception as e:
                self.logger.log(f"Error in periodic status check: {e}", "ERROR")
                self.logger.log(f"Traceback: {traceback.format_exc()}", "ERROR")
                print("--------------------------------")
                # Return default values on error, assume no mismatch to continue trading
                return False, Decimal(0), Decimal(0)
        
        # If not time to check yet, return current mismatch status
        return self.mismatch_detected, Decimal(0), Decimal(0)

    async def _auto_fix_position_mismatch(self, position_amt: Decimal, active_close_amount: Decimal):
        """Automatically fix position mismatch by adjusting position or orders."""
        try:
            self.logger.log("Starting automatic position mismatch fix...", "INFO")
            
            # Safety check: Verify the difference is reasonable before attempting fix
            # If difference is too large, it might indicate exchange data anomaly
            difference_abs = abs(position_amt - active_close_amount)
            if difference_abs > self.config.quantity:
                error_message = f"\n\nCRITICAL ERROR: [{self.config.exchange.upper()}_{self.config.ticker.upper()}] \n"
                error_message += "Position mismatch difference is too large, possible exchange data anomaly!\n"
                error_message += "仓位不平衡差异过大，可能是交易所数据异常！\n"
                error_message += f"Position: {position_amt} | Close orders: {active_close_amount} | Difference: {difference_abs}\n"
                error_message += f"Difference ({difference_abs}) > Quantity ({self.config.quantity})\n"
                error_message += "Disconnecting and exiting program for safety.\n"
                error_message += "为安全起见，断开连接并退出程序。\n"
                self.logger.log(error_message, "ERROR")
                await self.send_notification(error_message.lstrip())
                
                # Disconnect and exit
                await self.exchange_client.disconnect()
                self.shutdown_requested = True
                self.logger.log("Program exited due to excessive position mismatch difference", "ERROR")
                return
            
            # Calculate the difference
            difference = position_amt - active_close_amount
            
            # Reconnect if disconnected
            if self.position_mismatch_disconnected:
                self.logger.log("Reconnecting to exchange for auto-fix...", "INFO")
                await self.exchange_client.connect()
                await asyncio.sleep(2)
                self.position_mismatch_disconnected = False
            
            if difference > 0:
                # Position > Orders: Need to close excess position
                fix_amount = difference
                self.logger.log(f"Position ({position_amt}) > Close orders ({active_close_amount}). "
                              f"Closing excess position: {fix_amount}", "INFO")
                
                # Determine the side to close (opposite of bot direction)
                close_side = self.config.close_order_side
                
                # Try to use market order if available, otherwise use limit order at market price
                if hasattr(self.exchange_client, 'place_market_order'):
                    # Use market order for immediate execution
                    best_bid, best_ask = await self.exchange_client.fetch_bbo_prices(self.config.contract_id)
                    if best_bid > 0 and best_ask > 0:
                        result = await self.exchange_client.place_market_order(
                            self.config.contract_id,
                            fix_amount,
                            close_side
                        )
                        if result.success:
                            self.logger.log(f"Successfully closed {fix_amount} position using market order", "INFO")
                            await self.send_notification(f"Auto-fixed: Closed {fix_amount} excess position")
                        else:
                            self.logger.log(f"Market order failed: {result.error_message}", "ERROR")
                            # Fallback to limit order
                            await self._place_limit_order_to_fix(fix_amount, close_side, best_bid, best_ask)
                    else:
                        self.logger.log("Cannot get market prices for market order", "ERROR")
                else:
                    # Use limit order at market price
                    best_bid, best_ask = await self.exchange_client.fetch_bbo_prices(self.config.contract_id)
                    if best_bid > 0 and best_ask > 0:
                        await self._place_limit_order_to_fix(fix_amount, close_side, best_bid, best_ask)
                    else:
                        self.logger.log("Cannot get market prices", "ERROR")
                        
            elif difference < 0:
                # Position < Orders: Need to increase position (open new position)
                # Note: Only open position, do NOT place new close order
                # Because existing close orders are already enough to balance after opening position
                fix_amount = abs(difference)
                self.logger.log(f"Position ({position_amt}) < Close orders ({active_close_amount}). "
                              f"Opening new position: {fix_amount} (without placing new close order)", "INFO")
                
                # Open position in the same direction as bot
                open_direction = self.config.direction
                
                # Try to use market order if available
                if hasattr(self.exchange_client, 'place_market_order'):
                    result = await self.exchange_client.place_market_order(
                        self.config.contract_id,
                        fix_amount,
                        open_direction
                    )
                    if result.success:
                        self.logger.log(f"Successfully opened {fix_amount} position using market order", "INFO")
                        await self.send_notification(f"Auto-fixed: Opened {fix_amount} position to balance with existing close orders")
                    else:
                        self.logger.log(f"Market order failed: {result.error_message}", "ERROR")
                else:
                    # Use limit order at market price
                    best_bid, best_ask = await self.exchange_client.fetch_bbo_prices(self.config.contract_id)
                    if best_bid > 0 and best_ask > 0:
                        # Only open position, don't place close order
                        result = await self.exchange_client.place_open_order(
                            self.config.contract_id,
                            fix_amount,
                            open_direction
                        )
                        if result.success:
                            self.logger.log(f"Successfully placed open order: {open_direction} {fix_amount}", "INFO")
                            await self.send_notification(f"Auto-fixed: Placed open order for {fix_amount} to balance with existing close orders")
                        else:
                            self.logger.log(f"Failed to place open order: {result.error_message}", "ERROR")
                    else:
                        self.logger.log("Cannot get market prices", "ERROR")
            else:
                self.logger.log("Position and orders are balanced, no fix needed", "INFO")
                
            # Reset mismatch tracking after fix attempt
            self.mismatch_detected = False
            self.mismatch_start_time = None
            
        except Exception as e:
            self.logger.log(f"Error during auto-fix: {e}", "ERROR")
            self.logger.log(f"Traceback: {traceback.format_exc()}", "ERROR")
            await self.send_notification(f"Auto-fix failed: {str(e)}")

    async def _place_limit_order_to_fix(self, amount: Decimal, side: str, best_bid: Decimal, best_ask: Decimal):
        """Place a limit order at market price to fix position mismatch (only for closing excess position)."""
        try:
            if side == 'sell' or side == 'SELL':
                # For sell orders, use best bid price (slightly below to ensure execution)
                price = best_bid - self.config.tick_size
            else:
                # For buy orders, use best ask price (slightly above to ensure execution)
                price = best_ask + self.config.tick_size
            
            price = self.exchange_client.round_to_tick(price)
            
            # This method is only used for closing excess position
            # For opening position, we use place_open_order directly in _auto_fix_position_mismatch
            result = await self.exchange_client.place_close_order(
                self.config.contract_id,
                amount,
                price,
                side
            )
            
            if result.success:
                self.logger.log(f"Successfully placed limit order to close: {side} {amount} @ {price}", "INFO")
            else:
                self.logger.log(f"Failed to place limit order: {result.error_message}", "ERROR")
                
        except Exception as e:
            self.logger.log(f"Error placing limit order to fix: {e}", "ERROR")

    async def _meet_grid_step_condition(self) -> bool:
        if self.active_close_orders:
            picker = min if self.config.direction == "buy" else max
            next_close_order = picker(self.active_close_orders, key=lambda o: o["price"])
            next_close_price = next_close_order["price"]

            best_bid, best_ask = await self.exchange_client.fetch_bbo_prices(self.config.contract_id)
            if best_bid <= 0 or best_ask <= 0 or best_bid >= best_ask:
                raise ValueError("No bid/ask data available")

            if self.config.direction == "buy":
                new_order_close_price = best_ask * (1 + self.config.take_profit/100)
                if next_close_price / new_order_close_price > 1 + self.config.grid_step/100:
                    return True
                else:
                    return False
            elif self.config.direction == "sell":
                new_order_close_price = best_bid * (1 - self.config.take_profit/100)
                if new_order_close_price / next_close_price > 1 + self.config.grid_step/100:
                    return True
                else:
                    return False
            else:
                raise ValueError(f"Invalid direction: {self.config.direction}")
        else:
            return True

    async def _check_price_condition(self) -> bool:
        stop_trading = False
        pause_trading = False

        if self.config.pause_price == self.config.stop_price == -1:
            return stop_trading, pause_trading

        best_bid, best_ask = await self.exchange_client.fetch_bbo_prices(self.config.contract_id)
        if best_bid <= 0 or best_ask <= 0 or best_bid >= best_ask:
            raise ValueError("No bid/ask data available")

        if self.config.stop_price != -1:
            if self.config.direction == "buy":
                if best_ask >= self.config.stop_price:
                    stop_trading = True
            elif self.config.direction == "sell":
                if best_bid <= self.config.stop_price:
                    stop_trading = True

        if self.config.pause_price != -1:
            if self.config.direction == "buy":
                if best_ask >= self.config.pause_price:
                    pause_trading = True
            elif self.config.direction == "sell":
                if best_bid <= self.config.pause_price:
                    pause_trading = True

        return stop_trading, pause_trading

    async def send_notification(self, message: str):
        lark_token = os.getenv("LARK_TOKEN")
        if lark_token:
            async with LarkBot(lark_token) as lark_bot:
                await lark_bot.send_text(message)

        telegram_token = os.getenv("TELEGRAM_BOT_TOKEN")
        telegram_chat_id = os.getenv("TELEGRAM_CHAT_ID")
        if telegram_token and telegram_chat_id:
            with TelegramBot(telegram_token, telegram_chat_id) as tg_bot:
                tg_bot.send_text(message)

    async def run(self):
        """Main trading loop."""
        try:
            self.config.contract_id, self.config.tick_size = await self.exchange_client.get_contract_attributes()

            # Log current TradingConfig
            self.logger.log("=== Trading Configuration ===", "INFO")
            self.logger.log(f"Ticker: {self.config.ticker}", "INFO")
            self.logger.log(f"Contract ID: {self.config.contract_id}", "INFO")
            self.logger.log(f"Quantity: {self.config.quantity}", "INFO")
            self.logger.log(f"Take Profit: {self.config.take_profit}%", "INFO")
            self.logger.log(f"Direction: {self.config.direction}", "INFO")
            self.logger.log(f"Max Orders: {self.config.max_orders}", "INFO")
            self.logger.log(f"Wait Time: {self.config.wait_time}s", "INFO")
            self.logger.log(f"Exchange: {self.config.exchange}", "INFO")
            self.logger.log(f"Grid Step: {self.config.grid_step}%", "INFO")
            self.logger.log(f"Stop Price: {self.config.stop_price}", "INFO")
            self.logger.log(f"Pause Price: {self.config.pause_price}", "INFO")
            self.logger.log(f"Boost Mode: {self.config.boost_mode}", "INFO")
            self.logger.log("=============================", "INFO")

            # Capture the running event loop for thread-safe callbacks
            self.loop = asyncio.get_running_loop()
            # Connect to exchange
            await self.exchange_client.connect()

            # wait for connection to establish
            await asyncio.sleep(5)

            # Main trading loop
            while not self.shutdown_requested:
                # Update active orders
                active_orders = await self.exchange_client.get_active_orders(self.config.contract_id)

                # Filter close orders
                self.active_close_orders = []
                for order in active_orders:
                    if order.side == self.config.close_order_side:
                        self.active_close_orders.append({
                            'id': order.order_id,
                            'price': order.price,
                            'size': order.size
                        })

                # Periodic logging
                mismatch_detected, position_amt, active_close_amount = await self._log_status_periodically()

                # Handle position mismatch
                if mismatch_detected:
                    if not self.position_mismatch_disconnected:
                        # First time detecting mismatch, disconnect and wait
                        self.logger.log("Disconnecting from exchange due to position mismatch. Waiting for manual fix or auto-fix...", "WARNING")
                        await self.exchange_client.disconnect()
                        self.position_mismatch_disconnected = True
                        self.logger.log("Disconnected. Program will continue running and check for fix every 60 seconds.", "INFO")
                        self.logger.log("You can manually fix the position, or wait 10 minutes for automatic fix.", "INFO")
                    
                    # Wait and check again
                    await asyncio.sleep(60)
                    continue

                # If mismatch was resolved, reconnect
                if self.position_mismatch_disconnected and not mismatch_detected:
                    self.logger.log("Position mismatch resolved. Reconnecting to exchange...", "INFO")
                    await self.exchange_client.connect()
                    await asyncio.sleep(5)
                    self.position_mismatch_disconnected = False
                    continue

                stop_trading, pause_trading = await self._check_price_condition()
                if stop_trading:
                    msg = f"\n\nWARNING: [{self.config.exchange.upper()}_{self.config.ticker.upper()}] \n"
                    msg += "Stopped trading due to stop price triggered\n"
                    msg += "价格已经达到停止交易价格，脚本将停止交易\n"
                    await self.send_notification(msg.lstrip())
                    await self.graceful_shutdown(msg)
                    continue

                if pause_trading:
                    await asyncio.sleep(5)
                    continue

                if not mismatch_detected:
                    wait_time = self._calculate_wait_time()

                    if wait_time > 0:
                        await asyncio.sleep(wait_time)
                        continue
                    else:
                        meet_grid_step_condition = await self._meet_grid_step_condition()
                        if not meet_grid_step_condition:
                            await asyncio.sleep(1)
                            continue

                        await self._place_and_monitor_open_order()
                        self.last_close_orders += 1

        except KeyboardInterrupt:
            self.logger.log("Bot stopped by user")
            await self.graceful_shutdown("User interruption (Ctrl+C)")
        except Exception as e:
            self.logger.log(f"Critical error: {e}", "ERROR")
            self.logger.log(f"Traceback: {traceback.format_exc()}", "ERROR")
            await self.graceful_shutdown(f"Critical error: {e}")
            raise
        finally:
            # Ensure all connections are closed even if graceful shutdown fails
            try:
                await self.exchange_client.disconnect()
            except Exception as e:
                self.logger.log(f"Error disconnecting from exchange: {e}", "ERROR")
