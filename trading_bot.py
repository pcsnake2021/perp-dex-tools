"""
Modular Trading Bot - Supports multiple exchanges
"""

import os
import time
import asyncio
import traceback
import csv
from dataclasses import dataclass
from decimal import Decimal
from typing import Optional
from datetime import datetime, timedelta

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

        # Status tracking
        self.start_time = None
        self.initial_margin = None
        self.connection_notification_sent = False
        self.trade_count = 0  # Count of filled trades since program start

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
                    # Count filled trades (only count actual filled orders, not canceled)
                    self.trade_count += 1
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
        if time.time() - self.last_log_time > 60 or self.last_log_time == 0:
            print("--------------------------------")
            try:
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
                    error_message = f"\n\nERROR: [{self.config.exchange.upper()}_{self.config.ticker.upper()}] "
                    error_message += "Position mismatch detected\n"
                    error_message += "###### ERROR ###### ERROR ###### ERROR ###### ERROR #####\n"
                    error_message += "Please manually rebalance your position and take-profit orders\n"
                    error_message += "请手动平衡当前仓位和正在关闭的仓位\n"
                    error_message += f"current position: {position_amt} | active closing amount: {active_close_amount} | "f"Order quantity: {len(self.active_close_orders)}\n"
                    error_message += "###### ERROR ###### ERROR ###### ERROR ###### ERROR #####\n"
                    self.logger.log(error_message, "ERROR")

                    await self.send_notification(error_message.lstrip())

                    if not self.shutdown_requested:
                        self.shutdown_requested = True

                    mismatch_detected = True
                else:
                    mismatch_detected = False

                return mismatch_detected

            except Exception as e:
                self.logger.log(f"Error in periodic status check: {e}", "ERROR")
                self.logger.log(f"Traceback: {traceback.format_exc()}", "ERROR")

            print("--------------------------------")

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

    async def get_account_balance(self) -> Optional[Decimal]:
        """Get account balance/margin from exchange."""
        try:
            # Try to use exchange-specific method if available
            if hasattr(self.exchange_client, 'get_account_balance'):
                try:
                    return await asyncio.wait_for(
                        self.exchange_client.get_account_balance(),
                        timeout=5.0
                    )
                except asyncio.TimeoutError:
                    self.logger.log("Timeout getting account balance", "WARNING")
                    return None
                except Exception as e:
                    self.logger.log(f"Error getting account balance: {e}", "WARNING")
                    return None
            
            # Fallback: try get_account_margin for backward compatibility
            if hasattr(self.exchange_client, 'get_account_margin'):
                try:
                    return await asyncio.wait_for(
                        self.exchange_client.get_account_margin(),
                        timeout=5.0
                    )
                except asyncio.TimeoutError:
                    self.logger.log("Timeout getting account margin", "WARNING")
                    return None
                except Exception as e:
                    self.logger.log(f"Error getting account margin: {e}", "WARNING")
                    return None
            
            # Fallback for Backpack
            if self.config.exchange == "backpack":
                if hasattr(self.exchange_client, 'account_client'):
                    try:
                        collateral_data = self.exchange_client.account_client.get_collateral()
                        if collateral_data and isinstance(collateral_data, dict):
                            total_collateral = collateral_data.get('totalCollateral', 0)
                            if total_collateral:
                                return Decimal(str(total_collateral))
                    except Exception as e:
                        self.logger.log(f"Error getting Backpack collateral: {e}", "WARNING")
            
            return None
        except Exception as e:
            self.logger.log(f"Error getting account balance: {e}", "WARNING")
            return None
    
    async def _get_current_margin(self) -> Optional[Decimal]:
        """Get current margin from exchange (deprecated, use get_account_balance)."""
        return await self.get_account_balance()

    def _get_total_trades(self) -> int:
        """Get total number of filled trades since program start."""
        # Return the count of filled trades since this program instance started
        return self.trade_count

    def _format_runtime(self) -> str:
        """Format runtime duration."""
        if not self.start_time:
            return "未知"
        
        duration = time.time() - self.start_time
        days = int(duration // 86400)
        hours = int((duration % 86400) // 3600)
        minutes = int((duration % 3600) // 60)
        seconds = int(duration % 60)
        
        return f"{days}天{hours}小时{minutes}分{seconds}秒"

    def _calculate_pnl(self, current_margin: Optional[Decimal], initial_margin: Optional[Decimal]) -> tuple:
        """Calculate PnL (absolute and percentage)."""
        if not current_margin or not initial_margin:
            return None, None, None
        
        absolute_pnl = current_margin - initial_margin
        percentage_pnl = (absolute_pnl / initial_margin) * Decimal(100) if initial_margin > 0 else Decimal(0)
        
        # Calculate APR (annualized return)
        if self.start_time:
            runtime_days = Decimal(time.time() - self.start_time) / Decimal(86400)
            if runtime_days > 0:
                apr = percentage_pnl * (Decimal(365) / runtime_days)
            else:
                apr = Decimal(0)
        else:
            apr = Decimal(0)
        
        return absolute_pnl, percentage_pnl, apr

    async def get_status(self) -> str:
        """Get formatted status report."""
        try:
            # Check if contract_id is set
            if not hasattr(self.config, 'contract_id') or not self.config.contract_id:
                return "交易对未初始化，请等待连接完成"
            
            # Get account name
            account_name = os.getenv('ACCOUNT_NAME', 'N/A')
            
            # Get exchange name
            exchange_name = self.config.exchange.upper()
            
            # Get ticker
            ticker = self.config.ticker
            
            # Get current price with timeout
            try:
                best_bid, best_ask = await asyncio.wait_for(
                    self.exchange_client.fetch_bbo_prices(self.config.contract_id),
                    timeout=5.0
                )
                # Ensure both are Decimal
                best_bid = Decimal(best_bid) if not isinstance(best_bid, Decimal) else best_bid
                best_ask = Decimal(best_ask) if not isinstance(best_ask, Decimal) else best_ask
                current_price = (best_bid + best_ask) / Decimal(2) if best_bid > 0 and best_ask > 0 else best_ask if best_ask > 0 else best_bid
            except asyncio.TimeoutError:
                self.logger.log("Timeout fetching price", "WARNING")
                current_price = Decimal(0)
            except Exception as e:
                self.logger.log(f"Error fetching price: {e}", "WARNING")
                current_price = Decimal(0)
            
            # Get direction with icon
            direction = self.config.direction.upper()
            direction_icon = "📈" if direction == "BUY" else "📉"
            
            # Get runtime
            runtime = self._format_runtime()
            
            # Get initial and current margin with timeout
            initial_margin = self.initial_margin or Decimal(0)
            try:
                current_margin = await asyncio.wait_for(
                    self.get_account_balance(),
                    timeout=5.0
                )
                if current_margin is None:
                    current_margin = initial_margin
            except asyncio.TimeoutError:
                self.logger.log("Timeout getting current margin", "WARNING")
                current_margin = initial_margin
            except Exception as e:
                self.logger.log(f"Error getting current margin: {e}", "WARNING")
                current_margin = initial_margin
            
            # Get position with timeout
            try:
                position = await asyncio.wait_for(
                    self.exchange_client.get_account_positions(),
                    timeout=5.0
                )
                position = abs(position)
            except asyncio.TimeoutError:
                self.logger.log("Timeout getting position", "WARNING")
                position = Decimal(0)
            except Exception as e:
                self.logger.log(f"Error getting position: {e}", "WARNING")
                position = Decimal(0)
            
            # Calculate position MMR (Maintenance Margin Ratio) with risk indicator
            # This is a simplified calculation - actual MMR depends on exchange
            position_mmr = "N/A"
            mmr_risk_indicator = ""
            if position > 0 and current_margin > 0:
                # Ensure current_price is Decimal
                current_price_decimal = Decimal(current_price) if not isinstance(current_price, Decimal) else current_price
                # Simplified: assume position value is position * current_price
                position_value = position * current_price_decimal if current_price_decimal > 0 else Decimal(0)
                if position_value > 0:
                    mmr_ratio = (current_margin / position_value) * Decimal(100)
                    # Convert to float for formatting
                    mmr_ratio_float = float(mmr_ratio)
                    # Risk indicator based on margin ratio (lower = higher risk)
                    if mmr_ratio_float < 5:
                        mmr_risk_indicator = "🔴"  # Red: High risk
                        position_mmr = f"{mmr_ratio_float:.2f}% (高风险 High Risk)"
                    elif mmr_ratio_float < 10:
                        mmr_risk_indicator = "🟡"  # Yellow: Medium risk
                        position_mmr = f"{mmr_ratio_float:.2f}% (中风险 Medium Risk)"
                    else:
                        mmr_risk_indicator = "🟢"  # Green: Low risk
                        position_mmr = f"{mmr_ratio_float:.2f}% (低风险 Low Risk)"
            elif position == 0:
                mmr_risk_indicator = "⚪"  # White: No position
                position_mmr = "N/A (无持仓 No Position)"
            
            # Get active orders with timeout
            try:
                active_orders = await asyncio.wait_for(
                    self.exchange_client.get_active_orders(self.config.contract_id),
                    timeout=5.0
                )
                close_orders = [o for o in active_orders if o.side == self.config.close_order_side]
                order_count = len(close_orders)
                order_size = sum(o.size for o in close_orders)
            except asyncio.TimeoutError:
                self.logger.log("Timeout getting active orders", "WARNING")
                order_count = 0
                order_size = Decimal(0)
            except Exception as e:
                self.logger.log(f"Error getting active orders: {e}", "WARNING")
                order_count = 0
                order_size = Decimal(0)
            
            # Get total trades
            total_trades = self._get_total_trades()
            
            # Calculate PnL
            absolute_pnl, percentage_pnl, apr = self._calculate_pnl(current_margin, initial_margin)
            
            # Format status report with HTML bold tags and compact layout
            initial_margin_str = f"{initial_margin:.4f}" if initial_margin > 0 else "N/A"
            current_margin_str = f"{current_margin:.4f}" if current_margin > 0 else "N/A"
            current_price_str = f"{current_price:.4f}" if current_price > 0 else "N/A"
            
            status = f"<b>🧾 账户状态报告</b>\n"
            status += f"<b>📋 Account Status Report</b>\n\n"
            status += f"<b>👤 账户名称 Account Name:</b> {account_name}\n"
            status += f"<b>🏦 交易所 Exchange:</b> {exchange_name}\n"
            status += f"<b>💰 初始保证金 Initial Margin:</b> {initial_margin_str}\n"
            status += f"<b>🎯 交易对 Ticker:</b> {ticker}\n"
            status += f"<b>💵 当前价格 Current Price:</b> {current_price_str}\n"
            status += f"<b>{direction_icon} 交易方向 Direction:</b> {direction}\n"
            status += f"<b>⏱️ 运行时长 Runtime:</b> {runtime}\n\n"
            status += f"<b>💼 当前保证金 Current Margin:</b> {current_margin_str}\n"
            status += f"<b>📊 持仓 Position:</b> {position}\n"
            status += f"<b>{mmr_risk_indicator} 持仓MMR Position MMR:</b> {position_mmr}\n"
            status += f"<b>📑 挂单数量 Active Orders:</b> {order_count}\n"
            status += f"<b>⚖️ 挂单总量 Order Size:</b> {order_size}\n"
            status += f"<b>🔁 累计交易次数 Total Trades:</b> {total_trades}\n\n"
            status += f"<b>💹 账户盈亏 PnL:</b>\n"
            
            if absolute_pnl is not None:
                # Convert Decimal to float for formatting with automatic sign
                absolute_pnl_float = float(absolute_pnl)
                percentage_pnl_float = float(percentage_pnl)
                apr_float = float(apr)
                status += f"<b>📈 绝对盈亏 Absolute:</b> {absolute_pnl_float:+.4f}\n"
                status += f"<b>📉 百分比盈亏 Percentage:</b> {percentage_pnl_float:+.2f}%\n"
                status += f"<b>📅 年化收益 APR:</b> {apr_float:+.2f}%\n"
            else:
                status += f"<b>📈 绝对盈亏 Absolute:</b> N/A\n"
                status += f"<b>📉 百分比盈亏 Percentage:</b> N/A\n"
                status += f"<b>📅 年化收益 APR:</b> N/A\n"
            
            return status
            
        except Exception as e:
            error_detail = str(e) if str(e) else f"{type(e).__name__}"
            self.logger.log(f"Error generating status: {error_detail}", "ERROR")
            self.logger.log(f"Traceback: {traceback.format_exc()}", "ERROR")
            return f"获取状态时出错: {error_detail}"

    async def _send_connection_notification(self):
        """Send connection notification to Telegram with account information."""
        try:
            # Get account name
            account_name = os.getenv('ACCOUNT_NAME', 'N/A')
            
            # Get exchange name
            exchange_name = self.config.exchange.upper()
            
            # Get current time
            import pytz
            timezone = pytz.timezone(os.getenv('TIMEZONE', 'Asia/Shanghai'))
            current_time = datetime.now(timezone).strftime("%Y-%m-%d %H:%M:%S")
            
            # Get account balance/margin
            try:
                current_margin = await self.get_account_balance()
                margin_str = f"{current_margin:.4f}" if current_margin else "N/A"
            except:
                margin_str = "N/A"
            
            # Get position
            try:
                position = await self.exchange_client.get_account_positions()
                position = abs(position)
                position_str = f"{position}"
            except:
                position_str = "N/A"
            
            # Get active orders
            try:
                active_orders = await self.exchange_client.get_active_orders(self.config.contract_id)
                close_orders = [o for o in active_orders if o.side == self.config.close_order_side]
                order_count = len(close_orders)
                order_size = sum(o.size for o in close_orders)
                order_info = f"挂单数量: {order_count} | 挂单总量: {order_size}"
            except:
                order_info = "挂单信息获取失败"
            
            # Get current price
            try:
                best_bid, best_ask = await self.exchange_client.fetch_bbo_prices(self.config.contract_id)
                # Ensure both are Decimal
                best_bid = Decimal(best_bid) if not isinstance(best_bid, Decimal) else best_bid
                best_ask = Decimal(best_ask) if not isinstance(best_ask, Decimal) else best_ask
                current_price = (best_bid + best_ask) / Decimal(2) if best_bid > 0 and best_ask > 0 else best_ask if best_ask > 0 else best_bid
                price_str = f"{current_price:.4f}" if current_price > 0 else "N/A"
            except:
                price_str = "N/A"
            
            # Get direction with icon
            direction = self.config.direction.upper()
            direction_icon = "📈" if direction == "BUY" else "📉"
            
            # Calculate MMR risk indicator and value if position exists
            mmr_risk_indicator = ""
            mmr_str = "N/A"
            if position_str != "N/A" and float(position_str) > 0 and margin_str != "N/A" and price_str != "N/A":
                try:
                    position_decimal = Decimal(position_str)
                    current_price_decimal = Decimal(current_price) if not isinstance(current_price, Decimal) else current_price
                    position_value = position_decimal * current_price_decimal if current_price_decimal > 0 else Decimal(0)
                    if position_value > 0:
                        margin_decimal = Decimal(margin_str)
                        mmr_ratio = (margin_decimal / position_value) * Decimal(100)
                        mmr_ratio_float = float(mmr_ratio)
                        # Risk indicator based on margin ratio (lower = higher risk)
                        if mmr_ratio_float < 5:
                            mmr_risk_indicator = "🔴"  # Red: High risk
                            mmr_str = f"{mmr_ratio_float:.2f}% (高风险 High Risk)"
                        elif mmr_ratio_float < 10:
                            mmr_risk_indicator = "🟡"  # Yellow: Medium risk
                            mmr_str = f"{mmr_ratio_float:.2f}% (中风险 Medium Risk)"
                        else:
                            mmr_risk_indicator = "🟢"  # Green: Low risk
                            mmr_str = f"{mmr_ratio_float:.2f}% (低风险 Low Risk)"
                except Exception as e:
                    self.logger.log(f"Error calculating MMR in connection notification: {e}", "WARNING")
            elif position_str != "N/A" and float(position_str) == 0:
                mmr_risk_indicator = "⚪"  # White: No position
                mmr_str = "N/A (无持仓 No Position)"
            
            # Format connection notification with HTML bold tags and compact layout
            notification = f"<b>✅ 交易机器人已连接</b>\n"
            notification += f"<b>✅ Trading Bot Connected</b>\n\n"
            notification += f"<b>👤 账户名称 Account Name:</b> {account_name}\n"
            notification += f"<b>🕐 连接时间 Connection Time:</b> {current_time}\n"
            notification += f"<b>🏦 交易所 Exchange:</b> {exchange_name}\n"
            notification += f"<b>🎯 交易对 Ticker:</b> {self.config.ticker}\n"
            notification += f"<b>💵 当前价格 Current Price:</b> {price_str}\n"
            notification += f"<b>{direction_icon} 交易方向 Direction:</b> {direction}\n\n"
            notification += f"<b>💰 账户余额 Account Balance:</b> {margin_str}\n"
            notification += f"<b>📊 当前仓位 Current Position:</b> {position_str}\n"
            notification += f"<b>{mmr_risk_indicator} 持仓MMR Position MMR:</b> {mmr_str}\n"
            notification += f"<b>📑 挂单情况 Active Orders:</b> {order_info}\n\n"
            notification += f"<b>⚙️ 交易配置 Trading Config:</b>\n"
            notification += f"<b>  • 数量 Quantity:</b> {self.config.quantity}\n"
            notification += f"<b>  • 止盈 Take Profit:</b> {self.config.take_profit}%\n"
            notification += f"<b>  • 最大挂单 Max Orders:</b> {self.config.max_orders}\n"
            notification += f"<b>  • 等待时间 Wait Time:</b> {self.config.wait_time}s\n"
            notification += f"<b>  • 网格步长 Grid Step:</b> {self.config.grid_step}%\n"
            
            # Send notification
            await self.send_notification(notification)
            
        except Exception as e:
            self.logger.log(f"Error sending connection notification: {e}", "ERROR")

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

            # Record start time and initial margin
            self.start_time = time.time()
            try:
                self.initial_margin = await self.get_account_balance()
                if self.initial_margin is not None:
                    self.logger.log(f"初始保证金 (Initial Margin): {self.initial_margin:.4f}", "INFO")
                else:
                    self.logger.log("Failed to get initial margin: returned None", "WARNING")
            except Exception as e:
                self.logger.log(f"Failed to get initial margin: {e}", "WARNING")
                self.initial_margin = None

            # Send connection notification to Telegram
            if not self.connection_notification_sent:
                await self._send_connection_notification()
                self.connection_notification_sent = True

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
                mismatch_detected = await self._log_status_periodically()

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
