"""
Modular Trading Bot - Supports multiple exchanges
"""

import os
import time
import json
import uuid
import asyncio
import traceback
from dataclasses import dataclass
from decimal import Decimal
from typing import Optional, List

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
    profit_protection_threshold: Optional[Decimal] = None  # xx% profit to start protection
    profit_protection_drawdown: Optional[Decimal] = None  # yy% drawdown from peak to trigger
    stop_loss_threshold: Optional[Decimal] = None  # xx% loss from initial capital to trigger
    silent_mode_duration: Optional[int] = None  # xx minutes to wait in silent mode

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
        self.last_executed_quantity = self.config.quantity
        self.exit_requested = False
        self.start_time = time.time()
        
        # Position mismatch tracking
        self.mismatch_start_time = None  # Timestamp when mismatch was first detected
        self.mismatch_detected = False  # Current mismatch status
        self.position_mismatch_disconnected = False  # Whether we disconnected due to mismatch
        
        # Account tracking for Telegram bot
        self.initial_margin = None  # Initial margin balance when bot started
        self.trade_count = 0  # Total number of trades executed
        self.telegram_bot = None  # Telegram bot instance for command handling
        self.command_handling_task = None  # Task for handling Telegram commands
        
        # Profit protection state
        self.profit_protection_active = False  # Whether profit protection is currently active
        self.peak_profit = Decimal(0)  # Highest profit reached (in absolute value)
        self.peak_profit_percentage = Decimal(0)  # Highest profit percentage reached
        
        # Silent mode state
        self.in_silent_mode = False  # Whether currently in silent mode
        self.silent_mode_start_time = None  # When silent mode started

        # Register order callback
        self._setup_websocket_handlers()

    async def graceful_shutdown(self, reason: str = "Unknown"):
        """Perform graceful shutdown of the trading bot."""
        self.logger.log(f"Starting graceful shutdown: {reason}", "INFO")
        self.shutdown_requested = True

        try:
            # Delete status snapshot file to prevent /status from showing this account
            try:
                snapshot_path = self._status_snapshot_path()
                if os.path.exists(snapshot_path):
                    os.remove(snapshot_path)
                    self.logger.log(f"Deleted status snapshot file: {snapshot_path}", "INFO")
            except Exception as e:
                self.logger.log(f"Failed to delete status snapshot: {e}", "WARNING")

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
            executed_qty = getattr(order_result, 'size', self.config.quantity)
            if executed_qty is None:
                executed_qty = self.config.quantity
            executed_qty = Decimal(str(executed_qty))
            self.last_executed_quantity = executed_qty
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

        actual_quantity = getattr(order_result, 'size', None)
        if actual_quantity is not None:
            actual_quantity = Decimal(str(actual_quantity))
        if actual_quantity is None or actual_quantity == 0:
            filled_amount = getattr(self, 'order_filled_amount', None)
            if filled_amount is not None and filled_amount != 0:
                actual_quantity = Decimal(str(filled_amount))
            else:
                actual_quantity = self.config.quantity

        if self.order_filled_event.is_set() or order_result.status == 'FILLED':
            if self.config.boost_mode:
                close_order_result = await self.exchange_client.place_market_order(
                    self.config.contract_id,
                    actual_quantity,
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
                    actual_quantity,
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
                # Refresh snapshot for multi-account Telegram status
                await self.get_account_status(include_trade_count=True)
                
                # Check for position mismatch
                quantity_threshold = max(self.config.quantity, getattr(self, 'last_executed_quantity', self.config.quantity))
                if abs(position_amt - active_close_amount) > (2 * quantity_threshold):
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
    
    async def get_account_balance(self) -> Optional[Decimal]:
        """Get account balance/margin. Returns None if not available."""
        try:
            # Try to get balance from exchange client if method exists
            if hasattr(self.exchange_client, 'get_account_balance'):
                return await self.exchange_client.get_account_balance()
            
            # For other exchanges, this is a placeholder - each exchange may need specific implementation
            return None
        except Exception as e:
            self.logger.log(f"Error getting account balance: {e}", "WARNING")
            return None
    
    async def _check_profit_protection(self) -> bool:
        """Check profit protection conditions and trigger if needed.
        
        Returns:
            True if profit protection was triggered (should enter silent mode)
            False otherwise
        """
        if self.config.profit_protection_threshold is None:
            return False
        
        if self.initial_margin is None:
            return False
        
        current_balance = await self.get_account_balance()
        if current_balance is None:
            return False
        
        # Calculate current profit
        current_profit = current_balance - self.initial_margin
        if self.initial_margin > 0:
            current_profit_percentage = (current_profit / self.initial_margin) * Decimal(100)
        else:
            current_profit_percentage = Decimal(0)
        
        # Check if profit exceeds threshold to start protection
        if not self.profit_protection_active:
            if current_profit_percentage >= self.config.profit_protection_threshold:
                self.profit_protection_active = True
                self.peak_profit = current_profit
                self.peak_profit_percentage = current_profit_percentage
                self.logger.log(f"利润保护已启动 (Profit protection activated): "
                              f"当前盈利 {current_profit_percentage:.2f}% >= {self.config.profit_protection_threshold}%", "INFO")
        
        # If protection is active, check for drawdown
        if self.profit_protection_active:
            # Update peak if current profit is higher
            if current_profit > self.peak_profit:
                self.peak_profit = current_profit
                self.peak_profit_percentage = current_profit_percentage
                self.logger.log(f"利润新高 (New profit peak): {current_profit_percentage:.2f}%", "INFO")
            
            # Calculate drawdown from peak
            if self.peak_profit > 0:
                drawdown_from_peak = ((self.peak_profit - current_profit) / self.peak_profit) * Decimal(100)
            else:
                # If peak was 0 or negative, use percentage difference
                drawdown_from_peak = self.peak_profit_percentage - current_profit_percentage
            
            # Check if drawdown exceeds threshold
            if drawdown_from_peak >= self.config.profit_protection_drawdown:
                self.logger.log(f"利润保护触发 (Profit protection triggered): "
                              f"从峰值回撤 {drawdown_from_peak:.2f}% >= {self.config.profit_protection_drawdown}%", "WARNING")
                return True
        
        return False
    
    async def _check_stop_loss(self) -> bool:
        """Check stop loss conditions and trigger if needed.
        
        Stop loss is calculated based on:
        - Initial margin: account balance when bot started
        - Current balance: current account balance (including unrealized PnL)
        - Loss percentage: (initial_margin - current_balance) / initial_margin * 100
        
        Returns:
            True if stop loss was triggered (should enter silent mode)
            False otherwise
        """
        if self.config.stop_loss_threshold is None:
            return False
        
        if self.initial_margin is None:
            return False
        
        current_balance = await self.get_account_balance()
        if current_balance is None:
            return False
        
        # Validate that both values are positive
        if self.initial_margin <= 0:
            self.logger.log(f"[Stop Loss Check] Invalid initial_margin: {self.initial_margin:.4f}, skipping check", "WARNING")
            return False
        
        if current_balance <= 0:
            self.logger.log(f"[Stop Loss Check] Invalid current_balance: {current_balance:.4f}, skipping check", "WARNING")
            return False
        
        # Only check stop loss if we're actually losing money
        # If current balance >= initial margin, we're not losing, so skip stop loss check
        if current_balance >= self.initial_margin:
            return False
        
        # Calculate loss from initial capital (only when current_balance < initial_margin)
        # Loss = Initial Margin - Current Balance
        loss = self.initial_margin - current_balance
        
        # Loss Percentage = (Loss / Initial Margin) * 100
        # This represents the percentage drop from initial capital
        loss_percentage = (loss / self.initial_margin) * Decimal(100)
        
        # Add debug logging to help diagnose issues
        self.logger.log(f"[Stop Loss Check] Initial Margin: {self.initial_margin:.4f}, "
                       f"Current Balance: {current_balance:.4f}, "
                       f"Loss: {loss:.4f}, "
                       f"Loss %: {loss_percentage:.2f}%, "
                       f"Threshold: {self.config.stop_loss_threshold}%", "DEBUG")
        
        # Check if loss exceeds threshold
        if loss_percentage >= self.config.stop_loss_threshold:
            self.logger.log(f"止损触发 (Stop loss triggered): "
                          f"相对于初始本金下跌 {loss_percentage:.2f}% >= {self.config.stop_loss_threshold}%", "WARNING")
            return True
        
        return False
    
    async def _enter_silent_mode(self, reason: str):
        """Enter silent mode: close all positions, cancel orders, wait, then reset."""
        if self.in_silent_mode:
            return  # Already in silent mode
        
        self.in_silent_mode = True
        self.silent_mode_start_time = time.time()
        
        self.logger.log(f"进入系统静默期 (Entering silent mode): {reason}", "WARNING")
        
        # Close all positions and cancel all orders
        success = await self.close_all_positions_and_orders()
        if not success:
            self.logger.log("清仓和取消挂单时出现错误，但继续进入静默期", "WARNING")
        
        # Report account status
        status = await self.get_account_status(include_trade_count=True)
        status += f"\n\n<b>🔇 系统静默期已启动</b>\n"
        status += f"<b>Silent Mode Activated</b>\n"
        status += f"<b>原因 Reason:</b> {reason}\n"
        if self.config.silent_mode_duration:
            status += f"<b>静默时长 Duration:</b> {self.config.silent_mode_duration} 分钟 (minutes)\n"
        await self.send_notification(status)
        
        # Wait for specified duration
        if self.config.silent_mode_duration:
            wait_seconds = self.config.silent_mode_duration * 60
            self.logger.log(f"等待 {self.config.silent_mode_duration} 分钟后重置参数...", "INFO")
            await asyncio.sleep(wait_seconds)
        
        # Reset all parameters
        self.logger.log("重置所有参数 (Resetting all parameters)...", "INFO")
        
        # Reset initial margin and profit tracking
        new_initial_margin = await self.get_account_balance()
        if new_initial_margin is not None:
            self.initial_margin = new_initial_margin
            self.logger.log(f"新的初始保证金 (New initial margin): {new_initial_margin:.4f}", "INFO")
        
        # Reset profit protection state
        self.profit_protection_active = False
        self.peak_profit = Decimal(0)
        self.peak_profit_percentage = Decimal(0)
        
        # Reset trade count
        self.trade_count = 0
        
        # Reset start time
        self.start_time = time.time()
        
        # Exit silent mode
        self.in_silent_mode = False
        self.silent_mode_start_time = None
        
        # Report new account status
        new_status = await self.get_account_status(include_trade_count=False)
        new_status = "<b>🔄 系统已重置，重新开始</b>\n<b>System Reset, Restarting</b>\n\n" + new_status
        await self.send_notification(new_status)
        
        self.logger.log("系统静默期结束，参数已重置，重新开始交易", "INFO")
    
    def _shared_runtime_root(self) -> str:
        """Directory shared across all bot instances for Telegram coordination."""
        default_root = os.path.join(os.path.expanduser("~"), ".perp_dex_bot")
        root = os.getenv("TELEGRAM_SHARED_RUNTIME", default_root)
        os.makedirs(root, exist_ok=True)
        return root

    def _status_snapshot_dir(self) -> str:
        base_dir = os.path.join(self._shared_runtime_root(), "telegram_status")
        os.makedirs(base_dir, exist_ok=True)
        return base_dir

    def _status_snapshot_path(self) -> str:
        account_name = os.getenv('ACCOUNT_NAME', 'DEFAULT')
        safe_account = "".join(c if c.isalnum() or c in "-_" else "_" for c in account_name)
        filename = f"{safe_account}_{self.config.exchange}_{self.config.ticker}.json"
        return os.path.join(self._status_snapshot_dir(), filename)

    def _write_status_snapshot(self, status_text: str):
        snapshot = {
            "account": os.getenv('ACCOUNT_NAME', 'DEFAULT'),
            "exchange": self.config.exchange,
            "ticker": self.config.ticker,
            "timestamp": time.time(),
            "status": status_text
        }
        try:
            with open(self._status_snapshot_path(), "w", encoding="utf-8") as f:
                json.dump(snapshot, f, ensure_ascii=False, indent=2)
        except Exception as e:
            self.logger.log(f"Failed to write status snapshot: {e}", "WARNING")

    def _collect_status_snapshots(self) -> List[str]:
        base_dir = self._status_snapshot_dir()
        snapshots: List[tuple] = []
        current_time = time.time()
        # Consider account inactive if snapshot is older than 5 minutes
        max_age_seconds = 300  # 5 minutes
        
        try:
            for filename in os.listdir(base_dir):
                if not filename.endswith(".json"):
                    continue
                path = os.path.join(base_dir, filename)
                try:
                    with open(path, "r", encoding="utf-8") as f:
                        data = json.load(f)
                        status_text = data.get("status")
                        timestamp = data.get("timestamp", 0)
                        
                        # Skip if snapshot is too old (account likely exited)
                        age = current_time - timestamp
                        if age > max_age_seconds:
                            self.logger.log(
                                f"Skipping stale snapshot {filename} (age: {age:.0f}s > {max_age_seconds}s)",
                                "DEBUG"
                            )
                            continue
                        
                        if status_text:
                            snapshots.append((timestamp, status_text))
                except Exception:
                    continue
        except FileNotFoundError:
            pass
        snapshots.sort(key=lambda item: item[0], reverse=True)
        return [status for _, status in snapshots]

    async def _get_position_mmr(self) -> Optional[Decimal]:
        """Get Maintenance Margin Requirement (MMR) for current position.
        
        Returns MMR as a percentage (e.g., 2.5 for 2.5%), or None if not available.
        """
        try:
            # Try to get MMR from exchange-specific account info
            # Different exchanges may have different APIs for this
            exchange_name = self.config.exchange.lower()
            
            if exchange_name == "backpack":
                # Backpack: Try to get from collateral info
                try:
                    if hasattr(self.exchange_client, 'account_client'):
                        collateral_info = self.exchange_client.account_client.get_collateral()
                        if collateral_info:
                            if isinstance(collateral_info, dict):
                                # Look for maintenance margin rate
                                mmr = (collateral_info.get('maintenanceMarginRate') or
                                      collateral_info.get('maintenanceMargin') or
                                      collateral_info.get('mmr') or
                                      collateral_info.get('maintenanceMarginRatio'))
                                if mmr is not None:
                                    return Decimal(str(mmr)) * Decimal(100)  # Convert to percentage
                except Exception:
                    pass
            
            elif exchange_name == "edgex":
                # EdgeX: Try to get from account positions data
                try:
                    if hasattr(self.exchange_client, 'client'):
                        positions_data = await self.exchange_client.client.get_account_positions()
                        if positions_data and 'data' in positions_data:
                            account_data = positions_data.get('data', {})
                            mmr = (account_data.get('maintenanceMarginRate') or
                                  account_data.get('maintenanceMargin') or
                                  account_data.get('mmr'))
                            if mmr is not None:
                                return Decimal(str(mmr)) * Decimal(100)  # Convert to percentage
                except Exception:
                    pass
            
            # For other exchanges, return None to use calculated risk indicator
            return None
            
        except Exception as e:
            self.logger.log(f"Error getting MMR: {e}", "DEBUG")
            return None

    def _format_duration(self, seconds: float) -> str:
        seconds = max(0, int(seconds))
        days, remainder = divmod(seconds, 86400)
        hours, remainder = divmod(remainder, 3600)
        minutes, secs = divmod(remainder, 60)
        parts = []
        if days:
            parts.append(f"{days}天")
        if hours or parts:
            parts.append(f"{hours}小时")
        if minutes or parts:
            parts.append(f"{minutes}分")
        parts.append(f"{secs}秒")
        return "".join(parts)

    def _command_queue_dir(self) -> str:
        base_dir = os.path.join(self._shared_runtime_root(), "telegram_commands")
        os.makedirs(base_dir, exist_ok=True)
        return base_dir

    def _enqueue_command(self, command_name: str, args: str, metadata: Optional[dict] = None):
        queue_dir = self._command_queue_dir()
        payload = {
            "command": command_name.lower(),
            "args": args or "",
            "created": time.time(),
            "metadata": metadata or {}
        }

        dedup_key = None
        if metadata:
            dedup_key = metadata.get("update_id") or metadata.get("message_id")

        if dedup_key:
            filename = f"{command_name.strip('/')}_{dedup_key}.json"
        else:
            filename = f"{int(payload['created'] * 1000)}_{uuid.uuid4().hex}.json"

        path = os.path.join(queue_dir, filename)

        if dedup_key:
            try:
                fd = os.open(path, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
                with os.fdopen(fd, "w", encoding="utf-8") as f:
                    json.dump(payload, f, ensure_ascii=False, indent=2)
                self.logger.log(f"Enqueued unique command {command_name} with key {dedup_key}", "INFO")
                return
            except FileExistsError:
                self.logger.log(f"Command {command_name} with key {dedup_key} already enqueued. Skipping duplicate.", "INFO")
                return
            except Exception as e:
                self.logger.log(f"Failed to enqueue Telegram command {command_name}: {e}", "ERROR")
                return

        try:
            with open(path, "w", encoding="utf-8") as f:
                json.dump(payload, f, ensure_ascii=False, indent=2)
        except Exception as e:
            self.logger.log(f"Failed to enqueue Telegram command {command_name}: {e}", "ERROR")

    async def _process_exit_command(self, target_account: str) -> bool:
        account_name = os.getenv('ACCOUNT_NAME', 'DEFAULT')
        if not target_account:
            await self.send_notification("❌ /exit 命令需要指定账户名，例如 /exit ACCOUNT_NAME")
            return True

        if target_account.lower() != account_name.lower():
            return False

        if self.exit_requested:
            self.logger.log("Exit already in progress. Ignoring duplicate /exit command.", "WARNING")
            return True

        self.logger.log(f"/exit command received for account {account_name}", "WARNING")
        self.exit_requested = True
        success = await self.close_all_positions_and_orders()
        if success:
            final_status = await self.get_account_status(include_trade_count=True)
            final_status += "\n\n<b>所有交易已关闭，程序即将退出</b>\n"
            final_status += "<b>All trades closed, program exiting...</b>"
            await self.send_notification(final_status)
            self.shutdown_requested = True
            await self.graceful_shutdown("Exit command received from Telegram")
        else:
            await self.send_notification("❌ Failed to close all positions and orders. Please check manually.")
            self.exit_requested = False

        return True

    async def _process_local_command_queue(self):
        queue_dir = self._command_queue_dir()
        try:
            entries = sorted(
                [f for f in os.listdir(queue_dir) if f.endswith(".json")]
            )
        except FileNotFoundError:
            return

        for entry in entries:
            path = os.path.join(queue_dir, entry)
            lock_path = f"{path}.lock"
            try:
                os.replace(path, lock_path)
            except OSError:
                continue

            handled = False
            try:
                with open(lock_path, "r", encoding="utf-8") as f:
                    data = json.load(f)

                command_name = data.get("command", "").lower()
                args = data.get("args", "")
                metadata = data.get("metadata", {})

                if command_name == "/status":
                    handled = await self._handle_status_command()
                elif command_name == "/exit":
                    handled = await self._process_exit_command(args.strip())
                else:
                    self.logger.log(f"Unknown Telegram command: {command_name}", "WARNING")
                    handled = True

            except Exception as e:
                self.logger.log(f"Error processing Telegram command file {entry}: {e}", "ERROR")
                handled = True
            finally:
                if handled:
                    try:
                        os.remove(lock_path)
                    except FileNotFoundError:
                        pass
                else:
                    try:
                        os.replace(lock_path, path)
                    except OSError:
                        pass

    async def get_account_status(self, include_trade_count: bool = False, update_snapshot: bool = True) -> str:
        """Get formatted account status message."""
        try:
            account_name = os.getenv('ACCOUNT_NAME', 'DEFAULT')
            exchange_name = self.config.exchange.upper()
            ticker = self.config.ticker.upper()
            
            # Get current balance
            current_balance = await self.get_account_balance()
            balance_str = f"{current_balance:.4f}" if current_balance is not None else "N/A"
            
            # Get position
            position_amt = await self.exchange_client.get_account_positions()
            position_amt = abs(position_amt)
            
            # Get active orders
            active_orders = await self.exchange_client.get_active_orders(self.config.contract_id)
            total_order_size = sum(order.size for order in active_orders)
            
            # Get current market price
            current_price = None
            price_str = "N/A"
            try:
                best_bid, best_ask = await self.exchange_client.fetch_bbo_prices(self.config.contract_id)
                if best_bid > 0 and best_ask > 0:
                    current_price = (best_bid + best_ask) / Decimal(2)
                    price_str = f"{current_price:.4f}"
            except Exception as e:
                self.logger.log(f"Failed to get current price: {e}", "DEBUG")
            
            # Get trading direction
            direction_str = self.config.direction.upper()
            direction_icon = "📈" if direction_str == "BUY" else "📉"
            
            # Calculate MMR (Maintenance Margin Requirement) and risk level
            mmr_str = "N/A"
            mmr_risk_indicator = ""
            if position_amt > 0 and current_balance is not None and current_price is not None:
                try:
                    # Try to get MMR from exchange account info
                    mmr = await self._get_position_mmr()
                    if mmr is None:
                        # If MMR not available, calculate a risk indicator based on margin ratio
                        # Risk indicator = (current_balance / position_value) * 100
                        position_value = position_amt * current_price
                        if position_value > 0:
                            margin_ratio = (current_balance / position_value) * Decimal(100)
                            # Use margin_ratio as a proxy for MMR risk
                            # Lower margin_ratio = higher risk
                            if margin_ratio < Decimal(5):
                                mmr_risk_indicator = "🔴"  # Red: High risk
                                mmr_str = f"{margin_ratio:.2f}% (高风险 High Risk)"
                            elif margin_ratio < Decimal(10):
                                mmr_risk_indicator = "🟡"  # Yellow: Medium risk
                                mmr_str = f"{margin_ratio:.2f}% (中风险 Medium Risk)"
                            else:
                                mmr_risk_indicator = "🟢"  # Green: Low risk
                                mmr_str = f"{margin_ratio:.2f}% (低风险 Low Risk)"
                        else:
                            mmr_str = "N/A"
                    else:
                        # Use actual MMR from exchange
                        if mmr < Decimal(5):
                            mmr_risk_indicator = "🔴"  # Red: High risk
                            mmr_str = f"{mmr:.2f}% (高风险 High Risk)"
                        elif mmr < Decimal(10):
                            mmr_risk_indicator = "🟡"  # Yellow: Medium risk
                            mmr_str = f"{mmr:.2f}% (中风险 Medium Risk)"
                        else:
                            mmr_risk_indicator = "🟢"  # Green: Low risk
                            mmr_str = f"{mmr:.2f}% (低风险 Low Risk)"
                except Exception as e:
                    self.logger.log(f"Failed to calculate MMR: {e}", "DEBUG")
                    mmr_str = "N/A"
            elif position_amt == 0:
                mmr_risk_indicator = "⚪"  # White: No position
                mmr_str = "N/A (无持仓 No Position)"
            
            # Calculate PnL & APR
            initial_margin_str = f"{self.initial_margin:.4f}" if self.initial_margin is not None else "N/A"
            pnl_absolute = None
            pnl_abs_str = "N/A"
            pnl_pct_str = "N/A"
            apr_str = "N/A"
            runtime_seconds = max(0.0, time.time() - self.start_time)
            runtime_str = self._format_duration(runtime_seconds)
            if self.initial_margin is not None and current_balance is not None:
                pnl_absolute = current_balance - self.initial_margin
                if self.initial_margin > 0:
                    pnl_percentage = (pnl_absolute / self.initial_margin) * Decimal(100)
                else:
                    pnl_percentage = Decimal(0)
                pnl_abs_str = f"{pnl_absolute:+.4f}"
                pnl_pct_str = f"{pnl_percentage:+.2f}%"

                runtime_year_fraction = Decimal(str(runtime_seconds)) / Decimal('31536000') if runtime_seconds > 0 else None
                if runtime_year_fraction and runtime_year_fraction > 0 and self.initial_margin > 0:
                    apr_value = (pnl_absolute / self.initial_margin) / runtime_year_fraction * Decimal(100)
                    apr_str = f"{apr_value:+.2f}%"

            
            # Build status message
            status = f"<b>🧾 账户状态报告</b>\n"
            status += f"<b>📋 Account Status Report</b>\n\n"
            status += f"<b>👤 账户名称 Account Name:</b> {account_name}\n"
            status += f"<b>🏦 交易所 Exchange:</b> {exchange_name}\n"
            status += f"<b>💰 初始保证金 Initial Margin:</b> {initial_margin_str}\n"
            status += f"<b>🎯 交易对 Ticker:</b> {ticker}\n"
            status += f"<b>💵 当前价格 Current Price:</b> {price_str}\n"
            status += f"<b>{direction_icon} 交易方向 Direction:</b> {direction_str}\n"
            status += f"<b>⏱️ 运行时长 Runtime:</b> {runtime_str}\n\n"
            status += f"<b>💼 当前保证金 Current Margin:</b> {balance_str}\n"
            status += f"<b>📊 持仓 Position:</b> {position_amt}\n"
            status += f"<b>{mmr_risk_indicator} 持仓MMR Position MMR:</b> {mmr_str}\n"
            status += f"<b>📑 挂单数量 Active Orders:</b> {len(active_orders)}\n"
            status += f"<b>⚖️ 挂单总量 Order Size:</b> {total_order_size}\n"
            
            if include_trade_count:
                status += f"<b>🔁 累计交易次数 Total Trades:</b> {self.trade_count}\n"
            
            status += f"\n<b>💹 账户盈亏 PnL:</b>\n"
            status += f"<b>📈 绝对盈亏 Absolute:</b> {pnl_abs_str}\n"
            status += f"<b>📉 百分比盈亏 Percentage:</b> {pnl_pct_str}\n"
            status += f"<b>📅 年化收益 APR:</b> {apr_str}\n"

            if update_snapshot:
                self._write_status_snapshot(status)
            
            return status
        except Exception as e:
            self.logger.log(f"Error getting account status: {e}", "ERROR")
            return f"Error getting account status: {str(e)}"

    async def _handle_status_command(self) -> bool:
        """Send combined status for all connected accounts."""
        # Refresh our own snapshot
        await self.get_account_status(include_trade_count=True, update_snapshot=True)
        snapshots = self._collect_status_snapshots()
        if not snapshots:
            return True

        for snapshot in snapshots:
            if self.telegram_bot:
                self.telegram_bot.send_text(snapshot)
            else:
                await self.send_notification(snapshot)
            await asyncio.sleep(0.2)  # small delay to avoid flooding

        return True
    
    async def _wait_for_no_active_orders(self, timeout: int = 30) -> bool:
        """Repeatedly cancel orders until none remain or timeout."""
        start = time.time()
        while time.time() - start < timeout:
            active_orders = await self.exchange_client.get_active_orders(self.config.contract_id)
            if len(active_orders) == 0:
                return True

            for order in active_orders:
                try:
                    await self.exchange_client.cancel_order(order.order_id)
                except Exception as cancel_err:
                    self.logger.log(f"Error canceling order {order.order_id}: {cancel_err}", "WARNING")

            await asyncio.sleep(1)

        self.logger.log("Timeout waiting for active orders to cancel", "WARNING")
        return False

    async def _wait_for_flat_position(self, threshold: Decimal = Decimal("0.0001"), timeout: int = 60) -> bool:
        """Attempt to flatten position until below threshold or timeout."""
        start = time.time()
        while time.time() - start < timeout:
            position_amt = await self.exchange_client.get_account_positions()
            if abs(position_amt) <= threshold:
                return True

            close_side = 'sell' if position_amt > 0 else 'buy'

            if hasattr(self.exchange_client, 'place_market_order'):
                try:
                    result = await self.exchange_client.place_market_order(
                        self.config.contract_id,
                        abs(position_amt),
                        close_side
                    )
                    if result.success:
                        self.logger.log(f"Attempted market close of {abs(position_amt)} while waiting for flat position", "INFO")
                except Exception as market_err:
                    self.logger.log(f"Market close attempt failed during exit: {market_err}", "WARNING")

            else:
                try:
                    best_bid, best_ask = await self.exchange_client.fetch_bbo_prices(self.config.contract_id)
                    if best_bid > 0 and best_ask > 0:
                        if close_side == 'sell':
                            price = best_bid - self.config.tick_size
                        else:
                            price = best_ask + self.config.tick_size
                        price = self.exchange_client.round_to_tick(price)
                        result = await self.exchange_client.place_close_order(
                            self.config.contract_id,
                            abs(position_amt),
                            price,
                            close_side
                        )
                        if result.success:
                            self.logger.log(f"Reissued close order while waiting for flat position: {abs(position_amt)} @ {price}", "INFO")
                except Exception as limit_err:
                    self.logger.log(f"Limit close attempt failed during exit: {limit_err}", "WARNING")

            await asyncio.sleep(2)

        self.logger.log("Timeout waiting for position to close", "WARNING")
        return False

    async def close_all_positions_and_orders(self) -> bool:
        """Close all positions and cancel all orders."""
        try:
            self.logger.log("Closing all positions and canceling all orders...", "INFO")
            
            # Try to cancel all orders at once if method exists (e.g., Backpack)
            if hasattr(self.exchange_client, 'cancel_all_orders'):
                try:
                    result = await self.exchange_client.cancel_all_orders(self.config.contract_id)
                    self.logger.log("Canceled all orders using cancel_all_orders method", "INFO")
                except Exception as e:
                    self.logger.log(f"Failed to cancel all orders at once: {e}, trying individually...", "WARNING")
                    # Fallback to individual cancellation
                    active_orders = await self.exchange_client.get_active_orders(self.config.contract_id)
                    for order in active_orders:
                        try:
                            await self.exchange_client.cancel_order(order.order_id)
                            self.logger.log(f"Canceled order: {order.order_id}", "INFO")
                        except Exception as e:
                            self.logger.log(f"Failed to cancel order {order.order_id}: {e}", "WARNING")
            else:
                # Cancel orders individually
                active_orders = await self.exchange_client.get_active_orders(self.config.contract_id)
                for order in active_orders:
                    try:
                        await self.exchange_client.cancel_order(order.order_id)
                        self.logger.log(f"Canceled order: {order.order_id}", "INFO")
                    except Exception as e:
                        self.logger.log(f"Failed to cancel order {order.order_id}: {e}", "WARNING")
            
            orders_cleared = await self._wait_for_no_active_orders()
            if not orders_cleared:
                self.logger.log("Proceeding to close position even though some orders remain open", "WARNING")

            # Close all positions using market order
            position_amt = await self.exchange_client.get_account_positions()
            if abs(position_amt) > Decimal('0.0001'):  # Small threshold to avoid floating point issues
                close_side = 'sell' if position_amt > 0 else 'buy'
                
                # Try to use market order if available
                if hasattr(self.exchange_client, 'place_market_order'):
                    result = await self.exchange_client.place_market_order(
                        self.config.contract_id,
                        abs(position_amt),
                        close_side
                    )
                    if result.success:
                        self.logger.log(f"Closed position: {abs(position_amt)} using market order", "INFO")
                    else:
                        self.logger.log(f"Failed to close position: {result.error_message}", "ERROR")
                        return False
                else:
                    # Use limit order at market price
                    best_bid, best_ask = await self.exchange_client.fetch_bbo_prices(self.config.contract_id)
                    if best_bid > 0 and best_ask > 0:
                        if close_side == 'sell':
                            price = best_bid - self.config.tick_size
                        else:
                            price = best_ask + self.config.tick_size
                        
                        price = self.exchange_client.round_to_tick(price)
                        result = await self.exchange_client.place_close_order(
                            self.config.contract_id,
                            abs(position_amt),
                            price,
                            close_side
                        )
                        if result.success:
                            self.logger.log(f"Placed close order: {abs(position_amt)} @ {price}", "INFO")
                        else:
                            self.logger.log(f"Failed to place close order: {result.error_message}", "ERROR")
                            return False
                    else:
                        self.logger.log("Cannot get market prices for closing position", "ERROR")
                        return False

                flat = await self._wait_for_flat_position()
                if not flat:
                    self.logger.log("Failed to flatten position within timeout window", "ERROR")
                    return False
            
            return True
        except Exception as e:
            self.logger.log(f"Error closing positions and orders: {e}", "ERROR")
            self.logger.log(f"Traceback: {traceback.format_exc()}", "ERROR")
            return False
    
    async def handle_telegram_commands(self):
        """Handle Telegram commands in background."""
        telegram_token = os.getenv("TELEGRAM_BOT_TOKEN")
        telegram_chat_id = os.getenv("TELEGRAM_CHAT_ID")
        
        if not telegram_token or not telegram_chat_id:
            return
        
        self.telegram_bot = TelegramBot(telegram_token, telegram_chat_id)
        
        # Register command handlers
        async def handle_status_command(command: dict):
            metadata = {
                "update_id": command.get("update_id"),
                "message_id": command.get("message_id"),
                "chat_id": command.get("chat_id")
            }
            self._enqueue_command("/status", command.get("args", ""), metadata)
        
        async def handle_exit_command(command: dict):
            metadata = {
                "update_id": command.get("update_id"),
                "message_id": command.get("message_id"),
                "chat_id": command.get("chat_id")
            }
            self._enqueue_command("/exit", command.get("args", ""), metadata)
        
        # Register handlers (wrapped to handle async)
        def status_wrapper(command: dict):
            if self.loop:
                self.loop.create_task(handle_status_command(command))
            else:
                asyncio.create_task(handle_status_command(command))
        
        def exit_wrapper(command: dict):
            if self.loop:
                self.loop.create_task(handle_exit_command(command))
            else:
                asyncio.create_task(handle_exit_command(command))
        
        self.telegram_bot.register_command("/status", status_wrapper)
        self.telegram_bot.register_command("/exit", exit_wrapper)
        
        # Poll for updates
        while not self.shutdown_requested:
            try:
                updates = self.telegram_bot.get_updates(timeout=5, offset=self.telegram_bot.last_update_id + 1)
                commands = self.telegram_bot.process_updates(updates)
                
                for cmd in commands:
                    handler = self.telegram_bot.command_handlers.get(cmd["command"])
                    if handler:
                        handler(cmd)

                await self._process_local_command_queue()
                await asyncio.sleep(1)
            except Exception as e:
                self.logger.log(f"Error handling Telegram commands: {e}", "ERROR")
                await asyncio.sleep(5)

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
            if self.config.profit_protection_threshold is not None:
                self.logger.log(f"Profit Protection: {self.config.profit_protection_threshold}% threshold, "
                              f"{self.config.profit_protection_drawdown}% drawdown", "INFO")
            if self.config.stop_loss_threshold is not None:
                self.logger.log(f"Stop Loss: {self.config.stop_loss_threshold}%", "INFO")
            if self.config.silent_mode_duration is not None:
                self.logger.log(f"Silent Mode Duration: {self.config.silent_mode_duration} minutes", "INFO")
            self.logger.log("=============================", "INFO")

            # Capture the running event loop for thread-safe callbacks
            self.loop = asyncio.get_running_loop()
            # Connect to exchange
            await self.exchange_client.connect()

            # wait for connection to establish
            await asyncio.sleep(5)
            
            # Get initial margin balance
            self.initial_margin = await self.get_account_balance()
            if self.initial_margin is None:
                self.logger.log("Warning: Could not get initial margin balance", "WARNING")
            
            # Start Telegram command handling
            telegram_token = os.getenv("TELEGRAM_BOT_TOKEN")
            telegram_chat_id = os.getenv("TELEGRAM_CHAT_ID")
            if telegram_token and telegram_chat_id:
                self.command_handling_task = asyncio.create_task(self.handle_telegram_commands())
                # Send initial account status
                initial_status = await self.get_account_status(include_trade_count=False)
                initial_status = "<b>🚀 交易机器人启动</b>\n<b>Trading Bot Started</b>\n\n" + initial_status
                await self.send_notification(initial_status)

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

                # Check stop loss (before profit protection, as it's more critical)
                # Skip checks if in silent mode
                if not self.in_silent_mode:
                    if await self._check_stop_loss():
                        await self._enter_silent_mode("止损触发 (Stop loss triggered)")
                        continue
                    
                    # Check profit protection
                    if await self._check_profit_protection():
                        await self._enter_silent_mode("利润保护触发 (Profit protection triggered)")
                        continue
                else:
                    # In silent mode, just wait a bit and continue
                    # _enter_silent_mode handles the full silent mode cycle
                    await asyncio.sleep(10)
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

                if self.exit_requested:
                    await asyncio.sleep(1)
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

                        order_executed = await self._place_and_monitor_open_order()
                        if order_executed:
                            self.last_close_orders += 1
                            self.trade_count += 1

        except KeyboardInterrupt:
            self.logger.log("Bot stopped by user")
            await self.graceful_shutdown("User interruption (Ctrl+C)")
        except Exception as e:
            self.logger.log(f"Critical error: {e}", "ERROR")
            self.logger.log(f"Traceback: {traceback.format_exc()}", "ERROR")
            await self.graceful_shutdown(f"Critical error: {e}")
            raise
        finally:
            # Cancel Telegram command handling task
            if self.command_handling_task:
                self.command_handling_task.cancel()
                try:
                    await self.command_handling_task
                except asyncio.CancelledError:
                    pass
            
            # Ensure all connections are closed even if graceful shutdown fails
            try:
                await self.exchange_client.disconnect()
            except Exception as e:
                self.logger.log(f"Error disconnecting from exchange: {e}", "ERROR")
