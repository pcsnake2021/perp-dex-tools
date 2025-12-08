"""
Telegram command handler for trading bot status queries.
"""

import os
import asyncio
from typing import Optional
from helpers.telegram_bot import TelegramBot


class TelegramCommandHandler:
    """Handle Telegram commands for trading bot."""
    
    def __init__(self, trading_bot, token: str, chat_id: str):
        self.trading_bot = trading_bot
        self.token = token
        self.chat_id = chat_id
        self.running = False
        self.update_offset = 0
        
    async def start(self):
        """Start the command handler."""
        self.running = True
        bot = TelegramBot(self.token, self.chat_id)
        try:
            # Poll for updates asynchronously
            while self.running:
                try:
                    # Run get_updates in executor to avoid blocking event loop
                    loop = asyncio.get_event_loop()
                    updates = await loop.run_in_executor(
                        None,
                        lambda: bot.get_updates(offset=self.update_offset, timeout=5)
                    )
                    
                    if updates.get("ok") and updates.get("result"):
                        for update in updates["result"]:
                            self.update_offset = update["update_id"] + 1
                            # Handle update asynchronously in event loop
                            asyncio.create_task(self._handle_update_async(update, bot))
                    
                    # Small delay to avoid busy waiting
                    await asyncio.sleep(0.5)
                except Exception as e:
                    print(f"Error in Telegram command handler: {e}")
                    await asyncio.sleep(5)
        finally:
            bot.close()
    
    async def _handle_update_async(self, update: dict, bot: TelegramBot):
        """Handle update asynchronously in event loop."""
        message = update.get("message", {})
        text = message.get("text", "")
        chat_id = message.get("chat", {}).get("id")
        
        if not text or not chat_id:
            return
        
        # Check if it's a command
        if text.startswith("/"):
            command = text.split()[0].lstrip("/")
            if command == "status":
                try:
                    # Directly call get_status in the same event loop - no cross-thread call!
                    result = await self.trading_bot.get_status()
                    if result:
                        bot.send_text(result, chat_id=str(chat_id))
                except Exception as e:
                    import traceback
                    error_detail = str(e) if str(e) else type(e).__name__
                    print(f"Error handling status command: {error_detail}\n{traceback.format_exc()}")
                    bot.send_text(f"处理命令时出错: {error_detail}", chat_id=str(chat_id))
    
    def stop(self):
        """Stop the command handler."""
        self.running = False
    
    async def _handle_status_command_async(self) -> Optional[str]:
        """Handle /status command (async version)."""
        try:
            return await self.trading_bot.get_status()
        except Exception as e:
            import traceback
            error_detail = str(e) if str(e) else type(e).__name__
            error_trace = traceback.format_exc()
            print(f"Error in get_status: {error_detail}\n{error_trace}")
            return f"获取状态时出错: {error_detail}"
    
    def _handle_status_command(self) -> Optional[str]:
        """Handle /status command (sync wrapper)."""
        try:
            # Check if trading bot is initialized
            if not hasattr(self.trading_bot, 'exchange_client'):
                return "交易机器人未初始化，请等待连接完成"
            
            # Check if contract_id is set
            if not hasattr(self.trading_bot.config, 'contract_id') or not self.trading_bot.config.contract_id:
                return "交易对未设置，请等待初始化完成"
            
            # Run the async get_status method in the event loop
            if self.trading_bot.loop and self.trading_bot.loop.is_running():
                # If we're in the same event loop, schedule the coroutine
                try:
                    print(f"[Telegram] Starting status query...")
                    future = asyncio.run_coroutine_threadsafe(
                        self._handle_status_command_async(),
                        self.trading_bot.loop
                    )
                    result = future.result(timeout=60)  # Increase timeout to 60 seconds
                    print(f"[Telegram] Status query completed")
                    return result
                except asyncio.TimeoutError:
                    print(f"[Telegram] Status query timeout after 60 seconds")
                    return "获取状态超时（60秒），请稍后重试。可能是网络延迟或交易所API响应慢。"
                except Exception as e:
                    import traceback
                    error_detail = str(e) if str(e) else type(e).__name__
                    error_trace = traceback.format_exc()
                    print(f"Error in async call: {error_detail}\n{error_trace}")
                    return f"获取状态时出错: {error_detail}"
            else:
                # Fallback: run in new event loop
                try:
                    return asyncio.run(self._handle_status_command_async())
                except Exception as e:
                    import traceback
                    error_detail = str(e) if str(e) else type(e).__name__
                    error_trace = traceback.format_exc()
                    print(f"Error in fallback async call: {error_detail}\n{error_trace}")
                    return f"获取状态时出错: {error_detail}"
        except Exception as e:
            import traceback
            error_detail = str(e) if str(e) else type(e).__name__
            error_trace = traceback.format_exc()
            print(f"Error in _handle_status_command: {error_detail}\n{error_trace}")
            return f"获取状态时出错: {error_detail}"

