import os
import ssl
import requests
from typing import Dict, Any, Optional, Callable

import certifi

BASE_URL = "https://api.telegram.org/bot"

class TelegramBot:
    def __init__(self, token: str, chat_id: str, base_url: Optional[str] = None):
        self.token = token
        self.chat_id = chat_id
        self.base_url = base_url if base_url else BASE_URL
        self.api_url = f"{self.base_url.rstrip('/')}{self.token}"

        # Create session with SSL context
        self.session = requests.Session()
        self.session.verify = certifi.where()
        self.session.timeout = 10

        # Command handlers
        self.command_handlers: Dict[str, Callable] = {}

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
    
    def close(self):
        """close requests session"""
        if self.session:
            self.session.close()

    def register_command(self, command: str, handler: Callable):
        """Register a command handler."""
        self.command_handlers[command] = handler

    def send_text(self, content: str, parse_mode: str = "HTML", chat_id: Optional[str] = None) -> Dict[str, Any]:
        """Send a text message to Telegram"""
        payload = {
            "chat_id": chat_id if chat_id else self.chat_id,
            "text": content,
            "parse_mode": parse_mode
        }
        return self._send_message("sendMessage", payload)

    def get_updates(self, offset: Optional[int] = None, timeout: int = 0) -> Dict[str, Any]:
        """Get updates from Telegram (for polling)."""
        url = f"{self.api_url}/getUpdates"
        params = {"timeout": timeout}
        if offset:
            params["offset"] = offset
        
        try:
            response = self.session.get(url, params=params)
            response_data = response.json()
            return response_data
        except Exception as e:
            print(f"Telegram get updates failed: {e}")
            return {"ok": False, "error": str(e)}

    def handle_update(self, update: Dict[str, Any]):
        """Handle a Telegram update (message/command)."""
        message = update.get("message", {})
        text = message.get("text", "")
        chat_id = message.get("chat", {}).get("id")
        
        if not text or not chat_id:
            return
        
        # Check if it's a command
        if text.startswith("/"):
            command = text.split()[0].lstrip("/")
            if command in self.command_handlers:
                try:
                    result = self.command_handlers[command]()
                    if result:
                        self.send_text(result, chat_id=str(chat_id))
                    else:
                        self.send_text("命令执行失败，未返回结果", chat_id=str(chat_id))
                except Exception as e:
                    import traceback
                    error_detail = str(e) if str(e) else f"{type(e).__name__}"
                    error_trace = traceback.format_exc()
                    print(f"Error handling command: {error_detail}\n{error_trace}")
                    self.send_text(f"处理命令时出错: {error_detail}", chat_id=str(chat_id))

    def _send_message(self, method: str, payload: Dict[str, Any]) -> Dict[str, Any]:
        """Internal method to send messages to Telegram API"""
        url = f"{self.api_url}/{method}"
        
        try:
            response = self.session.post(url, json=payload)
            response_data = response.json()
            if not response_data.get("ok", False):
                print(f"Telegram send message failed: {response_data}")
            return response_data
        except Exception as e:
            print(f"Telegram send message failed: {e}")
            return {"ok": False, "error": str(e)}
