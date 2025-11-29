import os
import ssl
import requests
from typing import Dict, Any, Optional, List, Callable

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
        self.last_update_id = 0

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
    
    def close(self):
        """close requests session"""
        if self.session:
            self.session.close()

    def send_text(self, content: str, parse_mode: str = "HTML") -> Dict[str, Any]:
        """Send a text message to Telegram"""
        payload = {
            "chat_id": self.chat_id,
            "text": content,
            "parse_mode": parse_mode
        }
        return self._send_message("sendMessage", payload)

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
    
    def get_updates(self, timeout: int = 0, offset: Optional[int] = None) -> Dict[str, Any]:
        """Get updates from Telegram Bot API"""
        url = f"{self.api_url}/getUpdates"
        params = {"timeout": timeout}
        if offset is not None:
            params["offset"] = offset
        
        try:
            response = self.session.get(url, params=params, timeout=timeout + 5 if timeout > 0 else 10)
            response_data = response.json()
            return response_data
        except Exception as e:
            print(f"Telegram get updates failed: {e}")
            return {"ok": False, "error": str(e)}
    
    def register_command(self, command: str, handler: Callable):
        """Register a command handler"""
        self.command_handlers[command] = handler
    
    def process_updates(self, updates: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Process updates and return commands to execute"""
        commands = []
        if not updates.get("ok", False):
            return commands
        
        for update in updates.get("result", []):
            update_id = update.get("update_id", 0)
            if update_id > self.last_update_id:
                self.last_update_id = update_id
            
            message = update.get("message", {})
            if not message:
                continue
            
            text = message.get("text", "")
            chat_id = message.get("chat", {}).get("id")
            
            # Only process messages from authorized chat
            if str(chat_id) != str(self.chat_id):
                continue
            
            # Check if it's a command
            if text.startswith("/"):
                parts = text.split(maxsplit=1)
                command = parts[0]
                args = parts[1] if len(parts) > 1 else ""
                
                if command in self.command_handlers:
                    commands.append({
                        "command": command,
                        "args": args,
                        "message": message,
                        "update_id": update_id,
                        "message_id": message.get("message_id"),
                        "chat_id": chat_id
                    })
        
        return commands
