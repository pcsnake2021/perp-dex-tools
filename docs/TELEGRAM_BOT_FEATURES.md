# Telegram 机器人功能说明

## 一、概述

项目中的 Telegram 机器人用于向用户发送交易相关的通知和警报。它是一个轻量级的通知工具，通过 Telegram Bot API 发送消息。

## 二、实现的功能

### 2.1 核心功能

#### 1. **文本消息发送** ✅

**功能描述**：向指定的 Telegram 聊天发送文本消息

**实现位置**：`helpers/telegram_bot.py`

**方法**：
```python
def send_text(self, content: str, parse_mode: str = "HTML") -> Dict[str, Any]
```

**特性**：
- 支持 HTML 格式的消息解析（默认）
- 使用 Telegram Bot API 的 `sendMessage` 方法
- 返回 API 响应结果

**代码实现**：
```33:40:helpers/telegram_bot.py
    def send_text(self, content: str, parse_mode: str = "HTML") -> Dict[str, Any]:
        """Send a text message to Telegram"""
        payload = {
            "chat_id": self.chat_id,
            "text": content,
            "parse_mode": parse_mode
        }
        return self._send_message("sendMessage", payload)
```

#### 2. **上下文管理器支持** ✅

**功能描述**：支持 Python 的 `with` 语句，自动管理资源

**实现**：
```python
def __enter__(self):
    return self

def __exit__(self, exc_type, exc_val, exc_tb):
    self.close()
```

**使用示例**：
```python
with TelegramBot(token, chat_id) as tg_bot:
    tg_bot.send_text("消息内容")
```

#### 3. **SSL 安全连接** ✅

**功能描述**：使用 SSL 证书验证确保安全连接

**实现**：
```17:20:helpers/telegram_bot.py
        # Create session with SSL context
        self.session = requests.Session()
        self.session.verify = certifi.where()
        self.session.timeout = 10
```

**特性**：
- 使用 `certifi` 库提供 CA 证书
- 设置 10 秒超时
- 确保 HTTPS 连接安全

#### 4. **错误处理** ✅

**功能描述**：捕获并处理发送消息时的异常

**实现**：
```42:54:helpers/telegram_bot.py
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
```

**特性**：
- 捕获网络异常
- 检查 API 响应状态
- 返回错误信息而不是抛出异常

### 2.2 在交易机器人中的应用场景

#### 1. **仓位不平衡警报** 🔴

**触发时机**：检测到仓位不平衡时

**消息内容**：
- 交易所和交易对信息
- 当前持仓数量
- 活跃平仓订单总量
- 提示系统将在 10 分钟后自动修复

**代码位置**：
```432:432:trading_bot.py
                        await self.send_notification(error_message.lstrip())
```

#### 2. **仓位不平衡修复通知** ✅

**触发时机**：仓位不平衡被修复后（手动或自动）

**消息内容**：
```
Position mismatch resolved for {EXCHANGE}_{TICKER}
```

**代码位置**：
```450:450:trading_bot.py
                        await self.send_notification(f"Position mismatch resolved for {self.config.exchange.upper()}_{self.config.ticker.upper()}")
```

#### 3. **自动修复成功通知** ✅

**触发时机**：自动修复仓位不平衡成功时

**消息类型**：

**a) 平掉多余持仓**：
```
Auto-fixed: Closed {fix_amount} excess position
```

**代码位置**：
```501:501:trading_bot.py
                            await self.send_notification(f"Auto-fixed: Closed {fix_amount} excess position")
```

**b) 开仓平衡**：
```
Auto-fixed: Opened {fix_amount} position to balance with existing close orders
```

**代码位置**：
```536:536:trading_bot.py
                        await self.send_notification(f"Auto-fixed: Opened {fix_amount} position to balance with existing close orders")
```

**c) 限价单开仓**：
```
Auto-fixed: Placed open order for {fix_amount} to balance with existing close orders
```

**代码位置**：
```551:551:trading_bot.py
                            await self.send_notification(f"Auto-fixed: Placed open order for {fix_amount} to balance with existing close orders")
```

#### 4. **自动修复失败通知** ❌

**触发时机**：自动修复过程中发生错误

**消息内容**：
```
Auto-fix failed: {error_message}
```

**代码位置**：
```566:566:trading_bot.py
            await self.send_notification(f"Auto-fix failed: {str(e)}")
```

#### 5. **价格停止交易通知** ⚠️

**触发时机**：达到停止交易价格时

**消息内容**：
```
WARNING: [{EXCHANGE}_{TICKER}] 
Stopped trading due to stop price triggered
价格已经达到停止交易价格，脚本将停止交易
```

**代码位置**：
```739:739:trading_bot.py
                    await self.send_notification(msg.lstrip())
```

### 2.3 集成方式

#### 通知发送方法

**实现位置**：`trading_bot.py` 的 `send_notification` 方法

**代码**：
```653:663:trading_bot.py
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
```

**特性**：
- 同时支持 Telegram 和 Lark（飞书）通知
- 如果配置了多个通知渠道，会同时发送
- 使用环境变量配置

## 三、配置要求

### 3.1 环境变量

需要在 `.env` 文件中配置以下变量：

```bash
# Telegram Bot Token（从 BotFather 获取）
TELEGRAM_BOT_TOKEN=your_bot_token_here

# Telegram Chat ID（你的用户 ID 或群组 ID）
TELEGRAM_CHAT_ID=your_chat_id_here
```

### 3.2 获取配置信息

**获取 Bot Token**：
1. 在 Telegram 中搜索 `@BotFather`
2. 发送 `/newbot` 命令创建新机器人
3. 按照提示设置机器人名称和用户名
4. BotFather 会返回 Bot Token

**获取 Chat ID**：
1. 向你的机器人发送任意消息
2. 访问：`https://api.telegram.org/bot{your_bot_token}/getUpdates`
3. 在返回的 JSON 中找到 `"chat":{"id": ... }` 中的 `id` 值

详细设置步骤请参考：`docs/telegram-bot-setup.md`

## 四、功能限制

### 4.1 当前未实现的功能

以下功能**未实现**，但可以通过扩展实现：

1. ❌ **接收消息**：机器人只能发送消息，不能接收和处理用户消息
2. ❌ **命令处理**：不支持 `/start`、`/status` 等命令
3. ❌ **交互式操作**：不支持按钮、键盘等交互元素
4. ❌ **文件发送**：不支持发送图片、文档等文件
5. ❌ **消息格式化**：仅支持 HTML，不支持 Markdown
6. ❌ **消息编辑/删除**：不支持编辑或删除已发送的消息
7. ❌ **群组管理**：不支持群组管理功能
8. ❌ **定时消息**：不支持定时发送消息
9. ❌ **消息队列**：不支持消息队列和重试机制
10. ❌ **多聊天支持**：每次使用需要指定 chat_id

### 4.2 设计特点

**轻量级设计**：
- 专注于单向通知功能
- 最小化依赖和复杂度
- 适合交易机器人的通知需求

**同步发送**：
- 使用同步的 `requests` 库
- 在异步环境中使用，但发送本身是同步的
- 简单直接，适合通知场景

## 五、使用示例

### 5.1 基本使用

```python
from helpers.telegram_bot import TelegramBot

# 使用上下文管理器
with TelegramBot("your_token", "your_chat_id") as bot:
    bot.send_text("这是一条测试消息")
```

### 5.2 在交易机器人中使用

交易机器人会自动调用 `send_notification` 方法，无需手动调用：

```python
# 在 trading_bot.py 中
await self.send_notification("仓位不平衡警报！")
```

### 5.3 发送 HTML 格式消息

```python
message = """
<b>交易警报</b>
<i>交易所</i>: BACKPACK
<i>交易对</i>: SOL
<i>状态</i>: 仓位不平衡
"""
bot.send_text(message, parse_mode="HTML")
```

## 六、技术实现细节

### 6.1 API 端点

**基础 URL**：`https://api.telegram.org/bot`

**完整 API URL**：`https://api.telegram.org/bot{token}/{method}`

**使用的方法**：`sendMessage`

### 6.2 请求格式

```json
{
    "chat_id": "your_chat_id",
    "text": "消息内容",
    "parse_mode": "HTML"
}
```

### 6.3 响应格式

**成功响应**：
```json
{
    "ok": true,
    "result": {
        "message_id": 123,
        "date": 1234567890,
        "chat": {...},
        "text": "消息内容"
    }
}
```

**失败响应**：
```json
{
    "ok": false,
    "error_code": 400,
    "description": "错误描述"
}
```

## 七、总结

### 7.1 已实现功能

✅ 文本消息发送  
✅ SSL 安全连接  
✅ 错误处理  
✅ 上下文管理器支持  
✅ 与交易机器人集成  
✅ 多种通知场景支持  

### 7.2 适用场景

- ✅ 交易警报通知
- ✅ 错误和异常通知
- ✅ 状态变更通知
- ✅ 自动修复结果通知

### 7.3 设计理念

Telegram 机器人的设计遵循**简单、可靠、专注**的原则：

- **简单**：只实现必要的通知功能
- **可靠**：包含错误处理和 SSL 安全
- **专注**：专注于单向通知，不包含复杂的交互功能

这种设计使得机器人轻量、易用，完全满足交易机器人的通知需求。

