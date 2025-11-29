# Telegram 机器人命令功能说明

## 一、概述

Telegram 机器人已扩展支持接收消息和处理命令，实现了账户状态查询、启动上报和远程退出等功能。

## 二、新增功能

### 2.1 程序启动时主动上报账户状态 ✅

**触发时机**：程序启动并连接交易所后

-**上报内容**：
  - 账户名称（ACCOUNT_NAME）
  - 交易所名称
  - 初始保证金（程序启动时的账户余额）
  - 交易对
  - 当前保证金余额
  - 持仓情况
  - 挂单情况
  - **程序运行时长（xx天xx小时xx分xx秒）**
  - **年化收益率 APR（基于当前收益率 * 365 / 运行天数）**

**消息格式**：
```
🚀 交易机器人启动
Trading Bot Started

🧾 账户状态报告
📋 Account Status Report

👤 账户名称 Account Name: {ACCOUNT_NAME}
🏦 交易所 Exchange: {EXCHANGE}
💰 初始保证金 Initial Margin: {initial_margin}
🎯 交易对 Ticker: {TICKER}
⏱️ 运行时长 Runtime: {runtime_str}

💼 当前保证金 Current Margin: {current_balance}
📊 持仓 Position: {position_amt}
📑 挂单数量 Active Orders: {order_count}
⚖️ 挂单总量 Order Size: {total_order_size}

💹 账户盈亏 PnL:
📈 绝对盈亏 Absolute: {pnl_absolute}
📉 百分比盈亏 Percentage: {pnl_percentage}%
📅 年化收益 APR: {apr}
```

> APR 计算方式：APR = (当前收益率 ÷ 运行时长（以年为单位)) × 100%，其中运行时长 = 运行秒数 ÷ 31,536,000。

> **多账户提示**：当多个账户同时连接机器人时，每个账户都会单独发送一条状态消息，避免将所有信息堆在一条消息里导致阅读困难。

**实现位置**：`trading_bot.py` 的 `run()` 方法

**代码**：
```python
# Get initial margin balance
self.initial_margin = await self.get_account_balance()

# Start Telegram command handling
if telegram_token and telegram_chat_id:
    self.command_handling_task = asyncio.create_task(self.handle_telegram_commands())
    # Send initial account status
    initial_status = await self.get_account_status(include_trade_count=False)
    initial_status = "<b>🚀 交易机器人启动</b>\n<b>Trading Bot Started</b>\n\n" + initial_status
    await self.send_notification(initial_status)
```

### 2.2 /status 命令 - 查询账户状态 ✅

**命令格式**：`/status`

**功能**：查询并显示当前账户状态

**返回内容**：
- 账户名称、交易所名称、初始保证金
- 交易对
- 当前保证金余额、持仓情况、挂单情况
- **程序运行时长**
- **累计交易次数**
- 账户盈亏：绝对盈亏、百分比盈亏
- **年化收益 APR**

**消息格式**：
```
🧾 账户状态报告
📋 Account Status Report

👤 账户名称 Account Name: {ACCOUNT_NAME}
🏦 交易所 Exchange: {EXCHANGE}
💰 初始保证金 Initial Margin: {initial_margin}
🎯 交易对 Ticker: {TICKER}
⏱️ 运行时长 Runtime: {runtime_str}

💼 当前保证金 Current Margin: {current_balance}
📊 持仓 Position: {position_amt}
📑 挂单数量 Active Orders: {order_count}
⚖️ 挂单总量 Order Size: {total_order_size}
🔁 累计交易次数 Total Trades: {trade_count}

💹 账户盈亏 PnL:
📈 绝对盈亏 Absolute: {pnl_absolute}
📉 百分比盈亏 Percentage: {pnl_percentage}%
📅 年化收益 APR: {apr}
```

**实现位置**：`trading_bot.py` 的 `handle_telegram_commands()` 方法

**使用示例**：
```
用户发送: /status
机器人回复: [账户状态报告]
```

### 2.3 /exit 命令 - 关闭交易并退出程序 ✅

**命令格式**：`/exit {account_name}`

**功能**：
1. 验证账户名称
2. 市价平仓所有持仓
3. 取消所有挂单
4. 返回最终账户状态
5. 退出主程序

**参数**：
- `account_name`：必须与 `.env` 文件中的 `ACCOUNT_NAME` 匹配（不区分大小写）

**执行流程**：

1. **验证账户名称**：
   ```python
   account_name = os.getenv('ACCOUNT_NAME', 'DEFAULT')
   if args.strip().lower() == account_name.lower():
       # 继续执行
   else:
       # 返回错误消息
   ```

2. **关闭所有持仓**：
   - 优先使用市价单（如果交易所支持）
   - 否则使用限价单（价格略低于/高于市价以确保成交）

3. **取消所有挂单**：
   - 优先使用 `cancel_all_orders` 方法（如果交易所支持，如 Backpack）
   - 否则逐个取消订单

4. **返回最终状态**：
   - 包含所有账户信息
   - 包含累计交易次数
   - 提示程序即将退出

5. **退出程序**：
   - 调用 `graceful_shutdown()`
   - 断开交易所连接
   - 程序正常退出

**消息格式**（成功时）：
```
[账户状态报告 - 包含所有信息]

所有交易已关闭，程序即将退出
All trades closed, program exiting...
```

**消息格式**（失败时）：
```
❌ Failed to close all positions and orders. Please check manually.
```

**消息格式**（账户名不匹配）：
```
❌ Account name mismatch. Expected: {ACCOUNT_NAME}, Got: {provided_name}
```

**实现位置**：`trading_bot.py` 的 `handle_exit_command()` 和 `close_all_positions_and_orders()` 方法

**使用示例**：
```
用户发送: /exit MAIN
机器人执行: 
  1. 验证账户名称
  2. 平仓和取消订单
  3. 返回最终状态
  4. 程序退出
```

## 三、技术实现

### 3.1 TelegramBot 类扩展

**新增方法**：

1. **`get_updates()`**：获取 Telegram 消息更新
   ```python
   def get_updates(self, timeout: int = 0, offset: Optional[int] = None) -> Dict[str, Any]
   ```

2. **`process_updates()`**：处理更新并提取命令
   ```python
   def process_updates(self, updates: Dict[str, Any]) -> List[Dict[str, Any]]
   ```

3. **`register_command()`**：注册命令处理器
   ```python
   def register_command(self, command: str, handler: Callable)
   ```

**新增属性**：
- `command_handlers`：命令处理器字典
- `last_update_id`：最后处理的更新 ID

### 3.2 TradingBot 类扩展

**新增方法**：

1. **`get_account_balance()`**：获取账户余额/保证金
   ```python
   async def get_account_balance(self) -> Optional[Decimal]
   ```

2. **`get_account_status()`**：获取格式化的账户状态
   ```python
   async def get_account_status(self, include_trade_count: bool = False) -> str
   ```

3. **`close_all_positions_and_orders()`**：关闭所有持仓和订单
   ```python
   async def close_all_positions_and_orders(self) -> bool
   ```

4. **`handle_telegram_commands()`**：处理 Telegram 命令的后台任务
   ```python
   async def handle_telegram_commands(self)
   ```

**新增属性**：
- `initial_margin`：初始保证金（程序启动时记录）
- `trade_count`：累计交易次数
- `telegram_bot`：Telegram 机器人实例
- `command_handling_task`：命令处理任务

### 3.3 交易所客户端扩展

**BackpackClient 新增方法**：

1. **`get_account_balance()`**：获取账户余额
   ```python
   async def get_account_balance(self) -> Optional[Decimal]
   ```

2. **`cancel_all_orders()`**：取消所有订单
   ```python
   async def cancel_all_orders(self, contract_id: str) -> bool
   ```

## 四、工作流程

### 4.1 启动流程

```
程序启动
    ↓
连接交易所
    ↓
获取初始保证金 (initial_margin)
    ↓
启动 Telegram 命令处理任务
    ↓
发送启动状态报告
    ↓
开始主交易循环
```

### 4.2 命令处理流程

```
Telegram 消息到达
    ↓
get_updates() 获取更新
    ↓
process_updates() 解析命令
    ↓
匹配命令处理器
    ↓
执行命令（异步任务）
    ↓
发送响应消息
```

### 4.3 /exit 命令执行流程

```
接收 /exit {account_name} 命令
    ↓
验证账户名称
    ↓
cancel_all_orders() 或逐个取消订单
    ↓
close_all_positions() 平仓
    ↓
获取最终账户状态
    ↓
发送最终状态报告
    ↓
graceful_shutdown()
    ↓
程序退出
```

## 五、账户状态信息说明

### 5.1 账户基本信息

- **账户名称**：从环境变量 `ACCOUNT_NAME` 获取，默认为 "DEFAULT"
- **交易所名称**：当前使用的交易所（如 BACKPACK、EDGEX 等）
- **初始保证金**：程序启动时记录的账户余额
- **交易对**：当前交易的交易对（如 ETH、BTC、SOL）

### 5.2 当前状态信息

- **当前保证金**：当前账户余额/保证金
- **持仓**：当前持仓数量（绝对值）
- **挂单数量**：活跃订单的数量
- **挂单总量**：所有活跃订单的数量总和

### 5.3 盈亏计算

**绝对盈亏**：
```
PnL_absolute = 当前保证金 - 初始保证金
```

**百分比盈亏**：
```
PnL_percentage = (PnL_absolute / 初始保证金) × 100%
```

**注意事项**：
- 如果无法获取初始保证金或当前保证金，盈亏显示为 "N/A"
- 如果初始保证金为 0，百分比盈亏显示为 "N/A"

### 5.4 累计交易次数

- 每次成功执行 `_place_and_monitor_open_order()` 后，`trade_count` 加 1
- 在 `/status` 和 `/exit` 命令的响应中显示

## 六、安全机制

### 6.1 账户名称验证

`/exit` 命令要求提供账户名称，防止误操作：

- 必须与 `.env` 文件中的 `ACCOUNT_NAME` 完全匹配（不区分大小写）
- 如果不匹配，命令不会执行，返回错误消息

### 6.2 命令权限

- 只处理来自授权聊天（`TELEGRAM_CHAT_ID`）的消息
- 其他聊天的消息会被忽略

### 6.3 错误处理

- 所有命令执行都有异常捕获
- 错误会记录到日志
- 用户会收到错误通知

## 七、使用示例

### 7.1 启动机器人

```bash
python runbot.py --exchange backpack --ticker SOL --quantity 0.1 --take-profit 0.02
```

**Telegram 自动发送**：
```
🚀 交易机器人启动
Trading Bot Started

账户状态报告
Account Status Report

账户名称 Account Name: MAIN
交易所 Exchange: BACKPACK
初始保证金 Initial Margin: 1000.0000
交易对 Ticker: SOL

当前保证金 Current Margin: 1000.0000
持仓 Position: 0
挂单数量 Active Orders: 0
挂单总量 Order Size: 0

账户盈亏 PnL:
绝对盈亏 Absolute: +0.0000
百分比盈亏 Percentage: +0.00%
```

### 7.2 查询账户状态

**用户发送**：
```
/status
```

**机器人回复**：
```
账户状态报告
Account Status Report

账户名称 Account Name: MAIN
交易所 Exchange: BACKPACK
初始保证金 Initial Margin: 1000.0000
交易对 Ticker: SOL

当前保证金 Current Margin: 1005.5000
持仓 Position: 0.5
挂单数量 Active Orders: 3
挂单总量 Order Size: 0.3
累计交易次数 Total Trades: 15

账户盈亏 PnL:
绝对盈亏 Absolute: +5.5000
百分比盈亏 Percentage: +0.55%
```

### 7.3 退出程序

**用户发送**：
```
/exit MAIN
```

**机器人执行**：
1. 验证账户名称 ✓
2. 取消所有挂单 ✓
3. 平仓所有持仓 ✓
4. 返回最终状态 ✓
5. 程序退出 ✓

**机器人回复**：
```
账户状态报告
Account Status Report

账户名称 Account Name: MAIN
交易所 Exchange: BACKPACK
初始保证金 Initial Margin: 1000.0000
交易对 Ticker: SOL

当前保证金 Current Margin: 1005.5000
持仓 Position: 0
挂单数量 Active Orders: 0
挂单总量 Order Size: 0
累计交易次数 Total Trades: 15

账户盈亏 PnL:
绝对盈亏 Absolute: +5.5000
百分比盈亏 Percentage: +0.55%

所有交易已关闭，程序即将退出
All trades closed, program exiting...
```

## 八、配置要求

### 8.1 环境变量

需要在 `.env` 文件中配置：

```bash
# Telegram Bot 配置（必需）
TELEGRAM_BOT_TOKEN=your_bot_token_here
TELEGRAM_CHAT_ID=your_chat_id_here

# 账户名称（可选，用于多账户区分和 /exit 命令验证）
ACCOUNT_NAME=MAIN
```

### 8.2 获取配置信息

**获取 Bot Token**：
1. 在 Telegram 中搜索 `@BotFather`
2. 发送 `/newbot` 创建机器人
3. 获取 Bot Token

**获取 Chat ID**：
1. 向机器人发送任意消息
2. 访问：`https://api.telegram.org/bot{token}/getUpdates`
3. 查找 `"chat":{"id": ... }` 中的 `id` 值

详细步骤请参考：`docs/telegram-bot-setup.md`

## 九、注意事项

### 9.1 账户余额获取

- **Backpack**：已实现 `get_account_balance()` 方法
- **其他交易所**：需要根据各交易所 API 实现相应方法
- 如果无法获取余额，相关字段显示为 "N/A"

### 9.2 平仓和取消订单

- **Backpack**：支持 `cancel_all_orders()` 批量取消
- **其他交易所**：逐个取消订单
- 平仓优先使用市价单，否则使用限价单

### 9.3 命令处理

- 命令处理在后台异步任务中运行
- 不会阻塞主交易循环
- 命令执行失败不会影响交易

### 9.4 多账户支持

- 通过 `ACCOUNT_NAME` 区分不同账户
- `/exit` 命令需要提供正确的账户名称
- 每个账户的日志文件会包含账户名称

## 十、功能限制

### 10.1 当前限制

1. **账户余额**：目前仅 Backpack 完全支持，其他交易所需要实现
2. **批量取消**：仅 Backpack 支持批量取消，其他交易所逐个取消
3. **命令数量**：目前仅支持 `/status` 和 `/exit` 两个命令
4. **实时性**：账户状态查询需要调用交易所 API，可能有延迟

### 10.2 未来可扩展

- 支持更多命令（如 `/pause`、`/resume`、`/config` 等）
- 支持更多交易所的账户余额获取
- 支持更详细的交易统计
- 支持图表和可视化

## 十一、总结

Telegram 机器人已成功扩展为双向通信工具：

✅ **接收消息**：支持接收和处理用户命令  
✅ **命令处理**：支持 `/status` 和 `/exit` 命令  
✅ **启动上报**：程序启动时自动上报账户状态  
✅ **账户查询**：实时查询账户状态和盈亏  
✅ **远程控制**：支持远程关闭交易并退出程序  
✅ **安全验证**：账户名称验证防止误操作  

这些功能使得交易机器人更加智能和可控，用户可以随时了解账户状态，并在需要时安全地退出程序。

