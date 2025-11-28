# 仓位不平衡处理方案与流程

## 一、概述

仓位不平衡是指实际持仓数量与系统记录的平仓订单总量不一致的情况。这种情况可能由以下原因导致：
- 订单部分成交但未完全记录
- 网络延迟导致订单状态更新不及时
- 交易所API返回数据不一致
- 手动干预或外部交易
- WebSocket消息丢失或延迟

本项目实现了两套仓位不平衡检测和处理机制：
1. **标准交易模式**（`trading_bot.py`）：单交易所仓位监控
2. **对冲模式**（`hedge_mode_*.py`）：跨交易所仓位监控

---

## 二、标准交易模式的仓位不平衡处理（已优化）

### 2.1 检测机制

**检测位置**：`trading_bot.py` 的 `_log_status_periodically()` 方法

**检测频率**：每 60 秒执行一次定期检查

**检测逻辑**：

> **⚠️ 重要更新**：标准交易模式的仓位不平衡处理已优化，新增以下功能：
> - 检测到不平衡后，断开交易所连接但不退出程序
> - 记录不平衡开始时间，超过 10 分钟自动修复
> - 自动修复功能：根据持仓与订单差异自动调整

```363:415:trading_bot.py
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
```

### 2.2 检测公式

**不平衡判断条件**：
```
|实际持仓数量 - 活跃平仓订单总量| > 2 × 单笔订单数量
```

**关键变量**：
- `position_amt`：账户实际持仓数量（绝对值）
- `active_close_amount`：所有活跃平仓订单的数量总和
- `self.config.quantity`：单笔订单数量（阈值基准）

**阈值说明**：
- 使用 `2 × quantity` 作为容差，允许一定程度的误差
- 如果差值超过此阈值，认为存在严重不平衡

### 2.3 处理流程（优化后）

#### 步骤 1：首次检测到不平衡

当首次检测到仓位不平衡时，执行以下操作：

1. **记录不平衡开始时间**：
   - 设置 `self.mismatch_start_time = time.time()`
   - 设置 `self.mismatch_detected = True`

2. **生成错误消息**：
   - 包含交易所和交易对信息
   - 显示当前持仓数量
   - 显示活跃平仓订单总量
   - 显示活跃平仓订单数量
   - **提示系统将在 10 分钟后自动尝试修复**

3. **记录错误日志**：
   - 使用 `ERROR` 级别记录到日志文件
   - 日志文件位置：`logs/{exchange}_{ticker}_activity.log`

4. **发送通知**：
   - 如果配置了 Telegram Bot，发送错误消息
   - 如果配置了 Lark Bot，发送错误消息
   - 通知内容包含中英文提示

#### 步骤 2：断开连接但不退出程序

在主交易循环中，检测到不平衡后的处理：

```705:717:trading_bot.py
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
```

**关键逻辑**：
- **不断开程序**：程序继续运行，不退出
- **断开交易所连接**：停止交易，但保持程序运行
- **定期检查**：每 60 秒重新连接检查状态
- **等待修复**：等待手动修复或自动修复

#### 步骤 3：持续监控和自动修复

系统会持续监控不平衡状态：

1. **每 60 秒检查一次**：
   - 临时重新连接交易所
   - 检查持仓和订单状态
   - 检查是否已修复
   - 检查不平衡持续时间

2. **超过 10 分钟自动修复**：
   ```420:422:trading_bot.py
                        if mismatch_duration >= 600:  # 10 minutes = 600 seconds
                            self.logger.log(f"Position mismatch duration: {mismatch_duration:.0f} seconds, attempting auto-fix...", "INFO")
                            await self._auto_fix_position_mismatch(position_amt, active_close_amount)
   ```

3. **自动修复逻辑**：
   - **如果持仓 > 订单总和**：平掉多余的持仓
   - **如果持仓 < 订单总和**：增加持仓（开仓）并下对应的平仓单

#### 步骤 4：修复后重新连接

当不平衡被修复后（手动或自动）：

```719:725:trading_bot.py
                # If mismatch was resolved, reconnect
                if self.position_mismatch_disconnected and not mismatch_detected:
                    self.logger.log("Position mismatch resolved. Reconnecting to exchange...", "INFO")
                    await self.exchange_client.connect()
                    await asyncio.sleep(5)
                    self.position_mismatch_disconnected = False
                    continue
```

**关键逻辑**：
- 检测到已修复后，自动重新连接交易所
- 恢复正常交易流程
- 发送修复成功通知

### 2.4 处理流程图（优化后）

```
┌─────────────────────────────────┐
│  主循环运行（每60秒检查一次）      │
└────────────┬────────────────────┘
             │
             ▼
┌─────────────────────────────────┐
│ _log_status_periodically()      │
│ 1. 临时重连（如已断开）           │
│ 2. 获取活跃订单                   │
│ 3. 过滤平仓订单                   │
│ 4. 获取实际持仓                   │
│ 5. 计算平仓订单总量               │
│ 6. 断开连接（如仅用于检查）       │
└────────────┬────────────────────┘
             │
             ▼
┌─────────────────────────────────┐
│ 计算差值：                        │
│ |position - close_orders|        │
└────────────┬────────────────────┘
             │
      ┌──────┴──────┐
      │             │
      ▼             ▼
   > 2×quantity  ≤ 2×quantity
      │             │
      │             └──► 正常，继续交易
      │                    │
      │                    └──► 如之前不平衡，重新连接
      │
      ▼
┌─────────────────────────────────┐
│ 首次检测到不平衡：                │
│ 1. 记录开始时间                   │
│ 2. 记录ERROR日志                  │
│ 3. 发送通知（Telegram/Lark）      │
│ 4. 断开交易所连接                 │
│ 5. 设置 position_mismatch_       │
│    disconnected = True            │
└────────────┬────────────────────┘
             │
             ▼
┌─────────────────────────────────┐
│ 程序继续运行（不退出）            │
│ 每60秒检查一次状态                │
└────────────┬────────────────────┘
             │
             ▼
┌─────────────────────────────────┐
│ 检查不平衡持续时间：              │
│ duration = now - start_time      │
└────────────┬────────────────────┘
             │
      ┌──────┴──────┐
      │             │
      ▼             ▼
   ≥ 10分钟      < 10分钟
      │             │
      │             └──► 继续等待，记录剩余时间
      │
      ▼
┌─────────────────────────────────┐
│ 自动修复：                        │
│ _auto_fix_position_mismatch()    │
└────────────┬────────────────────┘
             │
             ▼
┌─────────────────────────────────┐
│ 安全检查：                        │
│ |差异| > quantity ?              │
└────────────┬────────────────────┘
             │
      ┌──────┴──────┐
      │             │
      ▼             ▼
     是             否
      │             │
      │             └──► 继续修复流程
      │
      ▼
┌─────────────────────────────────┐
│ 数据异常，安全退出：              │
│ 1. 记录严重错误                  │
│ 2. 发送通知                      │
│ 3. 断开连接                      │
│ 4. 退出程序                      │
└─────────────────────────────────┘

（如果通过安全检查）
             │
             ▼
      ┌──────┴──────┐
      │             │
      ▼             ▼
   持仓>订单      持仓<订单
      │             │
      │             └──► 仅开仓（不下平仓单）
      │
      ▼
┌─────────────────────────────────┐
│ 平掉多余持仓                      │
│ （使用市价单或限价单）             │
└────────────┬────────────────────┘
             │
             ▼
┌─────────────────────────────────┐
│ 修复完成                          │
│ 重置 mismatch_detected           │
└────────────┬────────────────────┘
             │
             ▼
┌─────────────────────────────────┐
│ 重新连接交易所                     │
│ 恢复正常交易                       │
└─────────────────────────────────┘
```

### 2.5 自动修复机制详解

#### 2.5.1 修复触发条件

- **时间条件**：仓位不平衡持续超过 10 分钟（600 秒）
- **自动执行**：无需用户干预，系统自动尝试修复

#### 2.5.1.1 修复前安全检查 ⚠️

在自动修复之前，系统会进行安全检查，防止因交易所数据异常导致的错误修复：

**检查条件**：
```
|持仓数量 - 平仓订单总和| > quantity（程序启动时设置的每次下单量）
```

**检查逻辑**：
- 如果差异绝对值 > `quantity`，认为可能是交易所数据异常
- **安全措施**：
  1. 记录严重错误日志
  2. 发送通知警报
  3. 断开交易所连接
  4. 退出主程序（`shutdown_requested = True`）

**代码实现**：
```465:490:trading_bot.py
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
```

**设计原因**：
- 如果差异过大（超过单次下单量），可能是：
  - 交易所 API 返回错误数据
  - 网络问题导致数据不一致
  - 交易所系统异常
- 在这种情况下自动修复可能导致更大的损失
- 因此选择安全退出，让用户手动检查和处理

#### 2.5.2 修复策略

**情况 1：持仓数量 > 平仓订单总和**

```
差值 = 持仓数量 - 平仓订单总和
修复操作：平掉多余的持仓
```

- 优先使用市价单（如果交易所支持）
- 否则使用限价单（价格略低于/高于市价以确保成交）
- 平仓方向：与机器人方向相反（`close_order_side`）

**情况 2：持仓数量 < 平仓订单总和**

```
差值 = 平仓订单总和 - 持仓数量
修复操作：仅增加持仓（开仓），不下新的平仓单
```

- 优先使用市价单开仓（如果交易所支持）
- **重要**：只开仓，不下新的平仓单
- 原因：现有的平仓订单已经足够，开仓后即可平衡
- 如果开仓后再下平仓单，会导致新的不平衡（持仓 = 平仓订单总和，但平仓订单过多）
- 开仓方向：与机器人方向相同（`direction`）

#### 2.5.3 修复流程

```445:553:trading_bot.py
    async def _auto_fix_position_mismatch(self, position_amt: Decimal, active_close_amount: Decimal):
        """Automatically fix position mismatch by adjusting position or orders."""
        try:
            self.logger.log("Starting automatic position mismatch fix...", "INFO")
            
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
                # ... 平掉多余持仓的逻辑 ...
            elif difference < 0:
                # Position < Orders: Need to increase position
                # Only open position, do NOT place new close order
                # Because existing close orders are already enough
                # ... 仅开仓的逻辑（不下平仓单）...
```

### 2.6 用户操作指南（优化后）

#### 选项 1：等待自动修复（推荐）

1. **无需操作**：程序会自动在 10 分钟后尝试修复
2. **监控日志**：查看 `logs/{exchange}_{ticker}_activity.log` 了解修复进度
3. **接收通知**：如果配置了通知，会收到修复结果通知

#### 选项 2：手动修复

如果不想等待 10 分钟，可以手动修复：

1. **查看错误日志**：
   - 检查 `logs/{exchange}_{ticker}_activity.log` 文件
   - 查看具体的持仓和订单信息

2. **手动检查账户**：
   - 登录交易所账户
   - 查看实际持仓数量
   - 查看活跃订单列表

3. **手动平衡**：
   - **如果持仓 > 平仓订单总量**：需要补充平仓订单或平掉多余持仓
   - **如果持仓 < 平仓订单总量**：需要取消多余的平仓订单或增加持仓
   - 确保：`|实际持仓 - 平仓订单总量| ≤ 2 × quantity`

4. **程序自动恢复**：
   - 程序每 60 秒检查一次
   - 检测到已修复后，会自动重新连接并恢复交易
   - **无需重启程序**

---

## 三、对冲模式的仓位不平衡处理

### 3.1 检测机制

**检测位置**：各对冲模式实现文件（如 `hedge_mode_bp.py`、`hedge_mode_ext.py` 等）

**检测频率**：每次交易循环开始前

**检测逻辑**：

对冲模式检查两个交易所的仓位总和是否平衡：

```1113:1119:hedge/hedge_mode_bp.py
            while self.backpack_position <= self.max_position and not self.stop_flag:
                self.lighter_position = self.get_lighter_position()
                self.backpack_position = await self.get_backpack_position()
                self.logger.info(f"Buying up to {self.max_position} | Backpack position: {self.backpack_position} | Lighter position: {self.lighter_position}")
                if abs(self.backpack_position + self.lighter_position) > self.order_quantity*2:
                    self.logger.error(f"❌ Position diff is too large: {self.backpack_position + self.lighter_position}")
                    sys.exit(1)
```

### 3.2 检测公式

**不平衡判断条件**：
```
|主交易所持仓 + Lighter持仓| > 2 × 单笔订单数量
```

**关键变量**：
- `主交易所持仓`：Backpack/Extended/Apex/GRVT/EdgeX 的持仓（通常为正数，表示多头）
- `Lighter持仓`：Lighter 的持仓（通常为负数，表示空头，用于对冲）
- `order_quantity`：单笔订单数量

**原理说明**：
- 理想情况下，两个交易所的仓位应该完全对冲，总和接近 0
- 如果总和超过阈值，说明对冲不平衡

### 3.3 处理流程

#### 步骤 1：检测到不平衡

1. **记录错误日志**：
   - 使用 `ERROR` 级别记录
   - 显示两个交易所的持仓信息
   - 显示差值

2. **立即退出程序**：
   - 调用 `sys.exit(1)` 强制退出
   - 不执行优雅关闭（因为可能处于异常状态）

#### 步骤 2：不同对冲模式的实现

**Backpack 对冲模式**：
```1117:1119:hedge/hedge_mode_bp.py
                if abs(self.backpack_position + self.lighter_position) > self.order_quantity*2:
                    self.logger.error(f"❌ Position diff is too large: {self.backpack_position + self.lighter_position}")
                    sys.exit(1)
```

**Extended 对冲模式**：
```1113:1115:hedge/hedge_mode_ext.py
            if abs(self.extended_position + self.lighter_position) > self.order_quantity*2:
                self.logger.error(f"❌ Position diff is too large: {self.extended_position + self.lighter_position}")
                break
```

**EdgeX 对冲模式**：
```1117:1119:hedge/hedge_mode_edgex.py
            if abs(self.edgex_position + self.lighter_position) > self.order_quantity*2:
                self.logger.error(f"❌ Position diff is too large: {self.edgex_position + self.lighter_position}")
                break
```

**Apex 对冲模式**：
```984:986:hedge/hedge_mode_apex.py
            if abs(self.apex_position + self.lighter_position) > self.order_quantity*2:
                self.logger.error(f"❌ Position diff is too large: {self.apex_position + self.lighter_position}")
                break
```

### 3.4 处理流程图

```
┌─────────────────────────────────┐
│  交易循环开始                     │
└────────────┬────────────────────┘
             │
             ▼
┌─────────────────────────────────┐
│ 获取两个交易所的持仓：             │
│ 1. 主交易所持仓（如Backpack）      │
│ 2. Lighter持仓                   │
└────────────┬────────────────────┘
             │
             ▼
┌─────────────────────────────────┐
│ 计算对冲差值：                    │
│ |主交易所持仓 + Lighter持仓|       │
└────────────┬────────────────────┘
             │
      ┌──────┴──────┐
      │             │
      ▼             ▼
   > 2×quantity  ≤ 2×quantity
      │             │
      │             └──► 正常，继续交易循环
      │
      ▼
┌─────────────────────────────────┐
│ 检测到不平衡：                    │
│ 1. 记录ERROR日志                  │
│ 2. 立即退出程序（sys.exit/break）  │
└────────────┬────────────────────┘
             │
             ▼
┌─────────────────────────────────┐
│ 程序终止                          │
│ 提示：检查两个交易所的仓位         │
└─────────────────────────────────┘
```

### 3.5 用户操作指南

当对冲模式检测到仓位不平衡时：

1. **查看错误日志**：
   - 检查对应的对冲模式日志文件
   - 查看两个交易所的具体持仓

2. **检查两个交易所账户**：
   - 登录主交易所（Backpack/Extended/Apex等）
   - 登录 Lighter 交易所
   - 分别查看持仓数量

3. **手动平衡**：
   - 如果 `主交易所持仓 + Lighter持仓 > 0`：需要在 Lighter 增加空头仓位
   - 如果 `主交易所持仓 + Lighter持仓 < 0`：需要在主交易所增加多头仓位
   - 确保：`|主交易所持仓 + Lighter持仓| ≤ 2 × order_quantity`

4. **重新启动**：
   - 平衡完成后，重新运行对冲模式
   - 系统会重新开始交易循环

---

## 四、关键设计要点

### 4.1 阈值选择

**标准模式**：`2 × quantity`
- 允许一定程度的误差和延迟
- 避免因网络延迟导致的误报
- 如果超过此阈值，说明存在严重问题

**对冲模式**：`2 × order_quantity`
- 同样使用 2 倍订单数量作为阈值
- 考虑到两个交易所的同步延迟

### 4.2 检测频率

**标准模式**：每 60 秒
- 平衡检测精度和性能
- 避免过于频繁的API调用
- 及时发现严重不平衡

**对冲模式**：每次循环
- 在每次交易前检查
- 确保交易前状态正确
- 防止在异常状态下继续交易

### 4.3 处理策略

**标准模式**：
- 优雅关闭：断开连接，保存状态
- 发送通知：及时提醒用户
- 停止交易：防止进一步恶化

**对冲模式**：
- 立即退出：避免在异常状态下继续
- 简单直接：不执行复杂恢复逻辑
- 依赖手动干预：用户需要手动平衡

### 4.4 容错机制

1. **异常捕获**：
   - 检测过程中的异常会被捕获
   - 记录错误日志，但不中断主循环

2. **重试机制**：
   - 使用 `query_retry` 装饰器处理API调用失败
   - 自动重试，提高可靠性

3. **状态同步**：
   - 定期同步订单状态
   - 通过WebSocket实时更新

---

## 五、常见问题与解决方案

### Q1: 为什么使用 `2 × quantity` 作为阈值？

**A**: 
- 允许单笔订单的误差范围
- 考虑网络延迟和API响应时间差
- 如果超过 2 倍，说明不是正常误差，而是真正的不平衡

### Q2: 检测到不平衡后，系统会自动修复吗？

**A**: 
- **不会**。系统只负责检测和报警
- 需要用户手动检查并平衡仓位
- 这是为了避免自动修复可能带来的风险

### Q3: 如何预防仓位不平衡？

**A**:
1. 确保网络连接稳定
2. 避免手动干预交易
3. 定期检查日志
4. 使用稳定的API密钥
5. 避免在系统运行时进行外部操作

### Q4: 对冲模式中，为什么两个持仓相加应该接近 0？

**A**:
- 对冲策略的核心是同时持有相反方向的仓位
- 主交易所持有多头（正数），Lighter持有空头（负数）
- 理想情况下，两者应该完全对冲，总和为 0
- 如果总和不为 0，说明对冲不完整，存在风险

### Q5: 如果频繁出现不平衡，应该怎么办？

**A**:
1. 检查网络连接质量
2. 检查交易所API状态
3. 增加检测频率（修改代码）
4. 调整阈值（不推荐，可能掩盖问题）
5. 联系技术支持

### Q6: 为什么在修复前要检查差异是否大于 quantity？

**A**:
- **安全考虑**：如果差异过大（超过单次下单量），可能是交易所数据异常
- **防止错误修复**：在数据异常的情况下自动修复可能导致更大的损失
- **用户干预**：差异过大时，系统会安全退出，让用户手动检查和处理
- **保护资金**：避免在异常情况下执行可能错误的交易操作

### Q7: 如果差异大于 quantity，程序会如何处理？

**A**:
1. **记录严重错误**：记录包含详细信息的错误日志
2. **发送通知**：通过 Telegram/Lark 发送严重错误通知
3. **断开连接**：断开与交易所的连接
4. **退出程序**：设置 `shutdown_requested = True`，程序安全退出
5. **用户处理**：需要用户手动检查交易所账户和系统状态

---

## 六、总结

仓位不平衡检测是交易机器人的重要安全机制，通过定期检查实际持仓与系统记录的一致性，及时发现异常情况并采取保护措施。

**标准模式（已优化）**：
- 检测频率：每 60 秒
- 检测对象：单交易所的持仓与平仓订单
- 处理方式：
  - **断开交易所连接但不退出程序**
  - **记录不平衡开始时间**
  - **超过 10 分钟自动修复**
  - **修复后自动重新连接并恢复交易**
- 优势：
  - 程序持续运行，无需重启
  - 自动修复功能减少人工干预
  - 修复后自动恢复交易

**对冲模式**：
- 检测频率：每次交易循环
- 检测对象：两个交易所的持仓总和
- 处理方式：立即退出，记录错误
- 说明：对冲模式仍需要手动干预

### 优化亮点

1. **程序不退出**：检测到不平衡后，程序继续运行，只断开交易所连接
2. **自动修复**：超过 10 分钟自动尝试修复，减少人工干预
3. **智能修复**：根据持仓与订单差异，自动选择平仓或开仓
4. **自动恢复**：修复完成后自动重新连接并恢复交易
5. **持续监控**：即使断开连接，仍定期检查状态

