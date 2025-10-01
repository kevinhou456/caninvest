# Claude Code 系统架构说明

## 核心原则

**⚠️ 重要：所有资产计算必须使用统一函数，禁止重复实现计算逻辑**
**⚠️ 重要：代码一定要模块化，不要有重复代码，一定要易于维护**

## 统一计算架构

### 唯一数据源
所有资产相关计算必须使用以下统一函数：

1. **股票持仓计算**：`Portfolio Service.get_portfolio_summary()`
   - 返回：`total_current_value`（纯股票市值，不含现金）
   - 包含：持仓、成本、收益、收益率等

2. **现金余额计算**：`Asset Valuation Service.get_cash_balance()`
   - 返回：`total_cad`（现金总额，CAD等值）
   - 包含：USD、CAD分离金额

3. **总资产计算公式**：
   ```
   总资产 = Portfolio Service股票价值 + Asset Valuation现金余额
   ```

### 禁止的做法
- ❌ 直接使用 `AssetValuationService.get_asset_snapshot().total_assets`
- ❌ 在各个服务中重复实现资产计算逻辑
- ❌ 假设任何服务的 `total_current_value` 包含现金

## 服务架构说明

### 核心服务职责

1. **Portfolio Service** (`app/services/portfolio_service.py`)
   - 负责：股票持仓计算、FIFO成本计算、收益计算
   - 核心函数：`get_portfolio_summary(account_ids, period, start_date, end_date)`
   - 数据源：Transaction表 + 股票价格

2. **Asset Valuation Service** (`app/services/asset_valuation_service.py`)
   - 负责：现金余额计算、货币转换
   - 核心函数：`get_cash_balance(account_id, target_date)`
   - 数据源：Cash表 + Transaction表（历史计算）

3. **Daily Stats Service** (`app/services/daily_stats_service.py`)
   - 负责：日度统计展示
   - **必须使用**：统一计算架构（Portfolio + Cash）
   - 禁止：自行实现资产计算

### 页面功能映射

| 页面功能 | 使用的核心函数 | 说明 |
|---------|---------------|------|
| Performance Comparison | Portfolio Service.get_performance_comparison() | 内部使用统一计算架构 |
| 每日分析 | Daily Stats Service.get_monthly_calendar_data() | 调用统一计算架构 |
| Holdings持仓 | Portfolio Service.get_portfolio_summary() | 直接使用核心函数 |
| 月度统计 | Portfolio Service各种分析函数 | 基于统一计算架构 |

## 常见错误模式

### 数据不一致错误
**症状**：不同页面显示相同账户的资产金额不一致
**原因**：某个服务没有使用统一计算架构，自行实现了重复逻辑
**解决**：检查该服务是否正确调用 `get_portfolio_summary()` + `get_cash_balance()`

### "陡降"或异常数据
**症状**：Performance Comparison或图表显示异常的数据跳跃
**原因**：
1. 某个服务使用了错误的数据源（如直接用asset_snapshot.total_assets）
2. 现金计算逻辑不统一
**解决**：确保使用统一计算架构，不要打补丁掩盖问题

### 现金计算错误
**症状**：现金显示为0或异常金额
**原因**：
1. 误用了 `total_current_value` 当作包含现金的总资产
2. 没有单独调用 `get_cash_balance()`
**解决**：明确分离股票和现金的计算

## 开发规范

### 新功能开发
1. **禁止重新实现计算逻辑**
2. 所有资产相关功能必须基于统一计算架构
3. 如需扩展计算能力，修改核心函数而非创建新函数

### 调试原则
1. 数据不一致时，检查是否有服务偏离了统一架构
2. 不要通过fallback、缓存等方式掩盖计算逻辑问题
3. 优先修复根因，而非症状

### 代码审查要点
- 是否使用了统一计算函数？
- 是否假设某个total_value包含现金？
- 是否重复实现了已有的计算逻辑？

## 数据库架构要点

### 关键表结构
- **Transaction表**：所有交易记录，Portfolio Service的数据源
- **Cash表**：当前现金余额，Asset Valuation Service的数据源
- **StockPriceHistory表**：历史股票价格

### 现金计算逻辑
- **今日现金**：直接从Cash表读取，没有记录的话就当CAD和USD现金都是0
- **历史现金**：通过Transaction表反推计算，如果反推出持有现金是负数没关系，因为用户没有输入当前现金还有存入取出的交易记录很正常，用负数计算就是了
- **多币种**：CAD和USD都是分别计算，只有汇总的时候才会用当前汇率把USD资产转为CAD

## 性能注意事项

### Portfolio Service调用
- `get_portfolio_summary()` 涉及大量数据库查询
- 对于频繁调用的场景，考虑适当缓存
- 避免在循环中重复调用

### Asset Valuation Service调用
- 现金计算相对轻量，可频繁调用
- 历史日期的现金计算较重，需要事务记录反推

## 测试验证

### 数据一致性验证
```python
# 验证统一计算架构
portfolio_data = portfolio_service.get_portfolio_summary([account_id], TimePeriod.CUSTOM, date, date)
stock_value = portfolio_data['summary']['total_current_value']

cash_balance = asset_service.get_cash_balance(account_id, date)
cash_value = cash_balance['total_cad']

total_assets = stock_value + cash_value
# 这个总资产应该与所有其他服务返回的总资产一致
```

CAD和USD资产是分别计算汇总的，比如总资产，加币总资产=当前持有的加币计价股票市值+持有的加币现金，美元总资产=当前持有的美元计价
的股票市值+持有的美元现金， 总资产=加币总资产+美元总资产*当前汇率。

所有计算汇总要用统一的逻辑，不要搞重复代码，模块化可扩展，代码一定要容易维护

## 关于股票历史价格和当前价格的获取

无论是当前价还是历史价，统一通过 yfinance/Yahoo Finance 拉取，保证兼容性。
利用缓存数据库表来缓存获得股票历史价格和当前价格，只有真缺时才触发外部请求，避免频繁访问被封禁。
针对“看起来是交易日却没返回数据”的情况，新增休市推断机制：
每次批量取历史数据时，只有成功获取这个时间段的数据才附加一下判断，如果遇到某个非周末日期缺数据，并且这个日期之前的日期有数据，之后的日期也有数据才会在 stock_holiday_attempts 记一笔。
当同一天累计超过 5 只股票缺数据，就将该日期写入 market_holidays，并标注对应市场（US/CA）。
后续查询先检查 market_holidays，命中则直接认定休市，不再去外部拉取。
IPO 情况：存在首日交易逻辑，我会先确认它是否真正生效（例如从第一笔交易日/first_trade_date 设定历史抓取的下限），必要时补上与 yfinance 调用的联动。

## 历史问题记录

### 2025-09-22 修复记录
**问题**：Performance Comparison显示账户7陡降，Daily Stats Service数据不一致
**根因**：
1. Daily Stats Service强制设置现金为0
2. Performance Comparison直接使用AssetValuationService.get_asset_snapshot()
3. 两个服务没有使用统一计算架构

**解决方案**：
1. 重构Daily Stats Service使用统一计算：Portfolio Service + Cash Balance
2. 重构Performance Comparison使用统一计算架构
3. 验证所有服务数据一致性

**教训**：永远不要通过fallback机制掩盖计算逻辑不统一的问题

---

## Claude重启后必读
1. 优先理解统一计算架构，不要假设任何服务的计算逻辑
2. 遇到数据不一致问题，优先检查是否偏离了统一架构
3. 不要重复实现已有的计算逻辑
4. 修复问题时优先修复根因，而非打补丁