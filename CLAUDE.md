# Claude Code 系统架构说明

## 核心原则

**⚠️ 重要：所有资产计算必须使用统一函数，禁止重复实现计算逻辑**
**⚠️ 重要：代码一定要模块化，不要有重复代码，一定要易于维护**
**⚠️ 重要：整个系统统一使用平均成本法（ACB / Average Cost Basis），严禁使用 FIFO**

## 成本计算方法：平均成本法（ACB）

### 规则
- 全系统所有股票的成本计算**只能用平均成本法（ACB）**，不允许使用 FIFO（先进先出）
- 买入时：`总成本 += 净买入金额`，`持仓数量 += 买入数量`
- 卖出时：`成本基础 = (总成本 / 持仓数量) × 卖出数量`，然后 `总成本 -= 成本基础`
- 已实现收益：`卖出净收入 - 成本基础`

### 正确示例
```python
# 买入
current_shares += quantity
total_cost += net_cost  # quantity * price + fee

# 卖出
if current_shares > 0:
    avg_cost = total_cost / current_shares
    cost_basis = avg_cost * quantity
    total_cost -= cost_basis
    realized_gain += net_proceeds - cost_basis
current_shares -= quantity
```

### 禁止的做法
- ❌ 使用 `buy_lots` 列表跟踪买入批次
- ❌ 使用 FIFO 队列（`buy_lots.pop(0)` 等方式）计算成本
- ❌ 新增任何基于批次的成本计算逻辑

### 已实现的文件（勿改回 FIFO）
- `app/models/transaction.py` — `get_portfolio_summary()`
- `app/services/portfolio_service.py` — `_process_transaction_acb()`
- `app/services/asset_valuation_service.py` — `_calculate_stock_stats()`、`_calculate_stock_statistics()`、`_calculate_stock_realized_gain()`、`_calculate_cost_basis_breakdown()`、`_calculate_realized_gain_for_account()`
- `app/services/holdings_service.py` — `AccountHolding.add_sell_transaction()`

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

## 报表缓存版本号规则

`app/api/reports.py` 中有常量 `_CACHE_SCHEMA_VERSION`，用于使旧缓存自动失效。

**必须将 `_CACHE_SCHEMA_VERSION` 加 1 的情况**（修改了计算结果）：
- `portfolio_service.py` 中的 FIFO 成本、收益率、年度汇总、持仓过滤等计算逻辑
- `get_annual_analysis`、`get_portfolio_summary` 等核心函数的输出结构或字段变化
- 任何会导致同一输入产生不同数值结果的改动

**不需要加 1 的情况**（不影响计算结果）：
- UI 模板、路由、日志、注释的修改
- 缓存系统本身的改动
- 性能优化但输出结果不变

---

## T5008 与 T3 Box 42 (ROC) 功能说明

### 功能概述
T5008 是加拿大税务报表，记录证券出售记录（收益/损失）。T3 Box 42 (Return of Capital，资本返还) 会降低股票的调整成本基础 (ACB)，影响最终的资本增益/损失计算。

### 数据模型
- **`T3Box42` 表** (`app/models/t3_box42.py`)：每条记录对应一个账户一只股票一个年度的 ROC 金额
  - 唯一约束：`(account_id, stock, year)`
  - 货币：T3 Box 42 永远是 CAD，不需要货币转换

### ACB 计算逻辑（`app/api/reports.py`）

**核心函数**：`_compute_t5008_for_accounts(account_ids_list, year_start, year_end, account_name_map)`
- 将多个账户的交易记录合并，用加权平均成本法（非 FIFO）计算联合 ACB
- 同一所有权组内的账户视为同一持有人，BUY/SELL/ROC 全部混合计算
- ACB 下限为 0（ROC 不能使 ACB 变成负数）

**ROC 插入时机**（函数 `_roc_insertion_sort_key`）：
- **清仓年度 + 当年有买入**：ROC 插入到该年最后一笔 BUY 之后（排序键：`(最后买入日期, 买入id, 1)`）
- **清仓年度 + 当年无买入（最后买入在前年）**：ROC 插入到清仓年度 1 月 1 日（排序键：`(date(year,1,1), -1, 0)`）
- **非清仓年度**：ROC 插入到该年 12 月 31 日（排序键：`(date(year,12,31), 10**9, 1)`）

**联合 ACB 标记（`joint_acb`）**：
- 当同一持有期内来自多个账户的 BUY，或多个账户都有该年 T3 ROC 时，`joint_acb=True`
- 前端将该行成本格显示为红色加粗

**每条卖出记录包含的字段**：
`date, symbol, quantity, proceeds, acb, original_acb, gain, currency, roc_adjusted, roc_applied_years, account_id, account_name, joint_acb`

**`roc_by_year` 数据结构**（内部跟踪每年 ROC）：
```python
roc_by_year[year] = {
    'amount': 0.0,
    'notes': [],
    'account_ids': set(),      # 哪些账户贡献了这年的 ROC
    'per_account': {}          # account_id -> amount（各账户金额明细）
}
```

**`t3_all_records[symbol]`** 返回给前端的结构：
```python
[{
    'year': int,
    'amount': float,           # 合并后的总金额
    'notes': str,
    'breakdown': [             # 仅当多账户贡献时存在
        {'account_name': str, 'amount': float}
    ]
}]
```

### API 接口（`get_t5008_data()`）

**请求方式**：
- `?member_id=X&year=YYYY`：按成员查询，自动按所有权分组（个人账户 + 联合账户各自独立）
- `?account_id=X&year=YYYY` 或 `?account_ids=X,Y&year=YYYY`：指定账户（向后兼容）

**所有权分组逻辑**：
1. 查询 `AccountMember` 表，得到每个账户的 `frozenset(member_ids)`
2. 跳过免税账户（`account_type.tax_advantaged=True`）
3. 相同 `frozenset` 的账户合并为一组
4. 排序：个人账户（frozenset 长度=1）在前，联合账户在后
5. 标签：个人→`'个人账户'`，联合→`'联合 (Partner1)'` 或 `'联合 (P1, P2)'`

**返回格式**：
```json
{
  "success": true,
  "year": 2024,
  "title": "...",
  "annual_usd_cad_rate": 1.3500,
  "groups": [
    {
      "label": "个人账户",
      "records": [...],
      "t3_all_records": {"SYMBOL": [...]}
    },
    {
      "label": "联合 (User2)",
      "records": [...],
      "t3_all_records": {...}
    }
  ]
}
```

### 前端渲染（`app/templates/investment/annual_stats.html`）

**入口**：`showT5008(year, target)`
- `target = {memberId: X}`：全账户视图，按成员分组
- `target = {accountIds: [X,...]}`：单账户视图

**每个分组渲染**：`buildGroupTable(group)`
- 多账户组（`multiAcct`）：显示"账户名"列
- `joint_acb=true` 的行：ACB 格文字红色加粗
- `roc_adjusted=true`：ACB 格附加 `(*)` 标记，悬浮显示 Tooltip

**Tooltip**（`buildRocTooltip(r, t3All)`）：
- 第一行：计算公式，如 `$180.00 = $230.00 - $30.00(22) - $20.00(23)`
- 分隔线
- `T3 Box42 全部记录:` 各年份金额+备注
- 若多账户贡献同一年 ROC，在该年下方缩进显示各账户明细：`  AccountA: $50.00`

**Bootstrap Tooltip 注意事项**：
- 必须设置 `data-bs-html="true"` 和 `sanitize: false` 才能渲染 HTML
- CSS 类 `.tooltip-roc .tooltip-inner { max-width: 480px; white-space: nowrap; text-align: left; }`
- HTML 特殊字符在 `data-bs-title` 里用 `&lt;br&gt;` 而非 `<br>`，最后用 `.replace(/"/g, '&quot;')` 转义引号

**T5008 按钮位置**：
- 单账户年度统计页：主行显示
- 全账户年度统计页：每个成员的汇总行（`is_member_row`）显示，`target={memberId: X}`

### 重要业务规则
1. **ACB 合并前提**：只有所有权完全相同（同一 `frozenset(member_ids)`）的账户才合并计算 ACB
2. **免税账户排除**：TFSA、RRSP 等 `tax_advantaged=True` 的账户不参与 T5008 计算
3. **持仓清零重置**：当某股票持仓归零时，`buy_accounts_in_period`、`roc_joint_in_period` 等状态全部重置
4. **ROC 上限**：ROC 不能使 ACB 降到 0 以下，超出部分直接按 0 处理（超出部分视为资本增益，但本系统暂不单独报告）
5. **货币**：T3 Box 42 只有 CAD；T5008 计算按原始货币，汇总展示时用当年年均汇率换算为 CAD

---

## Claude重启后必读
1. 优先理解统一计算架构，不要假设任何服务的计算逻辑
2. 遇到数据不一致问题，优先检查是否偏离了统一架构
3. 不要重复实现已有的计算逻辑
4. 修复问题时优先修复根因，而非打补丁