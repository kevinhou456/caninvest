# 股票价格自动更新设置说明

## ⚠️ 状态说明

**当前状态：暂时禁用**
- 自动价格更新功能已实现但暂时关闭
- 所有代码已保留，可随时启用
- 如需启用，请按照下面的步骤操作

## 概述

系统支持自动股票价格更新，具有以下特性：
- **交易时间内**：每15分钟更新一次
- **非交易时间**：每1小时更新一次
- **防频率限制**：自动检测和避免API限制
- **智能缓存**：避免不必要的API调用

## 安装步骤

### 1. 安装依赖
```bash
pip install APScheduler>=3.10.0
```

### 2. 启用自动调度器

**方法一：修改配置文件**
编辑 `config.py`，将 `SCHEDULER_AUTO_START` 设为 `True`：
```python
# 在 DevelopmentConfig 类中
SCHEDULER_AUTO_START = True  # 启用自动调度器
```

**方法二：使用环境变量**
复制环境配置文件并修改：
```bash
cp .env.example .env
```

编辑 `.env` 文件：
```bash
# 启用调度器
SCHEDULER_AUTO_START=true

# 必须配置的API密钥
ALPHA_VANTAGE_API_KEY=你的API密钥

# 可选配置
MAX_DAILY_PRICE_REQUESTS=500
SCHEDULER_API_ENABLED=true
```

### 3. 获取Alpha Vantage API密钥

1. 访问：https://www.alphavantage.co/support/#api-key
2. 免费注册账户
3. 获取API密钥
4. 将密钥填入 `.env` 文件

## 系统架构

### 调度器 (`app/scheduler.py`)
- 使用APScheduler实现后台任务
- 自动启动和管理定时任务
- 智能判断交易时间和非交易时间

### 价格服务 (`app/services/stock_price_service.py`)
- 从Alpha Vantage API获取实时价格
- 智能缓存管理
- API频率限制保护

### 缓存模型 (`app/models/stocks_cache.py`)
- 智能更新策略：交易时间15分钟，非交易时间1小时
- 自动判断美股交易时间（周一至周五 9:30-16:00 ET）

## 任务调度详情

### 1. 交易时间价格更新
- **时间**: 周一至周五，UTC 14:30-21:00 (对应ET 9:30-16:00)
- **频率**: 每15分钟
- **限制**: 每次最多更新20只股票

### 2. 非交易时间价格更新
- **时间**: 非交易时间
- **频率**: 每1小时
- **限制**: 每次最多更新50只股票

### 3. 缓存清理
- **时间**: 每天凌晨2点UTC
- **功能**: 清理过期的价格缓存和日志

### 4. 数据库维护
- **时间**: 每周日凌晨3点UTC
- **功能**: 数据库优化和日志清理

## API接口

### 查看调度器状态
```bash
GET /api/v1/scheduler/status
```

### 手动触发价格更新
```bash
POST /api/v1/scheduler/trigger-price-update
Content-Type: application/json

{
  "symbols": ["AAPL", "TSLA"]  // 可选，不传则更新所有需要更新的股票
}
```

### 查看需要更新的股票
```bash
GET /api/v1/scheduler/stocks-needing-update
```

### 查看API使用情况
```bash
GET /api/v1/scheduler/api-usage
```

## 监控和调试

### 1. 检查调度器状态
```python
from app.scheduler import scheduler
status = scheduler.get_job_status()
print(status)
```

### 2. 手动执行价格更新
```python
from app.services.stock_price_service import StockPriceService
from app.models.stocks_cache import StocksCache

# 获取需要更新的股票
stocks = StocksCache.get_stocks_needing_update()
print(f"需要更新的股票数量: {len(stocks)}")

# 手动更新价格
service = StockPriceService()
results = service.update_stock_prices(['AAPL', 'TSLA'])
print(results)
```

### 3. 查看日志
调度器的活动会记录在应用日志中，搜索关键词：
- `"Starting stock price update"`
- `"Stock price update completed"`
- `"Task scheduler started"`

## 故障排除

### 1. API密钥问题
- 确保 `.env` 文件中的 `ALPHA_VANTAGE_API_KEY` 正确配置
- 检查API密钥是否有效
- 免费账户每天有500次请求限制

### 2. 调度器未启动
- 检查 `SCHEDULER_API_ENABLED` 是否为 `true`
- 查看启动日志是否有错误
- 确保不在测试环境（测试环境会禁用调度器）

### 3. 价格未更新
- 检查股票是否需要更新：调用 `stock.needs_price_update()`
- 查看API使用情况是否达到限制
- 检查网络连接和API响应

## 配置选项

| 配置项 | 默认值 | 说明 |
|--------|--------|------|
| `ALPHA_VANTAGE_API_KEY` | 无 | Alpha Vantage API密钥（必需） |
| `MAX_DAILY_PRICE_REQUESTS` | 500 | 每日最大API请求数 |
| `PRICE_CACHE_TTL` | 900 | 价格缓存时间（秒） |
| `SCHEDULER_API_ENABLED` | true | 是否启用调度器API |
| `SCHEDULER_AUTO_START` | **false** | **是否自动启动调度器（当前禁用）** |

## 注意事项

1. **API限制**: 免费Alpha Vantage账户每天限制500次请求
2. **时区处理**: 系统使用UTC时间，交易时间会自动转换为美东时间
3. **错误处理**: 系统会自动处理API错误和网络问题
4. **性能优化**: 只更新真正需要更新的股票，避免浪费API调用
5. **测试环境**: 测试环境下调度器会被禁用，避免消耗API配额

启动应用后，调度器会自动开始工作，按照设定的规则更新股票价格。