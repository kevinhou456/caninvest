# Canadian Family Investment Management System

一个为加拿大家庭设计的股票投资管理系统，支持多账户、多成员、联名账户管理，以及TFSA、RRSP等税收优惠账户的供款跟踪。

## 功能特性

### 核心功能
- 🏠 **家庭投资管理**: 支持多成员、多账户管理
- 💰 **账户类型支持**: TFSA、RRSP、普通投资账户
- 👥 **联名账户**: 支持联名账户及出资比例管理
- 🌍 **多语言支持**: 英语和简体中文界面
- 💹 **多币种**: 支持CAD/USD股票交易

### 数据管理
- 📊 **智能导入**: CSV文件自动识别券商格式
- 📷 **OCR识别**: 交易截图智能识别录入
- 🏷️ **股票分类**: 可自定义的股票分类系统
- 📝 **交易备注**: 记录投资心得和决策理由

### 分析报告
- 📈 **收益统计**: 已实现/未实现收益分析
- 📋 **持仓报告**: 按账户、成员、分类统计
- 📅 **时间维度**: 日、月、季、年多维度分析
- 🎨 **可视化**: 线图、柱状图、饼状图展示

## 技术栈

### 后端
- **Flask 3.0**: Web框架
- **SQLAlchemy**: ORM数据库操作
- **Flask-Babel**: 国际化支持
- **Pandas**: 数据分析
- **YFinance**: 股票价格获取

### 前端
- **Bootstrap 5**: UI框架
- **Chart.js**: 数据可视化
- **jQuery**: DOM操作
- **Font Awesome**: 图标库

### 数据处理
- **Tesseract/EasyOCR**: OCR文字识别
- **OpenCV**: 图像预处理
- **APScheduler**: 定时任务

## 快速开始

### 环境要求
- Python 3.8+
- SQLite (开发环境) 或 PostgreSQL (生产环境)
- Tesseract OCR 引擎

### 安装步骤

1. **克隆项目**
```bash
git clone https://github.com/kevinhou456/canadian-family-investment.git
cd canadian-family-investment
```

2. **创建虚拟环境**
```bash
python -m venv venv
source venv/bin/activate  # Linux/Mac
# 或
venv\Scripts\activate     # Windows
```

3. **安装依赖**
```bash
pip install -r requirements.txt
```

4. **启动应用（自动初始化）**
```bash
python run.py
```

系统将自动检测数据库状态：
- 🔍 **全新安装**: 自动创建数据库表、初始化基础数据和演示数据
- ✅ **已有数据库**: 直接启动，跳过初始化步骤
- 🔧 **缺少基础数据**: 自动补充必要的账户类型和分类数据

首次运行访问 http://localhost:5050

## 系统架构

```
canadian_family_investment/
├── app/
│   ├── __init__.py           # 应用初始化
│   ├── models/               # 数据模型
│   ├── api/                  # API蓝图
│   ├── services/             # 业务逻辑
│   ├── utils/                # 工具类
│   └── templates/            # 前端模板
├── migrations/               # 数据库迁移
├── static/                   # 静态文件
├── tests/                    # 测试文件
├── config.py                 # 配置文件
└── run.py                   # 启动入口
```

## 支持的券商格式

### CSV导入支持
- ✅ Questrade
- ✅ TD Direct Investing
- ✅ Interactive Brokers
- ✅ Wealthsimple Trade

### OCR识别支持
- 📷 交易确认页面截图
- 📷 账户报表截图
- 📷 持仓明细截图

## 开发指南

### 添加新语言
```bash
# 提取翻译文本
pybabel extract -F babel.cfg -k _l -o messages.pot .

# 初始化新语言 (如法语)
pybabel init -i messages.pot -d app/translations -l fr

# 更新翻译
pybabel update -i messages.pot -d app/translations

# 编译翻译
pybabel compile -d app/translations
```

### 添加新券商格式
在 `app/services/csv_service.py` 中添加新的券商配置:

```python
BROKER_FORMATS = {
    'your_broker': {
        'name': 'Your Broker Name',
        'date_format': '%Y-%m-%d',
        'columns': {
            'transaction_date': 'Date Column',
            'symbol': 'Symbol Column',
            # ... 其他字段映射
        }
    }
}
```

## 贡献指南

1. Fork 项目
2. 创建功能分支 (`git checkout -b feature/AmazingFeature`)
3. 提交更改 (`git commit -m 'Add some AmazingFeature'`)
4. 推送到分支 (`git push origin feature/AmazingFeature`)
5. 打开 Pull Request

## 许可证

本项目采用 MIT 许可证 - 查看 [LICENSE](LICENSE) 文件了解详情

## 联系我们

项目链接: https://github.com/yourusername/canadian-family-investment

## 致谢

- [Yahoo Finance](https://finance.yahoo.com/) - 股票数据API
- [Flask](https://flask.palletsprojects.com/) - Web框架
- [Bootstrap](https://getbootstrap.com/) - UI框架
- [Chart.js](https://www.chartjs.org/) - 图表库