"""
Flask应用入口点
"""

import os
from app import create_app, db
from app.models.family import Family
from app.models.member import Member
from app.models.account import Account, AccountType, AccountMember
from app.models.transaction import Transaction
# from app.models.holding import CurrentHolding  # CurrentHolding model deleted
# from app.models.stock import Stock, StockCategory, StockCategoryI18n  # Stock models deleted
from app.models.contribution import Contribution
from app.models.price_cache import StockPriceCache, PriceUpdateLog
from app.models.import_task import ImportTask, OCRTask, OCRTransactionPending

# 创建应用实例
app = create_app()

@app.shell_context_processor
def make_shell_context():
    """Shell上下文处理器 - 为Flask shell提供便捷的对象访问"""
    return {
        'db': db,
        'Family': Family,
        'Member': Member,
        'Account': Account,
        'AccountType': AccountType,
        'AccountMember': AccountMember,
        'Transaction': Transaction,
        # 'CurrentHolding': CurrentHolding,  # CurrentHolding model deleted
        # 'Stock': Stock,  # Stock models deleted
        # 'StockCategory': StockCategory,  # Stock models deleted
        # 'StockCategoryI18n': StockCategoryI18n,  # Stock models deleted
        'Contribution': Contribution,
        'StockPriceCache': StockPriceCache,
        'PriceUpdateLog': PriceUpdateLog,
        'ImportTask': ImportTask,
        'OCRTask': OCRTask,
        'OCRTransactionPending': OCRTransactionPending
    }

@app.cli.command()
def init_db():
    """初始化数据库"""
    db.create_all()
    print("数据库表已创建！")

@app.cli.command()
def init_data():
    """初始化默认数据"""
    from app.services.init_service import InitializationService
    
    init_service = InitializationService()
    init_service.initialize_default_data()
    print("默认数据初始化完成！")

@app.cli.command()
def create_demo():
    """创建演示数据"""
    from app.services.init_service import InitializationService
    
    # 先初始化默认数据
    init_service = InitializationService()
    init_service.initialize_default_data()
    
    # 创建演示家庭
    demo_family = init_service.create_demo_family()
    print(f"演示家庭 '{demo_family.name}' 创建完成！")

if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=5050)