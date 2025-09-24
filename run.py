#!/usr/bin/env python3
"""
Canadian Family Investment Management System
应用启动入口
"""

import os
import sys
from sqlalchemy import inspect
from app import create_app, db
from app.models.family import Family
from app.models.member import Member
from app.models.account import Account, AccountType
from app.models.transaction import Transaction
# from app.models.stock import Stock, StockCategory  # Stock models deleted
from app.models.stocks_cache import StocksCache

def check_and_initialize_database(app):
    """检查并自动初始化数据库"""
    with app.app_context():
        try:
            # 检查数据库连接
            inspector = inspect(db.engine)
            existing_tables = inspector.get_table_names()
            
            # 如果没有任何表，说明是全新数据库
            if not existing_tables:
                print("🔍 检测到全新数据库，正在进行自动初始化...")
                
                # 创建所有表
                db.create_all()
                print("✅ 数据库表结构创建完成")
                
                # 初始化基础数据
                from app.services.init_service import InitializationService
                service = InitializationService()
                service.initialize_default_data()
                print("✅ 基础数据初始化完成")
                
                # 检查是否有家庭数据，如果没有则创建演示数据
                family_count = Family.query.count()
                if family_count == 0:
                    print("🏠 未检测到家庭数据，正在创建演示数据...")
                    
                    # 可以选择使用简单的演示数据或详细的示例数据
                    choice = os.getenv('DEMO_DATA_TYPE', 'simple')
                    if choice == 'full':
                        # 使用详细示例数据（init_sample_data.py的内容）
                        from init_sample_data import init_sample_data
                        init_sample_data()
                    else:
                        # 使用简单演示数据
                        service.create_demo_family()

                        # 创建示例交易记录
                        service.create_sample_transactions()

                    print("✅ 演示数据创建完成")
                
                print("🎉 数据库初始化完成！系统已准备就绪")
                print("🌐 访问地址: http://localhost:5050")
                
            else:
                print("✅ 数据库已存在，跳过初始化")
                
                # 检查是否需要更新基础数据
                account_type_count = AccountType.query.count()
                if account_type_count == 0:
                    print("🔧 检测到缺少基础数据，正在补充...")
                    from app.services.init_service import InitializationService
                    service = InitializationService()
                    service.initialize_default_data()
                    print("✅ 基础数据补充完成")
                
        except Exception as e:
            print(f"❌ 数据库检查/初始化失败: {e}")
            print("请检查数据库配置或手动运行初始化命令")
            return False
    
    return True

# 创建应用实例
app = create_app(os.getenv('FLASK_ENV'))

@app.cli.command()
def init_db():
    """手动初始化数据库"""
    db.create_all()
    print('数据库初始化完成!')

@app.cli.command()
def init_data():
    """手动初始化基础数据"""
    from app.services.init_service import InitializationService
    
    service = InitializationService()
    service.initialize_default_data()
    print('基础数据初始化完成!')

@app.cli.command()
def reset_db():
    """重置数据库（危险操作）"""
    if input("确认要重置数据库吗？这将删除所有数据 (y/N): ").lower() == 'y':
        db.drop_all()
        print("数据库已清空")
        check_and_initialize_database(app)
    else:
        print("操作已取消")

if __name__ == '__main__':
    # 启动前自动检查并初始化数据库
    if check_and_initialize_database(app):
        print("🚀 启动应用服务器...")
        app.run(host='0.0.0.0', port=5050, debug=True)
    else:
        print("❌ 应用启动失败")
        sys.exit(1)