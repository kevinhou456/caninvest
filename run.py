#!/usr/bin/env python3
"""
Canadian Family Investment Management System
应用启动入口
"""

import os
from app import create_app, db
from app.models.family import Family
from app.models.member import Member
from app.models.account import Account, AccountType
from app.models.transaction import Transaction
from app.models.stock import Stock, StockCategory

# 创建应用实例
app = create_app(os.getenv('FLASK_ENV'))

@app.cli.command()
def init_db():
    """初始化数据库"""
    db.create_all()
    print('数据库初始化完成!')

@app.cli.command()
def init_data():
    """初始化基础数据"""
    from app.services.init_service import InitializationService
    
    service = InitializationService()
    service.initialize_default_data()
    print('基础数据初始化完成!')

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=True)