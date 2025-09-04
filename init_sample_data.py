#!/usr/bin/env python3
"""
初始化示例数据
创建两个用户，每人三个账户（普通、TFSA、RRSP），加上一个联合账户
"""

import os
from datetime import datetime, date
from decimal import Decimal

# 添加项目根目录到路径
import sys
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from app import create_app, db
from app.models.family import Family
from app.models.member import Member
from app.models.account import Account, AccountType, AccountMember
from app.models.stock import Stock, StockCategory
from app.models.transaction import Transaction

def init_sample_data():
    """初始化示例数据"""
    app = create_app()
    
    with app.app_context():
        # 删除所有现有数据
        db.drop_all()
        db.create_all()
        
        print("🗑️  已清空数据库")
        
        # 1. 创建账户类型
        account_types = [
            AccountType(name='Taxable', description='Regular taxable investment account', tax_advantaged=False),
            AccountType(name='TFSA', description='Tax-Free Savings Account', tax_advantaged=True),
            AccountType(name='RRSP', description='Registered Retirement Savings Plan', tax_advantaged=True),
            AccountType(name='RESP', description='Registered Education Savings Plan', tax_advantaged=True),
            AccountType(name='FHSA', description='First Home Savings Account', tax_advantaged=True),
        ]
        
        for at in account_types:
            db.session.add(at)
        db.session.commit()
        
        print("✅ 已创建账户类型")
        
        # 2. 创建股票分类
        categories = [
            StockCategory(name='Technology', description='Technology companies'),
            StockCategory(name='Finance', description='Financial institutions'),
            StockCategory(name='Energy', description='Energy companies'),
            StockCategory(name='Healthcare', description='Healthcare companies'),
            StockCategory(name='Real Estate', description='Real Estate Investment Trusts'),
        ]
        
        for cat in categories:
            db.session.add(cat)
        db.session.commit()
        
        # 3. 创建示例股票
        stocks = [
            Stock(symbol='AAPL', name='Apple Inc.', exchange='NASDAQ', currency='USD', category_id=1),
            Stock(symbol='MSFT', name='Microsoft Corp.', exchange='NASDAQ', currency='USD', category_id=1),
            Stock(symbol='GOOGL', name='Alphabet Inc.', exchange='NASDAQ', currency='USD', category_id=1),
            Stock(symbol='TD.TO', name='Toronto-Dominion Bank', exchange='TSE', currency='CAD', category_id=2),
            Stock(symbol='RY.TO', name='Royal Bank of Canada', exchange='TSE', currency='CAD', category_id=2),
            Stock(symbol='SHOP.TO', name='Shopify Inc.', exchange='TSE', currency='CAD', category_id=1),
            Stock(symbol='CNQ.TO', name='Canadian Natural Resources', exchange='TSE', currency='CAD', category_id=3),
            Stock(symbol='REI.TO', name='RioCan REIT', exchange='TSE', currency='CAD', category_id=5),
        ]
        
        for stock in stocks:
            db.session.add(stock)
        db.session.commit()
        
        print("✅ 已创建股票数据")
        
        # 4. 创建家庭
        family = Family(name="Sample Family")
        db.session.add(family)
        db.session.commit()
        
        print("✅ 已创建家庭")
        
        # 5. 创建用户
        user1 = Member(
            family_id=family.id,
            name="Member1",
            email="member1@example.com",
            date_of_birth=date(1985, 6, 15),
            sin_number="123-456-789"
        )
        
        user2 = Member(
            family_id=family.id,
            name="Member2",
            email="member2@example.com", 
            date_of_birth=date(1987, 8, 20),
            sin_number="987-654-321"
        )
        
        db.session.add(user1)
        db.session.add(user2)
        db.session.commit()
        
        print("✅ 已创建用户: Member1, Member2")
        
        # 6. 创建账户
        # 获取账户类型
        taxable_type = AccountType.query.filter_by(name='Taxable').first()
        tfsa_type = AccountType.query.filter_by(name='TFSA').first()  
        rrsp_type = AccountType.query.filter_by(name='RRSP').first()
        
        # Member1的账户
        user1_accounts = [
            Account(name="Non-Registered", family_id=family.id, account_type_id=taxable_type.id, currency='CAD', broker_name='TD Direct'),
            Account(name="TFSA", family_id=family.id, account_type_id=tfsa_type.id, currency='CAD', broker_name='TD Direct'),
            Account(name="RRSP", family_id=family.id, account_type_id=rrsp_type.id, currency='CAD', broker_name='TD Direct'),
        ]
        
        # Member2的账户
        user2_accounts = [
            Account(name="Non-Registered", family_id=family.id, account_type_id=taxable_type.id, currency='CAD', broker_name='RBC Direct'),
            Account(name="TFSA", family_id=family.id, account_type_id=tfsa_type.id, currency='CAD', broker_name='RBC Direct'),
            Account(name="RRSP", family_id=family.id, account_type_id=rrsp_type.id, currency='CAD', broker_name='RBC Direct'),
        ]
        
        # 联合账户
        joint_account = Account(name="Joint", family_id=family.id, account_type_id=taxable_type.id, currency='CAD', broker_name='Scotia iTRADE', is_joint=True)
        
        # 保存所有账户
        for acc in user1_accounts + user2_accounts + [joint_account]:
            db.session.add(acc)
        db.session.commit()
        
        print("✅ 已创建账户")
        
        # 7. 创建账户成员关系
        # 张三的个人账户
        for acc in user1_accounts:
            account_member = AccountMember(
                account_id=acc.id,
                member_id=user1.id,
                ownership_percentage=100.0,
                is_primary=True
            )
            db.session.add(account_member)
        
        # 李四的个人账户
        for acc in user2_accounts:
            account_member = AccountMember(
                account_id=acc.id,
                member_id=user2.id,
                ownership_percentage=100.0,
                is_primary=True
            )
            db.session.add(account_member)
        
        # 联合账户 - 两人各占50%
        joint_member1 = AccountMember(
            account_id=joint_account.id,
            member_id=user1.id,
            ownership_percentage=50.0,
            is_primary=True
        )
        
        joint_member2 = AccountMember(
            account_id=joint_account.id,
            member_id=user2.id,
            ownership_percentage=50.0,
            is_primary=False
        )
        
        db.session.add(joint_member1)
        db.session.add(joint_member2)
        db.session.commit()
        
        print("✅ 已创建账户成员关系")
        
        # 8. 创建示例交易
        all_accounts = user1_accounts + user2_accounts + [joint_account]
        
        # 获取股票
        aapl = Stock.query.filter_by(symbol='AAPL').first()
        msft = Stock.query.filter_by(symbol='MSFT').first()
        googl = Stock.query.filter_by(symbol='GOOGL').first()
        td = Stock.query.filter_by(symbol='TD.TO').first()
        ry = Stock.query.filter_by(symbol='RY.TO').first()
        shop = Stock.query.filter_by(symbol='SHOP.TO').first()
        
        # 示例交易 - 包含多币种
        sample_transactions = [
            # Member1 Non-Registered Account - 混合USD和CAD交易
            Transaction(account_id=user1_accounts[0].id, stock_id=aapl.id, member_id=user1.id,
                       transaction_type='buy', quantity=100, price_per_share=Decimal('150.00'),
                       transaction_date=date(2024, 1, 15), notes='Apple purchase (USD)'),
            
            Transaction(account_id=user1_accounts[0].id, stock_id=td.id, member_id=user1.id,
                       transaction_type='buy', quantity=50, price_per_share=Decimal('82.50'),
                       transaction_date=date(2024, 2, 20), notes='TD Bank investment (CAD)'),
            
            Transaction(account_id=user1_accounts[0].id, stock_id=msft.id, member_id=user1.id,
                       transaction_type='buy', quantity=30, price_per_share=Decimal('380.00'),
                       transaction_date=date(2024, 3, 5), notes='Microsoft purchase (USD)'),
            
            # Member1 TFSA Account - CAD股票
            Transaction(account_id=user1_accounts[1].id, stock_id=shop.id, member_id=user1.id,
                       transaction_type='buy', quantity=25, price_per_share=Decimal('75.00'),
                       transaction_date=date(2024, 3, 10), notes='Shopify in TFSA (CAD)'),
            
            Transaction(account_id=user1_accounts[1].id, stock_id=ry.id, member_id=user1.id,
                       transaction_type='buy', quantity=40, price_per_share=Decimal('128.50'),
                       transaction_date=date(2024, 4, 15), notes='Royal Bank in TFSA (CAD)'),
            
            # Member1 RRSP Account - 混合
            Transaction(account_id=user1_accounts[2].id, stock_id=googl.id, member_id=user1.id,
                       transaction_type='buy', quantity=15, price_per_share=Decimal('145.00'),
                       transaction_date=date(2024, 5, 1), notes='Google in RRSP (USD)'),
            
            # Member2 Non-Registered Account - 混合USD和CAD交易
            Transaction(account_id=user2_accounts[0].id, stock_id=aapl.id, member_id=user2.id,
                       transaction_type='buy', quantity=75, price_per_share=Decimal('155.00'),
                       transaction_date=date(2024, 1, 25), notes='Apple purchase (USD)'),
            
            Transaction(account_id=user2_accounts[0].id, stock_id=shop.id, member_id=user2.id,
                       transaction_type='buy', quantity=60, price_per_share=Decimal('72.00'),
                       transaction_date=date(2024, 2, 10), notes='Shopify purchase (CAD)'),
            
            # Member2 TFSA Account - CAD为主
            Transaction(account_id=user2_accounts[1].id, stock_id=td.id, member_id=user2.id,
                       transaction_type='buy', quantity=35, price_per_share=Decimal('83.25'),
                       transaction_date=date(2024, 3, 20), notes='TD in TFSA (CAD)'),
            
            # Member2 RRSP Account - 混合
            Transaction(account_id=user2_accounts[2].id, stock_id=msft.id, member_id=user2.id,
                       transaction_type='buy', quantity=20, price_per_share=Decimal('385.00'),
                       transaction_date=date(2024, 4, 5), notes='Microsoft in RRSP (USD)'),
            
            Transaction(account_id=user2_accounts[2].id, stock_id=ry.id, member_id=user2.id,
                       transaction_type='buy', quantity=30, price_per_share=Decimal('130.00'),
                       transaction_date=date(2024, 4, 25), notes='Royal Bank in RRSP (CAD)'),
            
            # Joint Account - 混合投资
            Transaction(account_id=joint_account.id, stock_id=aapl.id, member_id=user1.id,
                       transaction_type='buy', quantity=50, price_per_share=Decimal('148.00'),
                       transaction_date=date(2024, 5, 15), notes='Joint Apple investment (USD)'),
            
            Transaction(account_id=joint_account.id, stock_id=shop.id, member_id=user2.id,
                       transaction_type='buy', quantity=80, price_per_share=Decimal('70.00'),
                       transaction_date=date(2024, 6, 1), notes='Joint Shopify investment (CAD)'),
        ]
        
        for transaction in sample_transactions:
            db.session.add(transaction)
        db.session.commit()
        
        print("✅ 已创建示例交易")
        
        # 9. 更新账户价值 (模拟当前价值)
        # 这里简化处理，假设股票都有一定涨幅
        price_updates = {
            'AAPL': Decimal('175.00'),  # 涨了
            'TD.TO': Decimal('88.00'),  # 涨了
            'SHOP.TO': Decimal('78.00'), # 涨了
        }
        
        print("📊 数据库初始化完成!")
        print(f"")
        print(f"🏠 家庭: {family.name}")
        print(f"👥 成员: Member1, Member2")
        print(f"💳 账户总数: {len(all_accounts)}")
        print(f"   - Member1: 3个个人账户 (Taxable、TFSA、RRSP)")
        print(f"   - Member2: 3个个人账户 (Taxable、TFSA、RRSP)")
        print(f"   - Joint: 1个联合账户 (各占50%)")
        print(f"📈 股票: 8只股票 (USD/CAD)")
        print(f"💼 交易: {len(sample_transactions)}笔交易 (多币种)")
        print(f"💱 支持货币: CAD, USD")
        print(f"")
        print(f"🌐 访问地址: http://localhost:5050/dashboard")

if __name__ == '__main__':
    init_sample_data()