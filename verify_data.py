#!/usr/bin/env python3
"""
验证初始化的数据结构
"""

import os
import sys
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from app import create_app, db
from app.models.family import Family
from app.models.member import Member
from app.models.account import Account, AccountType, AccountMember
from app.models.stock import Stock
from app.models.transaction import Transaction

def verify_data():
    app = create_app()

    with app.app_context():
        print('=== 验证数据库结构 ===')
        
        # 验证家庭
        families = Family.query.all()
        print(f'家庭数量: {len(families)}')
        for family in families:
            print(f'  - {family.name}')
        
        # 验证成员
        members = Member.query.all()
        print(f'成员数量: {len(members)}')
        for member in members:
            print(f'  - {member.name} ({member.email})')
        
        # 验证账户类型
        account_types = AccountType.query.all()
        print(f'账户类型数量: {len(account_types)}')
        for at in account_types:
            print(f'  - {at.name}: {at.description}')
        
        # 验证账户
        accounts = Account.query.all()
        print(f'账户数量: {len(accounts)}')
        for acc in accounts:
            print(f'  - {acc.name} ({acc.account_type.name if acc.account_type else "N/A"}) - {acc.currency}')
            # 检查账户成员
            for am in acc.account_members:
                print(f'    -> {am.member.name}: {am.ownership_percentage}% (Primary: {am.is_primary})')
        
        # 验证股票
        stocks = Stock.query.all()
        print(f'股票数量: {len(stocks)}')
        
        # 验证交易
        transactions = Transaction.query.all()
        print(f'交易数量: {len(transactions)}')
        
        print('\n=== 验证用户要求的结构 ===')
        print('要求: user1,user2两个用户，每个用户再创建一个普通账户，一个TFSA账户，一个RRSP账户，然后再创建一个联合账户，两个用户各占比50%')
        
        user1 = Member.query.filter_by(name='张三').first()
        user2 = Member.query.filter_by(name='李四').first()
        
        if user1 and user2:
            print(f'✅ User1: {user1.name}')
            user1_accounts = []
            for am in AccountMember.query.filter_by(member_id=user1.id).all():
                if not am.account.is_joint:
                    user1_accounts.append(am.account)
            
            print(f'  个人账户数量: {len(user1_accounts)}')
            for acc in user1_accounts:
                print(f'    - {acc.name} ({acc.account_type.name})')
            
            print(f'✅ User2: {user2.name}')
            user2_accounts = []
            for am in AccountMember.query.filter_by(member_id=user2.id).all():
                if not am.account.is_joint:
                    user2_accounts.append(am.account)
            
            print(f'  个人账户数量: {len(user2_accounts)}')
            for acc in user2_accounts:
                print(f'    - {acc.name} ({acc.account_type.name})')
            
            # 检查联合账户
            joint_accounts = Account.query.filter_by(is_joint=True).all()
            print(f'✅ 联合账户数量: {len(joint_accounts)}')
            for acc in joint_accounts:
                print(f'  - {acc.name}')
                for am in acc.account_members:
                    print(f'    -> {am.member.name}: {am.ownership_percentage}%')

if __name__ == '__main__':
    verify_data()