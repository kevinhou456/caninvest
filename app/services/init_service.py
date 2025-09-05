"""
系统初始化服务
"""

from app import db
from app.models.account import AccountType

class InitializationService:
    """系统初始化服务类"""
    
    def initialize_default_data(self):
        """初始化默认数据"""
        print("正在初始化账户类型...")
        self._create_default_account_types()
        
        db.session.commit()
        print("默认数据初始化完成！")
    
    def _create_default_account_types(self):
        """创建默认账户类型"""
        account_types_data = [
            {
                'name': 'TFSA',
                'tax_advantaged': True,
                'annual_contribution_limit': 6500.00,  # 2024年度限额
                'description': 'Tax-Free Savings Account'
            },
            {
                'name': 'RRSP',
                'tax_advantaged': True,
                'annual_contribution_limit': 30780.00,  # 2024年度限额
                'description': 'Registered Retirement Savings Plan'
            },
            {
                'name': 'RESP',
                'tax_advantaged': True,
                'annual_contribution_limit': 2500.00,  # 建议供款额
                'description': 'Registered Education Savings Plan'
            },
            {
                'name': 'FHSA',
                'tax_advantaged': True,
                'annual_contribution_limit': 8000.00,  # 2024年度限额
                'description': 'First Home Savings Account'
            },
            {
                'name': 'Regular',
                'tax_advantaged': False,
                'annual_contribution_limit': None,
                'description': 'Regular Investment Account'
            },
            {
                'name': 'Margin',
                'tax_advantaged': False,
                'annual_contribution_limit': None,
                'description': 'Margin Trading Account'
            }
        ]
        
        for data in account_types_data:
            existing = AccountType.query.filter_by(name=data['name']).first()
            if not existing:
                account_type = AccountType(**data)
                db.session.add(account_type)
                print(f"  创建账户类型: {data['name']}")
            else:
                print(f"  账户类型已存在: {data['name']}")
    
    
    def create_demo_family(self):
        """创建演示家庭数据"""
        from app.models.family import Family
        from app.models.member import Member
        from app.models.account import Account, AccountMember
        from datetime import datetime, date
        
        print("正在创建演示家庭数据...")
        
        # 创建家庭
        demo_family = Family.query.filter_by(name='Demo Family').first()
        if demo_family:
            print("  演示家庭已存在")
            return demo_family
        
        demo_family = Family(name='Demo Family')
        db.session.add(demo_family)
        db.session.flush()
        
        # 创建成员
        john = Member(
            family_id=demo_family.id,
            name='John Smith'
        )
        
        jane = Member(
            family_id=demo_family.id,
            name='Jane Smith'
        )
        
        db.session.add_all([john, jane])
        db.session.flush()
        
        # 创建账户
        tfsa_john = Account(
            name="John's TFSA",
            family_id=demo_family.id,
            account_type_id=AccountType.query.filter_by(name='TFSA').first().id,
            currency='CAD',
            broker_name='Questrade'
        )
        
        rrsp_joint = Account(
            name="Joint RRSP",
            family_id=demo_family.id,
            account_type_id=AccountType.query.filter_by(name='RRSP').first().id,
            is_joint=True,
            currency='CAD',
            broker_name='TD Direct Investing'
        )
        
        regular_account = Account(
            name="Regular Investment",
            family_id=demo_family.id,
            account_type_id=AccountType.query.filter_by(name='Regular').first().id,
            currency='USD',
            broker_name='Interactive Brokers'
        )
        
        db.session.add_all([tfsa_john, rrsp_joint, regular_account])
        db.session.flush()
        
        # 添加账户成员关系
        tfsa_john_member = AccountMember(
            account_id=tfsa_john.id,
            member_id=john.id,
            ownership_percentage=100.0,
            is_primary=True
        )
        
        rrsp_john_member = AccountMember(
            account_id=rrsp_joint.id,
            member_id=john.id,
            ownership_percentage=60.0,
            is_primary=True
        )
        
        rrsp_jane_member = AccountMember(
            account_id=rrsp_joint.id,
            member_id=jane.id,
            ownership_percentage=40.0,
            is_primary=False
        )
        
        regular_jane_member = AccountMember(
            account_id=regular_account.id,
            member_id=jane.id,
            ownership_percentage=100.0,
            is_primary=True
        )
        
        db.session.add_all([tfsa_john_member, rrsp_john_member, rrsp_jane_member, regular_jane_member])
        
        # 不创建演示股票和交易记录，保持系统初始化时的干净状态
        # 股票将在需要时通过stocks_cache表自动创建
        print("  跳过演示股票和交易创建，保持记录为空")
        
        db.session.commit()
        print("  演示家庭数据创建完成！")
        return demo_family