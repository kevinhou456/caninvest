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
        
        print("正在创建默认家庭数据...")
        
        # 创建家庭
        demo_family = Family.query.filter_by(name='Default Family').first()
        if demo_family:
            print("  默认家庭已存在")
            return demo_family
        
        demo_family = Family(name='Default Family')
        db.session.add(demo_family)
        db.session.flush()
        
        # 创建成员
        member1 = Member(
            family_id=demo_family.id,
            name='Member1'
        )
        
        member2 = Member(
            family_id=demo_family.id,
            name='Member2'
        )
        
        db.session.add_all([member1, member2])
        db.session.flush()
        
        # 获取账户类型
        regular_type = AccountType.query.filter_by(name='Regular').first()
        tfsa_type = AccountType.query.filter_by(name='TFSA').first()
        rrsp_type = AccountType.query.filter_by(name='RRSP').first()
        
        # 创建Member1的账户
        member1_regular = Account(
            name="Member1 Non-registered",
            family_id=demo_family.id,
            account_type_id=regular_type.id,
            broker_name='Questrade'
        )

        member1_tfsa = Account(
            name="Member1 TFSA",
            family_id=demo_family.id,
            account_type_id=tfsa_type.id,
            broker_name='Questrade'
        )

        member1_rrsp = Account(
            name="Member1 RRSP",
            family_id=demo_family.id,
            account_type_id=rrsp_type.id,
            broker_name='Questrade'
        )

        # 创建Member2的账户
        member2_regular = Account(
            name="Member2 Non-registered",
            family_id=demo_family.id,
            account_type_id=regular_type.id,
            broker_name='TD Direct Investing'
        )

        member2_tfsa = Account(
            name="Member2 TFSA",
            family_id=demo_family.id,
            account_type_id=tfsa_type.id,
            broker_name='TD Direct Investing'
        )

        member2_rrsp = Account(
            name="Member2 RRSP",
            family_id=demo_family.id,
            account_type_id=rrsp_type.id,
            broker_name='TD Direct Investing'
        )

        # 创建Joint账户
        joint_account = Account(
            name="Joint Account",
            family_id=demo_family.id,
            account_type_id=regular_type.id,
            is_joint=True,
            broker_name='RBC Direct Investing'
        )
        
        db.session.add_all([
            member1_regular, member1_tfsa, member1_rrsp,
            member2_regular, member2_tfsa, member2_rrsp,
            joint_account
        ])
        db.session.flush()
        
        # 创建账户成员关系
        account_members = [
            # Member1的个人账户 (100%)
            AccountMember(account_id=member1_regular.id, member_id=member1.id, ownership_percentage=100.0, is_primary=True),
            AccountMember(account_id=member1_tfsa.id, member_id=member1.id, ownership_percentage=100.0, is_primary=True),
            AccountMember(account_id=member1_rrsp.id, member_id=member1.id, ownership_percentage=100.0, is_primary=True),
            
            # Member2的个人账户 (100%)
            AccountMember(account_id=member2_regular.id, member_id=member2.id, ownership_percentage=100.0, is_primary=True),
            AccountMember(account_id=member2_tfsa.id, member_id=member2.id, ownership_percentage=100.0, is_primary=True),
            AccountMember(account_id=member2_rrsp.id, member_id=member2.id, ownership_percentage=100.0, is_primary=True),
            
            # Joint账户 (各50%)
            AccountMember(account_id=joint_account.id, member_id=member1.id, ownership_percentage=50.0, is_primary=True),
            AccountMember(account_id=joint_account.id, member_id=member2.id, ownership_percentage=50.0, is_primary=False)
        ]
        
        db.session.add_all(account_members)
        
        db.session.commit()
        print("  默认家庭数据创建完成！")
        print(f"  - 创建成员: Member1, Member2")
        print(f"  - Member1账户: Non-registered, TFSA, RRSP")
        print(f"  - Member2账户: Non-registered, TFSA, RRSP")
        print(f"  - Joint账户: Member1(50%), Member2(50%)")
        return demo_family