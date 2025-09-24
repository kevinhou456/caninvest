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

        print("正在初始化股票分类...")
        self._create_default_stock_categories()

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

    def _create_default_stock_categories(self):
        """创建默认股票分类"""
        from app.models.stock_category import StockCategory

        default_categories = [
            {'name': '科技股', 'name_en': 'Technology', 'color': '#007bff', 'description': '科技类股票'},
            {'name': '金融股', 'name_en': 'Financial', 'color': '#28a745', 'description': '金融类股票'},
            {'name': '消费股', 'name_en': 'Consumer', 'color': '#dc3545', 'description': '消费类股票'},
            {'name': '医疗股', 'name_en': 'Healthcare', 'color': '#6f42c1', 'description': '医疗保健类股票'},
            {'name': '能源股', 'name_en': 'Energy', 'color': '#fd7e14', 'description': '能源类股票'},
            {'name': '房地产', 'name_en': 'Real Estate', 'color': '#20c997', 'description': '房地产相关股票'},
            {'name': 'ETF', 'name_en': 'ETF', 'color': '#6c757d', 'description': '交易型开放式指数基金'},
        ]

        for cat_data in default_categories:
            existing = StockCategory.query.filter_by(name=cat_data['name']).first()
            if not existing:
                category = StockCategory(**cat_data)
                db.session.add(category)
                print(f"  创建股票分类: {cat_data['name']}")
            else:
                print(f"  股票分类已存在: {cat_data['name']}")

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

    def create_sample_transactions(self):
        """为账户1创建示例交易记录"""
        from app.models.transaction import Transaction
        from app.models.account import Account
        from datetime import datetime, date
        import decimal

        print("正在创建示例交易记录...")

        # 找到第一个账户（Member1 Non-registered或者第一个Non-registered账户）
        account = Account.query.filter_by(name="Member1 Non-registered").first()
        if not account:
            # 如果找不到指定名称的账户，使用第一个账户
            account = Account.query.first()
            if not account:
                print("  未找到任何账户，跳过创建交易记录")
                return
            print(f"  使用账户: {account.name} (ID: {account.id})")

        # 检查是否已经有交易记录
        existing_transactions = Transaction.query.filter_by(account_id=account.id).first()
        if existing_transactions:
            print("  账户已有交易记录，跳过创建")
            return

        account_id = account.id

        # 示例交易记录：
        # 1. AAPL (苹果) - 未清仓
        # 2. NVDA (英伟达) - 未清仓
        # 3. GOOGL (谷歌) - 已清仓
        # 4. MSFT (微软) - 已清仓

        sample_transactions = [
            # === AAPL (苹果) - 未清仓 ===
            # 买入100股 @ $150
            Transaction(
                account_id=account_id,
                trade_date=date(2024, 1, 15),
                stock='AAPL',
                type='BUY',
                quantity=decimal.Decimal('100'),
                price=decimal.Decimal('150.50'),
                fee=decimal.Decimal('9.95'),
                currency='USD',
                notes='苹果股票首次买入'
            ),
            # 买入50股 @ $160
            Transaction(
                account_id=account_id,
                trade_date=date(2024, 3, 20),
                stock='AAPL',
                type='BUY',
                quantity=decimal.Decimal('50'),
                price=decimal.Decimal('160.25'),
                fee=decimal.Decimal('9.95'),
                currency='USD',
                notes='苹果股票补仓'
            ),
            # 收到分红
            Transaction(
                account_id=account_id,
                trade_date=date(2024, 5, 10),
                stock='AAPL',
                type='DIVIDEND',
                quantity=decimal.Decimal('150'),  # 150股
                price=decimal.Decimal('0.25'),  # 每股分红金额
                amount=decimal.Decimal('37.50'),  # 总分红金额
                currency='USD',
                notes='苹果季度分红'
            ),
            # 收到分红
            Transaction(
                account_id=account_id,
                trade_date=date(2024, 8, 10),
                stock='AAPL',
                type='DIVIDEND',
                quantity=decimal.Decimal('150'),  # 150股
                price=decimal.Decimal('0.25'),  # 每股分红金额
                amount=decimal.Decimal('37.50'),  # 总分红金额
                currency='USD',
                notes='苹果季度分红'
            ),

            # === NVDA (英伟达) - 未清仓 ===
            # 买入30股 @ $400
            Transaction(
                account_id=account_id,
                trade_date=date(2024, 2, 10),
                stock='NVDA',
                type='BUY',
                quantity=decimal.Decimal('30'),
                price=decimal.Decimal('400.00'),
                fee=decimal.Decimal('9.95'),
                currency='USD',
                notes='英伟达AI概念股投资'
            ),
            # 买入20股 @ $500
            Transaction(
                account_id=account_id,
                trade_date=date(2024, 6, 15),
                stock='NVDA',
                type='BUY',
                quantity=decimal.Decimal('20'),
                price=decimal.Decimal('500.00'),
                fee=decimal.Decimal('9.95'),
                currency='USD',
                notes='英伟达追加投资'
            ),

            # === GOOGL (谷歌) - 已清仓 ===
            # 买入80股 @ $120
            Transaction(
                account_id=account_id,
                trade_date=date(2023, 11, 5),
                stock='GOOGL',
                type='BUY',
                quantity=decimal.Decimal('80'),
                price=decimal.Decimal('120.00'),
                fee=decimal.Decimal('9.95'),
                currency='USD',
                notes='谷歌股票投资'
            ),
            # 买入40股 @ $110
            Transaction(
                account_id=account_id,
                trade_date=date(2024, 1, 25),
                stock='GOOGL',
                type='BUY',
                quantity=decimal.Decimal('40'),
                price=decimal.Decimal('110.00'),
                fee=decimal.Decimal('9.95'),
                currency='USD',
                notes='谷歌股票补仓'
            ),
            # 全部卖出120股 @ $135
            Transaction(
                account_id=account_id,
                trade_date=date(2024, 7, 30),
                stock='GOOGL',
                type='SELL',
                quantity=decimal.Decimal('120'),
                price=decimal.Decimal('135.00'),
                fee=decimal.Decimal('9.95'),
                currency='USD',
                notes='谷歌股票全部清仓获利'
            ),

            # === MSFT (微软) - 已清仓 ===
            # 买入60股 @ $300
            Transaction(
                account_id=account_id,
                trade_date=date(2023, 12, 10),
                stock='MSFT',
                type='BUY',
                quantity=decimal.Decimal('60'),
                price=decimal.Decimal('300.00'),
                fee=decimal.Decimal('9.95'),
                currency='USD',
                notes='微软股票投资'
            ),
            # 收到分红
            Transaction(
                account_id=account_id,
                trade_date=date(2024, 3, 15),
                stock='MSFT',
                type='DIVIDEND',
                quantity=decimal.Decimal('60'),
                price=decimal.Decimal('0.75'),  # 每股分红金额
                amount=decimal.Decimal('45.00'),  # 总分红金额
                currency='USD',
                notes='微软季度分红'
            ),
            # 收到分红
            Transaction(
                account_id=account_id,
                trade_date=date(2024, 6, 15),
                stock='MSFT',
                type='DIVIDEND',
                quantity=decimal.Decimal('60'),
                price=decimal.Decimal('0.75'),  # 每股分红金额
                amount=decimal.Decimal('45.00'),  # 总分红金额
                currency='USD',
                notes='微软季度分红'
            ),
            # 全部卖出60股 @ $290
            Transaction(
                account_id=account_id,
                trade_date=date(2024, 8, 25),
                stock='MSFT',
                type='SELL',
                quantity=decimal.Decimal('60'),
                price=decimal.Decimal('290.00'),
                fee=decimal.Decimal('9.95'),
                currency='USD',
                notes='微软股票全部清仓止损'
            ),
        ]

        # 添加所有交易记录
        for transaction in sample_transactions:
            db.session.add(transaction)

        db.session.commit()
        print("  示例交易记录创建完成！")
        print(f"  - AAPL (苹果): 150股未清仓，含分红记录")
        print(f"  - NVDA (英伟达): 50股未清仓")
        print(f"  - GOOGL (谷歌): 已清仓获利")
        print(f"  - MSFT (微软): 已清仓止损，含分红记录")