"""
系统初始化服务
"""

from app import db
from app.models.account import AccountType
from app.models.stock import StockCategory, StockCategoryI18n

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
        categories_data = [
            {
                'name': 'Technology',
                'color': '#4A90E2',
                'icon': 'fas fa-microchip',
                'description': 'Technology companies and software',
                'translations': {
                    'en': {'name': 'Technology', 'description': 'Technology companies and software'},
                    'zh_CN': {'name': '科技股', 'description': '科技公司和软件企业'}
                }
            },
            {
                'name': 'Banking',
                'color': '#7ED321',
                'icon': 'fas fa-university',
                'description': 'Banking and financial services',
                'translations': {
                    'en': {'name': 'Banking & Financial', 'description': 'Banking and financial services'},
                    'zh_CN': {'name': '银行金融', 'description': '银行和金融服务'}
                }
            },
            {
                'name': 'Healthcare',
                'color': '#BD10E0',
                'icon': 'fas fa-heartbeat',
                'description': 'Healthcare and pharmaceutical companies',
                'translations': {
                    'en': {'name': 'Healthcare', 'description': 'Healthcare and pharmaceutical companies'},
                    'zh_CN': {'name': '医疗保健', 'description': '医疗保健和制药公司'}
                }
            },
            {
                'name': 'Energy',
                'color': '#D0021B',
                'icon': 'fas fa-fire',
                'description': 'Energy sector including oil, gas and renewables',
                'translations': {
                    'en': {'name': 'Energy', 'description': 'Energy sector including oil, gas and renewables'},
                    'zh_CN': {'name': '能源', 'description': '能源行业包括石油、天然气和可再生能源'}
                }
            },
            {
                'name': 'Real Estate',
                'color': '#9013FE',
                'icon': 'fas fa-building',
                'description': 'Real Estate Investment Trusts (REITs)',
                'translations': {
                    'en': {'name': 'Real Estate (REITs)', 'description': 'Real Estate Investment Trusts'},
                    'zh_CN': {'name': '房地产投资信托', 'description': '房地产投资信托基金'}
                }
            },
            {
                'name': 'Consumer Goods',
                'color': '#F5A623',
                'icon': 'fas fa-shopping-cart',
                'description': 'Consumer goods and retail companies',
                'translations': {
                    'en': {'name': 'Consumer Goods', 'description': 'Consumer goods and retail companies'},
                    'zh_CN': {'name': '消费品', 'description': '消费品和零售公司'}
                }
            },
            {
                'name': 'Index Funds',
                'color': '#50E3C2',
                'icon': 'fas fa-layer-group',
                'description': 'Index funds and ETFs',
                'translations': {
                    'en': {'name': 'Index Funds & ETFs', 'description': 'Index funds and exchange-traded funds'},
                    'zh_CN': {'name': '指数基金', 'description': '指数基金和交易所交易基金'}
                }
            },
            {
                'name': 'Utilities',
                'color': '#B8E986',
                'icon': 'fas fa-bolt',
                'description': 'Utility companies',
                'translations': {
                    'en': {'name': 'Utilities', 'description': 'Utility companies providing essential services'},
                    'zh_CN': {'name': '公用事业', 'description': '提供基础服务的公用事业公司'}
                }
            },
            {
                'name': 'Materials',
                'color': '#8B572A',
                'icon': 'fas fa-industry',
                'description': 'Mining and materials companies',
                'translations': {
                    'en': {'name': 'Materials & Mining', 'description': 'Mining and materials companies'},
                    'zh_CN': {'name': '原材料', 'description': '矿业和原材料公司'}
                }
            },
            {
                'name': 'Telecommunications',
                'color': '#34495E',
                'icon': 'fas fa-signal',
                'description': 'Telecommunications companies',
                'translations': {
                    'en': {'name': 'Telecommunications', 'description': 'Telecommunications and media companies'},
                    'zh_CN': {'name': '电信', 'description': '电信和媒体公司'}
                }
            }
        ]
        
        for data in categories_data:
            existing = StockCategory.query.filter_by(name=data['name']).first()
            if not existing:
                # 创建分类
                translations = data.pop('translations')
                category = StockCategory(
                    is_system=True,
                    sort_order=len(categories_data),
                    **data
                )
                db.session.add(category)
                db.session.flush()  # 获取ID
                
                # 添加翻译
                for lang_code, translation in translations.items():
                    category_i18n = StockCategoryI18n(
                        category_id=category.id,
                        language_code=lang_code,
                        name=translation['name'],
                        description=translation['description']
                    )
                    db.session.add(category_i18n)
                
                print(f"  创建股票分类: {data['name']}")
            else:
                print(f"  股票分类已存在: {data['name']}")
    
    def create_demo_family(self):
        """创建演示家庭数据"""
        from app.models.family import Family
        from app.models.member import Member
        from app.models.account import Account, AccountMember
        from app.models.stock import Stock
        from app.models.transaction import Transaction
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
            name='John Smith',
            email='john@example.com',
            date_of_birth=date(1980, 5, 15),
            preferred_language='en'
        )
        
        jane = Member(
            family_id=demo_family.id,
            name='Jane Smith',  
            email='jane@example.com',
            date_of_birth=date(1985, 8, 22),
            preferred_language='en'
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
        
        # 创建一些演示股票
        tech_category = StockCategory.query.filter_by(name='Technology').first()
        banking_category = StockCategory.query.filter_by(name='Banking').first()
        etf_category = StockCategory.query.filter_by(name='Index Funds').first()
        
        stocks_data = [
            {
                'symbol': 'AAPL',
                'name': 'Apple Inc.',
                'exchange': 'NASDAQ',
                'currency': 'USD',
                'category_id': tech_category.id if tech_category else None
            },
            {
                'symbol': 'SHOP.TO',
                'name': 'Shopify Inc.',
                'exchange': 'TSX',
                'currency': 'CAD',
                'category_id': tech_category.id if tech_category else None
            },
            {
                'symbol': 'TD.TO',
                'name': 'Toronto-Dominion Bank',
                'exchange': 'TSX',
                'currency': 'CAD',
                'category_id': banking_category.id if banking_category else None
            },
            {
                'symbol': 'VTI',
                'name': 'Vanguard Total Stock Market ETF',
                'exchange': 'NYSE',
                'currency': 'USD',
                'category_id': etf_category.id if etf_category else None
            },
            {
                'symbol': 'VGRO.TO',
                'name': 'Vanguard Growth ETF Portfolio',
                'exchange': 'TSX',
                'currency': 'CAD',
                'category_id': etf_category.id if etf_category else None
            }
        ]
        
        created_stocks = {}
        for stock_data in stocks_data:
            stock = Stock.query.filter_by(symbol=stock_data['symbol']).first()
            if not stock:
                stock = Stock(**stock_data)
                db.session.add(stock)
                db.session.flush()
            created_stocks[stock_data['symbol']] = stock
        
        # 不创建演示交易记录，保持系统初始化时的干净状态
        # 用户可以根据需要手动添加交易记录
        print("  跳过演示交易创建，保持记录为空")
        
        db.session.commit()
        print("  演示家庭数据创建完成！")
        return demo_family