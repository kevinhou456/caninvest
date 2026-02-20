import os
from decimal import Decimal, InvalidOperation
from flask import Flask, request, session, g
from flask_sqlalchemy import SQLAlchemy
from flask_migrate import Migrate
from flask_babel import Babel, get_locale
from flask_cors import CORS
from sqlalchemy import inspect, text
from config import config

# 初始化扩展
db = SQLAlchemy()
migrate = Migrate()
babel = Babel()
cors = CORS()


def _ensure_performance_indexes(app: Flask) -> None:
    """Ensure critical indexes exist for existing databases."""
    try:
        with app.app_context():
            inspector = inspect(db.engine)
            if 'transactions' not in inspector.get_table_names():
                return

            existing_indexes = {
                idx.get('name')
                for idx in inspector.get_indexes('transactions')
                if idx.get('name')
            }
            required_indexes = {
                'idx_transactions_account_trade_date_id':
                    'CREATE INDEX IF NOT EXISTS idx_transactions_account_trade_date_id '
                    'ON transactions (account_id, trade_date, id)'
            }

            with db.engine.begin() as connection:
                for name, ddl in required_indexes.items():
                    if name in existing_indexes:
                        continue
                    connection.execute(text(ddl))
    except Exception as exc:
        app.logger.warning(f'Failed to ensure performance indexes: {exc}')


def create_app(config_name=None):
    """应用工厂函数"""
    if config_name is None:
        config_name = os.environ.get('FLASK_ENV') or 'default'
    
    app = Flask(__name__)
    app.config.from_object(config[config_name])
    config[config_name].init_app(app)
    
    # 初始化扩展
    db.init_app(app)
    migrate.init_app(app, db)
    cors.init_app(app)
    _ensure_performance_indexes(app)
    
    # 初始化任务调度器 - 检查配置选项
    if not app.config.get('TESTING') and app.config.get('SCHEDULER_AUTO_START', False):
        from app.scheduler import scheduler
        scheduler.init_app(app)
    
    # 初始化数据变更监听器 - 暂时禁用
    # if not app.config.get('TESTING'):
    #     from app.services.data_change_listener import init_data_change_listener
    #     try:
    #         init_data_change_listener(app)
    #     except Exception as e:
    #         app.logger.warning(f"数据变更监听器初始化失败: {e}")
    
    # 注册蓝图
    from app.api import bp as api_bp
    app.register_blueprint(api_bp, url_prefix='/api/v1')
    
    from app.api.daily_stats import daily_stats_bp
    app.register_blueprint(daily_stats_bp)
    
    from app.main import bp as main_bp
    app.register_blueprint(main_bp)
    
    # 配置翻译目录（在初始化Babel之前）
    # 翻译目录在项目根目录，需要使用绝对路径
    project_root = os.path.dirname(app.root_path)
    translations_path = os.path.join(project_root, 'translations')
    app.config['BABEL_TRANSLATION_DIRECTORIES'] = translations_path

    # 初始化 Babel
    babel.init_app(app)

    # 向Jinja2模板环境添加翻译函数
    from flask_babel import gettext, ngettext
    app.jinja_env.globals['_'] = gettext
    app.jinja_env.globals['ngettext'] = ngettext
    
    # 语言选择函数 - Flask-Babel 2.0 语法
    @babel.localeselector
    def get_locale():
        # 1. URL参数优先
        if request.args.get('lang'):
            session['language'] = request.args.get('lang')

        # 2. 会话存储优先
        if 'language' in session:
            requested_lang = session['language']
            # 统一使用zh_CN，避免Windows系统符号链接问题
            if requested_lang == 'zh_CN' or requested_lang == 'zh_Hans_CN':
                return 'zh_CN'
            elif requested_lang in app.config['LANGUAGES']:
                return requested_lang

        # 3. 默认英语
        return 'en'
    
    # 模板全局函数
    @app.template_global()
    def get_current_language():
        return get_locale()
    
    @app.template_filter('format_shares')
    def format_shares_filter(value):
        """Format share quantities: no decimals for whole shares, four decimals for fractions."""
        try:
            num = Decimal(str(value))
        except (InvalidOperation, TypeError, ValueError):
            return "0"
        is_integer = num == num.to_integral()
        fmt = "{:,.0f}" if is_integer else "{:,.4f}"
        return fmt.format(num)
    
    @app.template_global()
    def get_current_date():
        """获取当前日期 - 与持仓计算使用相同的方式"""
        from datetime import date
        return date.today()
    
    @app.template_global()
    def _get_account_name_with_members(account):
        """
        获取带有成员信息的账户名称
        格式: "账户名称 - 所有者"
        """
        if not account:
            return ""

        from app.services.account_service import AccountService
        return AccountService.get_account_name_with_members(account)
    
    @app.template_global()
    def get_sorted_accounts_with_members(accounts):
        """
        获取按成员排序的账户列表，联合账户放在最后

        Args:
            accounts: 账户列表

        Returns:
            list: 排序后的账户列表，使用统一的排序规则
        """
        if not accounts:
            return []

        from app.services.account_service import AccountService
        # 使用统一的排序逻辑，但这里是对现有账户列表排序而不是查询数据库

        # 定义账户类型排序顺序
        account_type_order = {
            'Regular': 1,
            'Margin': 2,
            'TFSA': 3,
            'RRSP': 4,
            'RESP': 5,
            'FHSA': 6
        }

        def get_account_sort_key(account):
            """获取账户排序键"""
            # 联合账户优先级最低，排在所有个人账户之后
            if account.is_joint:
                return (9999, 1000, account.name)

            # 获取账户成员信息
            account_members = account.account_members.all()

            # 获取主要成员或第一个成员的ID（用于匹配sidebar顺序）
            if account_members:
                primary_member = next((am.member for am in account_members if am.is_primary), None)
                if primary_member:
                    member_id = primary_member.id
                else:
                    member_id = account_members[0].member.id
            else:
                member_id = 9998  # 没有成员的账户排在倒数第二

            # 账户类型排序值
            account_type = account.account_type.name if account.account_type else ''
            type_order = account_type_order.get(account_type, 999)

            return (member_id, type_order, account.name)

        # 排序并返回
        return sorted(accounts, key=get_account_sort_key)
    
    @app.template_global()
    def get_current_filter_display(family, member_id=None, account_id=None, include_members=False, account_members=None):
        """获取当前选择的成员或账户显示信息
        
        Args:
            family: 家庭对象
            member_id: 成员ID（可选）
            account_id: 账户ID（可选）
            include_members: 是否包含成员名字（默认False）
            account_members: 账户成员列表（当include_members=True且account_id存在时需要）
            
        Returns:
            dict: 包含display_text, icon_class, type等信息的字典
        """
        if member_id and family and hasattr(family, 'members'):
            # 查找成员
            target_member = None
            for member in family.members:
                if member.id == member_id:
                    target_member = member
                    break
                    
            if target_member:
                return {
                    'display_text': target_member.name,
                    'icon_class': 'fas fa-user',
                    'type': 'member',
                    'id': member_id
                }
            else:
                return {
                    'display_text': f'Member {member_id}',
                    'icon_class': 'fas fa-user',
                    'type': 'member',
                    'id': member_id
                }
                
        elif account_id and family and hasattr(family, 'accounts'):
            # 查找账户
            target_account = None
            for account in family.accounts:
                if account.id == account_id:
                    target_account = account
                    break
                    
            if target_account:
                display_text = target_account.name
                
                # 如果需要包含成员名字且提供了account_members
                if include_members and account_members:
                    current_account_members = [am for am in account_members if am.account_id == account_id]
                    if current_account_members:
                        member_names = [am.member.name for am in current_account_members]
                        display_text += f" ({', '.join(member_names)})"
                
                return {
                    'display_text': display_text,
                    'icon_class': 'fas fa-piggy-bank',
                    'type': 'account',
                    'id': account_id
                }
            else:
                return {
                    'display_text': f'Account {account_id}',
                    'icon_class': 'fas fa-piggy-bank',
                    'type': 'account',
                    'id': account_id
                }
                
        return None
    
    # 投资界面上下文处理器
    @app.context_processor
    def inject_investment_context():
        if request.endpoint and (request.endpoint.startswith('main.') or request.endpoint.startswith('api.')):
            # Get current family and members with accounts
            family = Family.query.first()
            family_structure = []
            unique_accounts = set()
            total_accounts_count = 0
            
            if family:
                # 定义账户类型排序顺序
                account_type_order = {
                    'Regular': 1,
                    'Margin': 2, 
                    'TFSA': 3,
                    'RRSP': 4,
                    'RESP': 5,
                    'FHSA': 6
                }
                
                members = Member.query.filter_by(family_id=family.id).all()
                for member in members:
                    # Get accounts for this member using the get_accounts method
                    accounts = member.get_accounts()
                    
                    # 按账户类型排序账户，联名账户放到最后
                    def account_sort_key(account):
                        # 从数据库获取账户信息来检查是否联名
                        from app.models.account import Account
                        full_account = Account.query.get(account['id'])
                        is_joint = full_account.is_joint if full_account else False
                        
                        # 联名账户排在最后（使用1000作为排序值）
                        if is_joint:
                            return 1000
                        else:
                            return account_type_order.get(account.get('account_type', ''), 999)
                    
                    sorted_accounts = sorted(accounts, key=account_sort_key)
                    
                    # 为联名账户添加成员信息以便在导航栏显示占比
                    for account in sorted_accounts:
                        account['is_joint'] = False  # 默认不是联名
                        account['account_members'] = []  # 默认空的成员信息
                        
                        # 从数据库获取完整的账户信息来检查是否联名
                        from app.models.account import Account
                        full_account = Account.query.get(account['id'])
                        if full_account:
                            account['is_joint'] = full_account.is_joint
                            if full_account.is_joint and full_account.account_members:
                                account['account_members'] = [{
                                    'member_id': am.member_id,
                                    'member_name': am.member.name,
                                    'ownership_percentage': float(am.ownership_percentage)
                                } for am in full_account.account_members]
                    
                    
                    # Group accounts by type (joint -> Regular)
                    account_groups = []
                    type_groups = {}
                    type_order = []
                    for account in sorted_accounts:
                        type_key = account.get('account_type') or 'Regular'
                        if account.get('is_joint'):
                            type_key = 'Regular'
                        if type_key not in type_groups:
                            type_groups[type_key] = []
                            type_order.append(type_key)
                        type_groups[type_key].append(account)

                    # Place Regular group last
                    if 'Regular' in type_order:
                        type_order = [t for t in type_order if t != 'Regular'] + ['Regular']

                    for type_key in type_order:
                        accounts_in_type = type_groups.get(type_key, [])
                        has_joint = any(acc.get('is_joint') for acc in accounts_in_type)
                        show_header = len(accounts_in_type) > 1 or (type_key == 'Regular' and has_joint)
                        account_groups.append({
                            'type': type_key,
                            'accounts': accounts_in_type,
                            'show_header': show_header
                        })

                    member_data = {
                        'id': member.id,
                        'name': member.name,
                        'accounts': sorted_accounts,
                        'account_groups': account_groups
                    }

                    family_structure.append(member_data)
                    
                    # 统计唯一账户数量（避免联名账户重复计算）
                    for account in sorted_accounts:
                        unique_accounts.add(account['id'])
                
                total_accounts_count = len(unique_accounts)
            
            member_id = request.args.get('member_id', type=int)
            account_id = request.args.get('account_id', type=int)
            account_type = request.args.get('account_type')
            if account_id:
                member_id = None
                account_type = None

            return {
                'current_family': family,
                'family_structure': family_structure,
                'total_accounts_count': total_accounts_count,
                'current_member_id': member_id,
                'current_account_id': account_id,
                'current_account_type': account_type,
                'current_view': request.endpoint.split('.')[-1] if request.endpoint else 'dashboard'
            }
        return {}
    
    # 错误处理
    @app.errorhandler(404)
    def not_found_error(error):
        return f'<h1>404 - Page Not Found</h1><p>The requested page was not found.</p>', 404
    
    @app.errorhandler(500)
    def internal_error(error):
        db.session.rollback()
        return f'<h1>500 - Internal Server Error</h1><p>An internal error occurred.</p>', 500
    
    # Shell上下文处理器
    @app.shell_context_processor
    def make_shell_context():
        return {
            'db': db,
            'Family': Family,
            'Member': Member,
            'Account': Account,
            'Transaction': Transaction,
            'StocksCache': StocksCache
        }
    
    return app

# 导入模型（避免循环导入）
from app.models.family import Family
from app.models.member import Member  
from app.models.account import Account, AccountType
from app.models.transaction import Transaction
from app.models.stocks_cache import StocksCache
from app.models.csv_format import CsvFormat
