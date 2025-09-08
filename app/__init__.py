import os
from flask import Flask, request, session, g
from flask_sqlalchemy import SQLAlchemy
from flask_migrate import Migrate
from flask_babel import Babel, get_locale
from flask_cors import CORS
from config import config

# 初始化扩展
db = SQLAlchemy()
migrate = Migrate()
babel = Babel()
cors = CORS()

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
    
    # 初始化任务调度器 - 检查配置选项
    if not app.config.get('TESTING') and app.config.get('SCHEDULER_AUTO_START', False):
        from app.scheduler import scheduler
        scheduler.init_app(app)
    
    # 注册蓝图
    from app.api import bp as api_bp
    app.register_blueprint(api_bp, url_prefix='/api/v1')
    
    from app.main import bp as main_bp
    app.register_blueprint(main_bp)
    
    # 初始化 Babel
    babel.init_app(app)
    
    # 语言选择函数 - Flask-Babel 2.0 语法
    @babel.localeselector
    def get_locale():
        # 1. URL参数优先
        if request.args.get('lang'):
            session['language'] = request.args.get('lang')
        
        # 2. 会话存储优先  
        if 'language' in session and session['language'] in app.config['LANGUAGES']:
            return session['language']
            
        # 3. 默认英语
        return 'en'
    
    # 模板全局函数
    @app.template_global()
    def get_current_language():
        return get_locale()
    
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
                    
                    member_data = {
                        'id': member.id,
                        'name': member.name,
                        'accounts': sorted_accounts
                    }
                    family_structure.append(member_data)
                    
                    # 统计唯一账户数量（避免联名账户重复计算）
                    for account in sorted_accounts:
                        unique_accounts.add(account['id'])
                
                total_accounts_count = len(unique_accounts)
            
            return {
                'current_family': family,
                'family_structure': family_structure,
                'total_accounts_count': total_accounts_count,
                'current_member_id': request.args.get('member_id', type=int),
                'current_account_id': request.args.get('account_id', type=int),
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