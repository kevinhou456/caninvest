"""
主要路由 - 页面视图
"""

from flask import render_template, request, jsonify, redirect, url_for, flash, session, current_app
from flask_babel import _
from app.main import bp
from app import db
from app.models.family import Family
from app.models.member import Member
from app.models.account import Account, AccountType, AccountMember
from app.models.transaction import Transaction
# from app.models.stock import Stock, StockCategory  # Stock models deleted
from app.models.stocks_cache import StocksCache
from app.models.import_task import ImportTask, OCRTask
from app.services.analytics_service import analytics_service, TimePeriod
from app.services.currency_service import currency_service


@bp.route('/')
@bp.route('/index')
def index():
    """首页 - 直接重定向到仪表板"""
    return redirect(url_for('main.overview'))


@bp.route('/overview')
def overview():
    """仪表板 - 投资组合总览"""
    # 获取默认家庭（假设只有一个家庭，或者使用第一个家庭）
    family = Family.query.first()
    if not family:
        # 如果没有家庭，创建一个默认家庭
        family = Family(name="我的家庭")
        from app import db
        db.session.add(family)
        db.session.commit()
    
    # 获取过滤参数
    member_id = request.args.get('member_id', type=int)
    account_id = request.args.get('account_id', type=int)
    time_period = request.args.get('period', 'all_time')
    
    # 转换时间段参数
    try:
        period_enum = TimePeriod(time_period)
    except ValueError:
        period_enum = TimePeriod.ALL_TIME
    
    # 使用新的分析服务获取投资组合指标
    try:
        metrics = analytics_service.get_portfolio_metrics(
            family_id=family.id,
            member_id=member_id,
            account_id=account_id,
            period=period_enum
        )
        
        # 获取汇率信息
        exchange_rates = currency_service.get_cad_usd_rates()
        
        # 获取基本统计数据
        from app.models.member import Member
        members_count = Member.query.filter_by(family_id=family.id).count()
        
        # 根据过滤条件获取账户
        if account_id:
            accounts = Account.query.filter_by(id=account_id, family_id=family.id).all()
            filter_description = f"账户: {accounts[0].name}" if accounts else "未找到账户"
        elif member_id:
            from app.models.account import AccountMember
            member_accounts = AccountMember.query.filter_by(member_id=member_id).all()
            account_ids = [am.account_id for am in member_accounts]
            accounts = Account.query.filter(Account.id.in_(account_ids), Account.family_id == family.id).all()
            
            member = Member.query.get(member_id)
            filter_description = f"成员: {member.name}" if member else "未找到成员"
        else:
            accounts = Account.query.filter_by(family_id=family.id).all()
            filter_description = "全部成员"
        
        accounts_count = len(accounts)
        
        # 计算交易数量
        if accounts:
            account_ids = [acc.id for acc in accounts]
            transactions_count = Transaction.query.filter(Transaction.account_id.in_(account_ids)).count()
        else:
            transactions_count = 0
        
        # 获取最近的交易
        if accounts:
            account_ids = [acc.id for acc in accounts]
            recent_transactions = Transaction.query.filter(
                Transaction.account_id.in_(account_ids)
            ).order_by(Transaction.trade_date.desc()).limit(8).all()
        else:
            recent_transactions = []
        
        # 获取股票数量
        stocks_count = StocksCache.query.count()
        
        # 获取待处理任务
        pending_imports = ImportTask.query.filter_by(status='pending').count()
        pending_ocr = OCRTask.query.filter_by(status='pending').count()
        
        stats = {
            'members_count': members_count,
            'accounts_count': accounts_count,
            'transactions_count': transactions_count,
            'stocks_count': stocks_count,
            'pending_imports': pending_imports,
            'pending_ocr': pending_ocr
        }
        
        # 准备持仓数据  
        holdings = metrics.holdings
        cleared_holdings = metrics.cleared_holdings
        
        
        
        return render_template('investment/overview.html',
                             title=_('Overview'),
                             family=family,
                             stats=stats,
                             metrics=metrics.to_dict(),
                             holdings=holdings,
                             cleared_holdings=cleared_holdings,
                             exchange_rates=exchange_rates,
                             recent_transactions=recent_transactions,
                             filter_description=filter_description,
                             current_period=time_period,
                             member_id=member_id,
                             account_id=account_id,
                             current_view='overview')
        
    except Exception as e:
        # 如果新服务出错，回退到基本仪表板
        import logging
        import traceback
        logging.error(f"Dashboard analytics error: {e}")
        logging.error(f"Traceback: {traceback.format_exc()}")
        
        # 基本统计信息  
        from app.models.member import Member
        from app.models.account import AccountMember
        members_count = Member.query.filter_by(family_id=family.id).count()
        accounts_count = Account.query.filter_by(family_id=family.id).count()
        transactions_count = Transaction.query.join(Account).filter(Account.family_id == family.id).count()
        stocks_count = StocksCache.query.count()
        
        stats = {
            'members_count': members_count,
            'accounts_count': accounts_count,
            'transactions_count': transactions_count,
            'stocks_count': stocks_count,
            'pending_imports': 0,
            'pending_ocr': 0
        }
        
        recent_transactions = Transaction.query.join(Account).filter(
            Account.family_id == family.id
        ).order_by(Transaction.trade_date.desc()).limit(8).all()
        
        
        # 使用默认的空metrics对象，确保使用新的overview模板
        return render_template('investment/overview.html',
                             title=_('Overview'),
                             family=family,
                             stats=stats,
                             metrics=None,
                             holdings=[],
                             exchange_rates=None,
                             recent_transactions=recent_transactions,
                             filter_description="全部成员",
                             current_period='all_time',
                             member_id=None,
                             account_id=None,
                             current_view='overview')


@bp.route('/family-members')
def family_members():
    """家庭成员管理页面"""
    family = Family.query.first()
    if not family:
        family = Family(name="我的家庭")
        from app import db
        db.session.add(family)
        db.session.commit()
    
    from app.models.member import Member
    members = Member.query.filter_by(family_id=family.id).order_by(Member.created_at.desc()).all()
    
    return render_template('members/list.html',
                         title=_('Family Members'),
                         family=family,
                         members=members)


@bp.route('/families/create')
def create_family():
    """创建家庭页面"""
    return render_template('families/create.html',
                         title=_('Create Family'))


@bp.route('/families/<int:family_id>')
def family_detail(family_id):
    """家庭详情页面"""
    family = Family.query.get_or_404(family_id)
    return render_template('families/detail.html',
                         title=family.name,
                         family=family)


@bp.route('/families/<int:family_id>/dashboard')
def family_dashboard(family_id):
    """家庭仪表板"""
    family = Family.query.get_or_404(family_id)
    return render_template('families/dashboard.html',
                         title=f"{family.name} - {_('Dashboard')}",
                         family=family)


@bp.route('/families/<int:family_id>/edit', methods=['GET', 'POST'])
def edit_family(family_id):
    """编辑家庭"""
    family = Family.query.get_or_404(family_id)
    
    if request.method == 'POST':
        new_name = request.form.get('name', '').strip()
        if new_name:
            family.name = new_name
            db.session.commit()
            flash(_('Family name updated successfully'), 'success')
        else:
            flash(_('Family name cannot be empty'), 'error')
        return redirect(url_for('main.family_members'))
    
    # For GET requests, redirect to family members page
    return redirect(url_for('main.family_members'))


@bp.route('/accounts')
def accounts():
    """账户列表页面"""
    # 获取默认家庭
    family = Family.query.first()
    if not family:
        family = Family(name="My Family")
        db.session.add(family)
        db.session.commit()
    
    # 获取账户列表（按ID排序）
    accounts = Account.query.filter_by(family_id=family.id).order_by(Account.id.asc()).all()
    
    # 获取账户类型和成员数据供模态框使用
    account_types = AccountType.query.all()
    members = Member.query.filter_by(family_id=family.id).all()
    
    return render_template('accounts/list.html',
                         title=_('Account Management'),
                         accounts=accounts,
                         account_types=account_types,
                         members=members,
                         family=family)


@bp.route('/accounts/create', methods=['GET', 'POST'])
def create_account():
    """创建账户"""
    if request.method == 'POST':
        try:
            # 获取基本信息
            name = request.form.get('name')
            account_type_id = request.form.get('account_type_id')
            is_joint = 'is_joint' in request.form
            
            # 验证账户类型是否支持联名账户
            if is_joint and account_type_id:
                account_type = AccountType.query.get(account_type_id)
                if account_type and account_type.name not in ['Regular', 'Margin']:
                    flash(_('Only Regular and Margin accounts can be joint accounts. Tax-advantaged accounts (TFSA, RRSP, RESP, FHSA) must have a single owner.'), 'error')
                    return redirect(url_for('main.accounts'))
            
            # 获取家庭ID（假设只有一个家庭）
            family = Family.query.first()
            if not family:
                flash(_('No family found'), 'error')
                return redirect(url_for('main.accounts'))
            
            # 创建账户
            account = Account(
                name=name,
                family_id=family.id,
                account_type_id=account_type_id,
                is_joint=is_joint,
                currency='CAD'  # 默认CAD
            )
            db.session.add(account)
            db.session.flush()  # 获取account.id
            
            if is_joint:
                # 处理联合账户成员
                members = request.form.getlist('members')
                total_percentage = 0
                
                for member_id in members:
                    percentage = float(request.form.get(f'percentage_{member_id}', 0))
                    total_percentage += percentage
                    
                    account_member = AccountMember(
                        account_id=account.id,
                        member_id=int(member_id),
                        ownership_percentage=percentage,
                        is_primary=(member_id == members[0])
                    )
                    db.session.add(account_member)
                
                if abs(total_percentage - 100.0) > 0.01:
                    flash(_('Total percentage must equal 100%%'), 'error')
                    db.session.rollback()
                    return redirect(url_for('main.accounts'))
            else:
                # 处理单一所有者账户
                single_owner_id = request.form.get('single_owner')
                if not single_owner_id:
                    flash(_('Please select an account owner'), 'error')
                    db.session.rollback()
                    return redirect(url_for('main.accounts'))
                
                account_member = AccountMember(
                    account_id=account.id,
                    member_id=int(single_owner_id),
                    ownership_percentage=100.0,
                    is_primary=True
                )
                db.session.add(account_member)
            
            db.session.commit()
            flash(_('Account created successfully'), 'success')
            
        except Exception as e:
            db.session.rollback()
            flash(_('Error creating account: {}').format(str(e)), 'error')
    
    return redirect(url_for('main.accounts'))


@bp.route('/account-types')
def account_types():
    """账户类型管理页面"""
    account_types = AccountType.query.all()
    return render_template('accounts/types.html',
                         title=_('Account Types'),
                         account_types=account_types)


@bp.route('/transactions')
def transactions():
    """交易记录列表页面"""
    try:
        page = request.args.get('page', 1, type=int)
        account_id = request.args.get('account_id', type=int)
        type_filter = request.args.get('type')
        
        # 构建查询
        query = Transaction.query
        if account_id:
            query = query.filter(Transaction.account_id == account_id)
        if type_filter:
            query = query.filter(Transaction.type == type_filter)
        
        # 执行分页查询
        transactions = query.order_by(Transaction.trade_date.desc()).paginate(
            page=page, per_page=50, error_out=False
        )
        
        # 获取所有账户
        accounts = Account.query.all()
        
        return render_template('transactions/list.html',
                             title=_('Transactions'),
                             transactions=transactions,
                             accounts=accounts,
                             current_view='transactions')
    except Exception as e:
        current_app.logger.error(f"Error in transactions route: {str(e)}")
        flash(_('Error loading transactions: ') + str(e), 'error')
        return redirect(url_for('main.overview'))


def save_transaction_record(transaction_id=None, account_id=None, transaction_type=None, quantity=None, price=None, currency=None, stock=None, fee=0.0, trade_date=None, notes='', amount=None):
    """
    统一的交易记录数据库操作函数
    - 如果 transaction_id 为 None，则创建新记录
    - 如果 transaction_id 不为 None，则修改现有记录
    """
    
    try:
        if transaction_id is None:
            # 创建新记录 - 如果没有提供交易日期，使用今天
            if trade_date is None:
                from datetime import date
                trade_date = date.today()
                
            # 验证必需字段
            if not account_id:
                raise ValueError("account_id is required")
            if not transaction_type:
                raise ValueError("transaction_type is required")
            if not currency:
                raise ValueError("currency is required")
            
            # 对于股票交易，验证quantity和price是必需的
            if transaction_type in ['BUY', 'SELL']:
                if not quantity:
                    raise ValueError("quantity is required for stock transactions")
                if not price:
                    raise ValueError("price is required for stock transactions")
            
            transaction = Transaction(
                account_id=account_id,
                stock=stock,  # 直接使用stock，可能为None
                type=transaction_type,
                quantity=quantity,
                price=price,
                amount=amount,  # 新增amount字段
                fee=fee,
                trade_date=trade_date,
                currency=currency,
                notes=notes
            )
            db.session.add(transaction)
        else:
            # 修改现有记录
            transaction = Transaction.query.get(transaction_id)
            if not transaction:
                raise ValueError(f"Transaction with ID {transaction_id} not found")
            
            # 只更新提供的字段
            if account_id is not None:
                transaction.account_id = account_id
            if transaction_type is not None:
                transaction.type = transaction_type
            if quantity is not None:
                transaction.quantity = quantity
            if price is not None:
                transaction.price = price
            if currency is not None:
                transaction.currency = currency
            if stock is not None:
                transaction.stock = stock
            if fee is not None:
                transaction.fee = fee
            if trade_date is not None:
                transaction.trade_date = trade_date
            if notes is not None:
                transaction.notes = notes
        
        db.session.commit()
        
        flash(f"成功保存交易记录! ID: {transaction.id}", 'success')
        return transaction
        
    except Exception as e:
        print(f"DEBUG: ❌ Exception occurred in save_transaction_record: {str(e)}")
        print(f"DEBUG: Exception type: {type(e)}")
        import traceback
        print(f"DEBUG: Full traceback:")
        traceback.print_exc()
        
        db.session.rollback()
        print(f"DEBUG: Database session rolled back")
        
        error_msg = f"数据库保存失败: {str(e)}"
        flash(error_msg, 'error')
        raise Exception(error_msg)

@bp.route('/transactions/create', methods=['GET', 'POST'])
def create_transaction():
    """创建交易记录"""
    if request.method == 'POST':
        # 获取表单数据
        account_id = request.form.get('account_id')
        type = request.form.get('type')
        quantity = request.form.get('quantity')
        price = request.form.get('price')
        fee = request.form.get('fee', 0)
        trade_date = request.form.get('trade_date')
        currency = request.form.get('currency')
        notes = request.form.get('notes', '').strip()
        amount = request.form.get('amount')  # 新增amount参数
        
        # 根据交易类型处理股票代码
        stock_symbol = None
        if type in ['BUY', 'SELL', 'DIVIDEND', 'INTEREST']:
            stock_symbol = request.form.get('stock_symbol', '').strip().upper()
            if not stock_symbol:
                transaction_type_name = {
                    'BUY': '买入',
                    'SELL': '卖出', 
                    'DIVIDEND': '分红',
                    'INTEREST': '利息'
                }.get(type, type)
                flash(f'{transaction_type_name}交易需要股票代码', 'error')
                return redirect(url_for('main.transactions', account_id=account_id))
        # DEPOSIT, WITHDRAWAL, FEE 等现金交易不需要股票代码
        
        # 验证必填字段
        if not all([account_id, type, trade_date, currency]):
            flash(_('Please fill in all required fields'), 'error')
            return redirect(url_for('main.transactions', account_id=account_id))
        
        # 对于现金交易，验证amount字段
        if type in ['DEPOSIT', 'WITHDRAWAL', 'DIVIDEND', 'INTEREST']:
            if not amount:
                flash(_('Amount is required for this transaction type'), 'error')
                return redirect(url_for('main.transactions', account_id=account_id))
        
        # 对于股票交易，验证quantity和price字段
        if type in ['BUY', 'SELL']:
            if not quantity or not price:
                flash(_('Quantity and price are required for stock transactions'), 'error')
                return redirect(url_for('main.transactions', account_id=account_id))
        
       
        try:
            # 转换数据类型
            from datetime import datetime
            trade_date = datetime.strptime(trade_date, '%Y-%m-%d').date()
            fee = float(fee) if fee else 0
            
            # 根据交易类型处理数据
            if type in ['DEPOSIT', 'WITHDRAWAL', 'DIVIDEND', 'INTEREST']:
                # 现金交易使用amount字段
                amount_value = float(amount) if amount else 0
                # quantity和price保持原值，如果为空则设为0（因为数据库不允许NULL）
                quantity = float(quantity) if quantity else 0
                price = float(price) if price else 0
            else:
                # 股票交易使用quantity和price
                quantity = float(quantity) if quantity else 0
                price = float(price) if price else 0
                amount_value = None  # 股票交易不使用amount字段
            
            print(f"DEBUG: Creating {type} transaction for stock={stock_symbol or 'None'}, amount={amount_value}")
            
            # 使用统一函数创建交易记录
            try:
                transaction = save_transaction_record(
                    account_id=int(account_id),
                    transaction_type=type,
                    quantity=quantity,
                    price=price,
                    amount=amount_value,
                    currency=currency,
                    stock=stock_symbol,
                    fee=fee,
                    trade_date=trade_date,
                    notes=notes
                )
                print(f"DEBUG: ✅ Transaction saved with ID: {transaction.id}")
                    
            except Exception as save_error:
                print(f"DEBUG: ERROR in save_transaction_record: {save_error}")
                raise save_error
            flash(_('Transaction created successfully'), 'success')
            return redirect(url_for('main.transactions', account_id=account_id))
            
        except Exception as e:
            db.session.rollback()
            flash(f'Error creating transaction: {str(e)}', 'error')
            return redirect(url_for('main.transactions', account_id=account_id))
    
    # GET request - show form
    accounts = Account.query.all()
    # stocks = StocksCache.query.all()  # 暂时不需要预加载股票
    family_members = Member.query.all()
    
    from datetime import date
    return render_template('transactions/create.html',
                         title=_('Create Transaction'),
                         accounts=accounts,
                         # stocks=stocks,  # 暂时不需要股票列表
                         family_members=family_members,
                         today=date.today())


@bp.route('/stocks')
def stocks():
    """股票缓存管理页面"""
    page = request.args.get('page', 1, type=int)
    # category_id = request.args.get('category_id', type=int)  # 暂时移除分类功能
    search = request.args.get('search', '')
    
    query = StocksCache.query
    # if category_id:  # 暂时移除分类功能
    #     query = query.filter_by(category_id=category_id)
    if search:
        query = query.filter(StocksCache.symbol.contains(search) | StocksCache.name.contains(search))
    
    stocks = query.order_by(StocksCache.symbol).paginate(
        page=page, per_page=50, error_out=False
    )
    
    # categories = StockCategory.query.all()  # 暂时移除分类功能
    
    return render_template('stocks/list.html',
                         title=_('Stocks Cache'),
                         stocks=stocks,
                         # categories=categories,  # 暂时移除分类功能
                         search=search)


@bp.route('/stocks/<symbol>')
def stock_detail(symbol):
    """股票详情页面"""
    stock = StocksCache.query.filter_by(symbol=symbol).first_or_404()
    return render_template('stocks/detail.html',
                         title=f"{stock.symbol} - {stock.name}",
                         stock=stock)


@bp.route('/categories')
def categories():
    """股票分类管理页面 - 暂时禁用"""
    # categories = StockCategory.query.order_by(StockCategory.sort_order).all()  # 分类功能暂时移除
    return render_template('categories/list.html',
                         title=_('Stock Categories - Disabled'),
                         # categories=categories  # 分类功能暂时移除
                         )


@bp.route('/import-transactions')
def import_transactions():
    """数据导入页面"""
    accounts = Account.query.all()
    
    # 获取预选账户ID
    preselected_account_id = request.args.get('account_id', type=int)
    
    # 获取最近的导入任务
    recent_imports = ImportTask.query.order_by(ImportTask.created_at.desc()).limit(10).all()
    recent_ocr = OCRTask.query.order_by(OCRTask.created_at.desc()).limit(10).all()
    
    return render_template('imports/index.html',
                         title=_('Import Data'),
                         accounts=accounts,
                         preselected_account_id=preselected_account_id,
                         recent_imports=recent_imports,
                         recent_ocr=recent_ocr)


@bp.route('/ocr-tasks/<int:task_id>/review')
def review_ocr_task(task_id):
    """OCR任务审核页面"""
    task = OCRTask.query.get_or_404(task_id)
    return render_template('imports/ocr_review.html',
                         title=f"{_('Review OCR Task')} #{task.id}",
                         task=task)


@bp.route('/portfolio-reports')
def portfolio_reports():
    """投资组合报告页面"""
    families = Family.query.all()
    return render_template('reports/portfolio.html',
                         title=_('Portfolio Reports'),
                         families=families)


@bp.route('/performance-reports')
def performance_reports():
    """表现报告页面"""
    accounts = Account.query.all()
    return render_template('reports/performance.html',
                         title=_('Performance Reports'),
                         accounts=accounts)


@bp.route('/tax-reports')
def tax_reports():
    """税务报告页面"""
    families = Family.query.all()
    return render_template('reports/tax.html',
                         title=_('Tax Reports'),
                         families=families)


@bp.route('/settings')
def settings():
    """设置页面"""
    return render_template('main/settings.html',
                         title=_('Settings'))


@bp.route('/about')
def about():
    """关于页面"""
    return render_template('main/about.html',
                         title=_('About'))


@bp.route('/privacy')
def privacy():
    """隐私政策页面"""
    return render_template('main/privacy.html',
                         title=_('Privacy Policy'))


@bp.route('/set-language', methods=['POST'])
def set_language():
    """设置语言"""
    language = request.json.get('language')
    if language and language in ['en', 'zh_CN']:
        session['language'] = language
        return jsonify({'success': True, 'language': language})
    return jsonify({'success': False, 'error': 'Invalid language'}), 400



@bp.route('/api/translations')
def get_translations():
    """获取前端翻译"""
    translations = {
        'Language switched successfully': _('Language switched successfully'),
        'Failed to switch language': _('Failed to switch language'),
        'Select Language': _('Select Language'),
        'Loading...': _('Loading...'),
        'Save': _('Save'),
        'Cancel': _('Cancel'),
        'Delete': _('Delete'),
        'Edit': _('Edit'),
        'Add': _('Add'),
        'Success': _('Success'),
        'Error': _('Error'),
        'Warning': _('Warning'),
        'Confirmation': _('Confirmation'),
        'Are you sure?': _('Are you sure?'),
        'Total Value': _('Total Value'),
        'Unrealized Gain': _('Unrealized Gain'),
        'Realized Gain': _('Realized Gain'),
        'Buy': _('Buy'),
        'Sell': _('Sell'),
        'Quantity': _('Quantity'),
        'Price': _('Price'),
        'Date': _('Date'),
        'Notes': _('Notes'),
        'Categories': _('Categories'),
        'Import CSV': _('Import CSV'),
        'Import Screenshot': _('Import Screenshot'),
        'Export Data': _('Export Data')
    }
    
    return jsonify(translations)

@bp.route('/api/stock-lookup')
def stock_lookup():
    """股票信息查找API"""
    symbol = request.args.get('symbol', '').strip().upper()
    currency = request.args.get('currency', '').upper()
    
    if not symbol or not currency:
        return jsonify({'success': False, 'error': 'Symbol and currency are required'})
    
    # 在股票缓存中查找股票
    stock_cache = StocksCache.query.filter_by(symbol=symbol).first()
    
    if stock_cache:
        return jsonify({
            'success': True,
            'stock': {
                'id': stock_cache.id,
                'symbol': stock_cache.symbol,
                'name': stock_cache.name,
                'exchange': stock_cache.exchange,
                # 'currency': currency  # currency不在StocksCache中存储
            }
        })
    else:
        return jsonify({
            'success': False,
            'message': 'Stock not found in cache'
        })


# 占位符路由 - 用于模板链接，避免 BuildError
@bp.route('/accounts/<int:account_id>/edit', methods=['POST'])
def edit_account(account_id):
    """编辑账户"""
    try:
        account = Account.query.get_or_404(account_id)
        old_account_type_id = account.account_type_id
        new_account_type_id = int(request.form.get('account_type_id'))
        
        # 获取新账户类型信息
        new_account_type = AccountType.query.get(new_account_type_id)
        if not new_account_type:
            flash(_('Invalid account type'), 'error')
            return redirect(url_for('main.accounts'))
        
        # 检查账户类型变更的合法性 - 加强验证
        if account.is_joint and new_account_type.name not in ['Regular', 'Margin']:
            flash(_('Joint accounts can only be Regular or Margin types. Tax-advantaged accounts (TFSA, RRSP, RESP, FHSA) can only have single owners.'), 'error')
            return redirect(url_for('main.accounts'))
        
        # 检查多成员账户变更为税收优惠账户的情况
        account_members_count = AccountMember.query.filter_by(account_id=account.id).count()
        if account_members_count > 1 and new_account_type.name not in ['Regular', 'Margin']:
            flash(_('Accounts with multiple members can only be Regular or Margin types. Tax-advantaged accounts can only have single owners.'), 'error')
            return redirect(url_for('main.accounts'))
        
        # 更新基本信息
        account.name = request.form.get('name')
        account.account_type_id = new_account_type_id
        
        # 如果从Taxable改为税收优惠账户，需要确保只有一个成员
        type_changed_to_single_owner = (
            old_account_type_id != new_account_type_id and 
            new_account_type.name in ['TFSA', 'RRSP', 'RESP', 'FHSA']
        )
        
        if type_changed_to_single_owner and account.is_joint:
            flash(_('Cannot change joint account to tax-advantaged type'), 'error')
            db.session.rollback()
            return redirect(url_for('main.accounts'))
        elif type_changed_to_single_owner:
            # 如果原来不是联名账户但有多个成员，重置为单一成员100%所有权
            account_members = AccountMember.query.filter_by(account_id=account.id).all()
            if len(account_members) > 1:
                # 删除现有成员关系
                AccountMember.query.filter_by(account_id=account.id).delete()
                # 只保留第一个成员，100%所有权
                primary_member = account_members[0]
                account_member = AccountMember(
                    account_id=account.id,
                    member_id=primary_member.member_id,
                    ownership_percentage=100.0,
                    is_primary=True
                )
                db.session.add(account_member)
        else:
            # 正常的成员更新逻辑
            if account.is_joint:
                # 删除现有成员关系
                AccountMember.query.filter_by(account_id=account.id).delete()
                
                # 添加新的成员关系
                total_percentage = 0
                member_ids = []
                
                for key in request.form.keys():
                    if key.startswith('member_'):
                        member_id = int(key.split('_')[1])
                        member_ids.append(member_id)
                        percentage = float(request.form.get(f'percentage_{member_id}', 0))
                        total_percentage += percentage
                        
                        account_member = AccountMember(
                            account_id=account.id,
                            member_id=member_id,
                            ownership_percentage=percentage,
                            is_primary=(member_id == member_ids[0] if member_ids else False)
                        )
                        db.session.add(account_member)
                
                if abs(total_percentage - 100.0) > 0.01:
                    flash(_('Total percentage must equal 100%%'), 'error')
                    db.session.rollback()
                    return redirect(url_for('main.accounts'))
        
        db.session.commit()
        flash(_('Account updated successfully'), 'success')
        
    except Exception as e:
        db.session.rollback()
        flash(_('Error updating account: {}').format(str(e)), 'error')
    
    return redirect(url_for('main.accounts'))


@bp.route('/accounts/<int:account_id>/delete', methods=['POST'])
def delete_account(account_id):
    """删除账户"""
    try:
        account = Account.query.get_or_404(account_id)
        account_name = account.name
        
        # 删除相关的账户成员关系
        AccountMember.query.filter_by(account_id=account.id).delete()
        
        # 删除账户
        db.session.delete(account)
        db.session.commit()
        
        flash(_('Account "{}" has been deleted successfully').format(account_name), 'success')
        
    except Exception as e:
        db.session.rollback()
        flash(_('Error deleting account: {}').format(str(e)), 'error')
    
    return redirect(url_for('main.accounts'))


@bp.route('/api/v1/accounts/<int:account_id>/edit')
def api_get_account_for_edit(account_id):
    """获取账户编辑信息 API"""
    try:
        account = Account.query.get_or_404(account_id)
        
        account_data = {
            'id': account.id,
            'name': account.name,
            'account_type_id': account.account_type_id,
            'is_joint': account.is_joint,
            'account_members': []
        }
        
        # 添加账户成员信息
        for am in account.account_members:
            account_data['account_members'].append({
                'member_id': am.member_id,
                'member_name': am.member.name,
                'ownership_percentage': float(am.ownership_percentage)
            })
        
        return jsonify({'success': True, 'account': account_data})
    
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500

@bp.route('/api/v1/accounts/<int:account_id>')
def api_get_account(account_id):
    """获取账户详细信息 API"""
    try:
        account = Account.query.get_or_404(account_id)
        return jsonify(account.to_dict(include_summary=True))
    
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@bp.route('/accounts/<int:account_id>')
def account_detail(account_id):
    """账户详情页面"""
    account = Account.query.get_or_404(account_id)
    return render_template('accounts/detail.html',
                         title=account.name,
                         account=account)

@bp.route('/transactions/<int:transaction_id>')
def transaction_detail(transaction_id):
    """交易记录详情页面"""
    transaction = Transaction.query.get_or_404(transaction_id)
    return render_template('transactions/detail.html',
                         title=f"{_('Transaction')} #{transaction.id}",
                         transaction=transaction)

@bp.route('/transactions/<int:transaction_id>/edit')
def edit_transaction(transaction_id):
    """编辑交易 - 占位符"""
    return f"<h1>Edit Transaction {transaction_id}</h1><p>This feature is under development.</p>"

@bp.route('/stocks/<int:stock_id>')
def stock_detail_id(stock_id):
    """股票详情 - 占位符"""
    return f"<h1>Stock {stock_id} Detail</h1><p>This feature is under development.</p>"

@bp.route('/stocks/<int:stock_id>/edit')
def edit_stock(stock_id):
    """编辑股票 - 占位符"""
    return f"<h1>Edit Stock {stock_id}</h1><p>This feature is under development.</p>"

@bp.route('/stocks/create')
def create_stock():
    """创建股票 - 占位符"""
    return "<h1>Create Stock</h1><p>This feature is under development.</p>"

@bp.route('/stocks/categories')
def stock_categories():
    """股票分类管理页面"""
    try:
        from app.models.stock_category import StockCategory
        
        # 获取所有分类及其股票数量
        categories = StockCategory.get_all_with_counts()
        
        # 获取未分类的股票
        uncategorized_stocks = StocksCache.query.filter(
            StocksCache.category_id.is_(None)
        ).all()
        
        # 获取所有分类的详细信息（用于编辑）
        all_categories = StockCategory.query.all()
        
        # 获取所有股票
        all_stocks = StocksCache.query.all()
        
        return render_template('stocks/categories.html',
                             title=_('Manage Stock Category'),
                             categories=categories,
                             uncategorized=uncategorized_stocks,
                             all_categories=all_categories,
                             all_stocks=all_stocks,
                             current_view='stock_categories')
    except Exception as e:
        flash(_('Error loading stock categories: ') + str(e), 'error')
        return redirect(url_for('main.overview'))

# 股票分类CRUD路由
@bp.route('/api/stock-categories', methods=['POST'])
def create_stock_category():
    """创建股票分类"""
    try:
        from app.models.stock_category import StockCategory
        data = request.get_json()
        
        if not data or not data.get('name'):
            return jsonify({'success': False, 'error': _('Category name is required')}), 400
        
        # 检查是否已存在同名分类
        existing = StockCategory.query.filter_by(name=data['name']).first()
        if existing:
            return jsonify({'success': False, 'error': _('Category name already exists')}), 400
        
        category = StockCategory(
            name=data['name'],
            name_en=data.get('name_en', ''),
            description=data.get('description', ''),
            color=data.get('color', '#007bff')
        )
        
        db.session.add(category)
        db.session.commit()
        
        return jsonify({
            'success': True,
            'message': _('Category created successfully'),
            'category': category.to_dict()
        })
        
    except Exception as e:
        db.session.rollback()
        return jsonify({'success': False, 'error': str(e)}), 500

@bp.route('/api/stock-categories/<int:category_id>', methods=['PUT'])
def update_stock_category(category_id):
    """更新股票分类"""
    try:
        from app.models.stock_category import StockCategory
        category = StockCategory.query.get_or_404(category_id)
        data = request.get_json()
        
        if not data or not data.get('name'):
            return jsonify({'success': False, 'error': _('Category name is required')}), 400
        
        # 检查是否已存在同名分类（除了当前分类）
        existing = StockCategory.query.filter(
            StockCategory.name == data['name'],
            StockCategory.id != category_id
        ).first()
        if existing:
            return jsonify({'success': False, 'error': _('Category name already exists')}), 400
        
        category.name = data['name']
        category.name_en = data.get('name_en', '')
        category.description = data.get('description', '')
        category.color = data.get('color', category.color)
        
        db.session.commit()
        
        return jsonify({
            'success': True,
            'message': _('Category updated successfully'),
            'category': category.to_dict()
        })
        
    except Exception as e:
        db.session.rollback()
        return jsonify({'success': False, 'error': str(e)}), 500

@bp.route('/api/stock-categories/<int:category_id>', methods=['DELETE'])
def delete_stock_category(category_id):
    """删除股票分类"""
    try:
        from app.models.stock_category import StockCategory
        category = StockCategory.query.get_or_404(category_id)
        
        # 检查是否有股票使用这个分类
        stock_count = StocksCache.query.filter_by(category_id=category_id).count()
        if stock_count > 0:
            return jsonify({
                'success': False, 
                'error': _('Cannot delete category with %(count)d stocks. Please reassign stocks first.', count=stock_count)
            }), 400
        
        db.session.delete(category)
        db.session.commit()
        
        return jsonify({
            'success': True,
            'message': _('Category deleted successfully')
        })
        
    except Exception as e:
        db.session.rollback()
        return jsonify({'success': False, 'error': str(e)}), 500

@bp.route('/api/stocks/<int:stock_id>/category', methods=['PUT'])
def assign_stock_category(stock_id):
    """分配股票分类"""
    try:
        stock = StocksCache.query.get_or_404(stock_id)
        data = request.get_json()
        
        category_id = data.get('category_id') if data else None
        stock.category_id = category_id
        
        db.session.commit()
        
        return jsonify({
            'success': True,
            'message': _('Stock category updated successfully')
        })
        
    except Exception as e:
        db.session.rollback()
        return jsonify({'success': False, 'error': str(e)}), 500

@bp.route('/members/create', methods=['GET', 'POST'])
def create_member():
    """创建家庭成员"""
    if request.method == 'POST':
        name = request.form.get('name', '').strip()
        if name:
            family = Family.query.first()
            if family:
                new_member = Member(
                    family_id=family.id,
                    name=name
                )
                db.session.add(new_member)
                db.session.commit()
                flash(_('Member added successfully'), 'success')
            else:
                flash(_('No family found'), 'error')
        else:
            flash(_('Member name cannot be empty'), 'error')
        return redirect(url_for('main.family_members'))
    
    # For GET requests, redirect to family members page
    return redirect(url_for('main.family_members'))

@bp.route('/members/<int:member_id>')
def member_detail(member_id):
    """家庭成员详情 - 占位符"""
    return f"<h1>Member {member_id} Detail</h1><p>This feature is under development.</p>"

@bp.route('/members/<int:member_id>/edit', methods=['GET', 'POST'])
def edit_member(member_id):
    """编辑家庭成员"""
    member = Member.query.get_or_404(member_id)
    
    if request.method == 'POST':
        new_name = request.form.get('name', '').strip()
        if new_name:
            member.name = new_name
            db.session.commit()
            flash(_('Member updated successfully'), 'success')
        else:
            flash(_('Member name cannot be empty'), 'error')
        return redirect(url_for('main.family_members'))
    
    # For GET requests, redirect to family members page
    return redirect(url_for('main.family_members'))

@bp.route('/members/<int:member_id>/delete', methods=['POST'])
def delete_member(member_id):
    """删除家庭成员"""
    member = Member.query.get_or_404(member_id)
    member_name = member.name
    
    try:
        db.session.delete(member)
        db.session.commit()
        flash(_('Member "{}" has been deleted successfully').format(member_name), 'success')
    except Exception as e:
        db.session.rollback()
        flash(_('Error deleting member: {}').format(str(e)), 'error')
    
    return redirect(url_for('main.family_members'))


# 统计视图路由
@bp.route('/annual-stats')
def annual_stats():
    """年度统计视图"""
    family = Family.query.first()
    if not family:
        family = Family(name="我的家庭")
        from app import db
        db.session.add(family)
        db.session.commit()
    
    # 获取年度数据（简化版）
    from datetime import datetime, timedelta
    from sqlalchemy import func, extract
    
    current_year = datetime.now().year
    years = range(current_year - 4, current_year + 1)  # 最近5年
    
    annual_data = []
    for year in years:
        year_transactions = Transaction.query.join(Account).filter(
            Account.family_id == family.id,
            extract('year', Transaction.trade_date) == year
        ).all()
        
        buy_amount = sum(t.quantity * t.price for t in year_transactions if t.type == 'buy')
        sell_amount = sum(t.quantity * t.price for t in year_transactions if t.type == 'sell')
        net_investment = buy_amount - sell_amount
        
        annual_data.append({
            'year': year,
            'buy_amount': buy_amount,
            'sell_amount': sell_amount,
            'net_investment': net_investment,
            'transaction_count': len(year_transactions)
        })
    
    return render_template('investment/annual_stats.html',
                         title=_('Annual Statistics'),
                         annual_data=annual_data,
                         current_year=current_year)


@bp.route('/monthly-stats')
def monthly_stats():
    """月度统计视图"""
    family = Family.query.first()
    if not family:
        family = Family(name="我的家庭")
        from app import db
        db.session.add(family)
        db.session.commit()
    
    from datetime import datetime, timedelta
    from sqlalchemy import extract
    
    # 获取最近12个月的数据
    current_date = datetime.now()
    monthly_data = []
    
    for i in range(12):
        month_date = current_date - timedelta(days=30*i)
        year = month_date.year
        month = month_date.month
        
        month_transactions = Transaction.query.join(Account).filter(
            Account.family_id == family.id,
            extract('year', Transaction.trade_date) == year,
            extract('month', Transaction.trade_date) == month
        ).all()
        
        buy_amount = sum(t.quantity * t.price for t in month_transactions if t.type == 'buy')
        sell_amount = sum(t.quantity * t.price for t in month_transactions if t.type == 'sell')
        
        monthly_data.append({
            'year': year,
            'month': month,
            'month_name': month_date.strftime('%Y-%m'),
            'buy_amount': buy_amount,
            'sell_amount': sell_amount,
            'net_investment': buy_amount - sell_amount,
            'transaction_count': len(month_transactions)
        })
    
    monthly_data.reverse()  # 按时间顺序排列
    
    return render_template('investment/monthly_stats.html',
                         title=_('Monthly Statistics'),
                         monthly_data=monthly_data)


@bp.route('/quarterly-stats')
def quarterly_stats():
    """季度统计视图"""
    family = Family.query.first()
    if not family:
        family = Family(name="我的家庭")
        from app import db
        db.session.add(family)
        db.session.commit()
    
    from datetime import datetime
    from sqlalchemy import extract
    
    current_year = datetime.now().year
    current_quarter = (datetime.now().month - 1) // 3 + 1
    
    quarterly_data = []
    
    # 获取最近8个季度的数据
    for year_offset in range(2):
        year = current_year - year_offset
        for quarter in [4, 3, 2, 1]:
            if year == current_year and quarter > current_quarter:
                continue
                
            # 计算季度月份范围
            start_month = (quarter - 1) * 3 + 1
            end_month = quarter * 3
            
            quarter_transactions = Transaction.query.join(Account).filter(
                Account.family_id == family.id,
                extract('year', Transaction.trade_date) == year,
                extract('month', Transaction.trade_date) >= start_month,
                extract('month', Transaction.trade_date) <= end_month
            ).all()
            
            buy_amount = sum(t.quantity * t.price for t in quarter_transactions if t.type == 'buy')
            sell_amount = sum(t.quantity * t.price for t in quarter_transactions if t.type == 'sell')
            
            quarterly_data.append({
                'year': year,
                'quarter': quarter,
                'quarter_name': f'{year}Q{quarter}',
                'buy_amount': buy_amount,
                'sell_amount': sell_amount,
                'net_investment': buy_amount - sell_amount,
                'transaction_count': len(quarter_transactions)
            })
    
    quarterly_data.reverse()  # 按时间顺序排列
    
    return render_template('investment/quarterly_stats.html',
                         title=_('Quarterly Statistics'),
                         quarterly_data=quarterly_data)


@bp.route('/daily-stats')
def daily_stats():
    """每日统计视图"""
    family = Family.query.first()
    if not family:
        family = Family(name="我的家庭")
        from app import db
        db.session.add(family)
        db.session.commit()
    
    from datetime import datetime, timedelta
    from sqlalchemy import func
    
    # 获取最近30天的数据
    end_date = datetime.now().date()
    start_date = end_date - timedelta(days=29)
    
    daily_data = []
    current_date = start_date
    
    while current_date <= end_date:
        day_transactions = Transaction.query.join(Account).filter(
            Account.family_id == family.id,
            func.date(Transaction.trade_date) == current_date
        ).all()
        
        buy_amount = sum(t.quantity * t.price for t in day_transactions if t.type == 'buy')
        sell_amount = sum(t.quantity * t.price for t in day_transactions if t.type == 'sell')
        
        daily_data.append({
            'date': current_date,
            'date_str': current_date.strftime('%Y-%m-%d'),
            'weekday': current_date.strftime('%A'),
            'buy_amount': buy_amount,
            'sell_amount': sell_amount,
            'net_investment': buy_amount - sell_amount,
            'transaction_count': len(day_transactions)
        })
        
        current_date += timedelta(days=1)
    
    return render_template('investment/daily_stats.html',
                         title=_('Daily Statistics'),
                         daily_data=daily_data)


@bp.route('/holdings-analysis')
def holdings_analysis():
    """持仓分析视图"""
    family = Family.query.first()
    if not family:
        family = Family(name="我的家庭")
        from app import db
        db.session.add(family)
        db.session.commit()
    
    # 获取所有账户的持仓信息（简化版）
    accounts = Account.query.filter_by(family_id=family.id).all()
    
    holdings_by_stock = {}
    holdings_by_category = {}
    holdings_by_account = []
    
    for account in accounts:
        # 计算每个账户的持仓（简化版 - 基于交易记录）
        account_transactions = Transaction.query.filter_by(account_id=account.id).all()
        account_holdings = {}
        
        for transaction in account_transactions:
            symbol = transaction.stock if transaction.stock else 'CASH'
            if symbol not in account_holdings:
                account_holdings[symbol] = {
                    'quantity': 0,
                    'total_cost': 0,
                    'stock': symbol  # 现在直接使用股票代码字符串
                }
            
            if transaction.type == 'buy':
                account_holdings[symbol]['quantity'] += transaction.quantity
                account_holdings[symbol]['total_cost'] += transaction.quantity * transaction.price
            elif transaction.type == 'sell':
                account_holdings[symbol]['quantity'] -= transaction.quantity
                account_holdings[symbol]['total_cost'] -= transaction.quantity * transaction.price
        
        # 过滤掉数量为0的持仓
        account_holdings = {k: v for k, v in account_holdings.items() if v['quantity'] > 0}
        
        holdings_by_account.append({
            'account': account,
            'holdings': account_holdings
        })
        
        # 汇总到总持仓
        for symbol, holding in account_holdings.items():
            if symbol not in holdings_by_stock:
                holdings_by_stock[symbol] = {
                    'quantity': 0,
                    'total_cost': 0,
                    'stock': holding['stock'],
                    'accounts': []
                }
            
            holdings_by_stock[symbol]['quantity'] += holding['quantity']
            holdings_by_stock[symbol]['total_cost'] += holding['total_cost']
            holdings_by_stock[symbol]['accounts'].append(account.name)
            
            # 按分类汇总
            if holding['stock'] and holding['stock'].category:
                category_name = holding['stock'].category.name
                if category_name not in holdings_by_category:
                    holdings_by_category[category_name] = {
                        'total_cost': 0,
                        'stocks': []
                    }
                holdings_by_category[category_name]['total_cost'] += holding['total_cost']
                if symbol not in holdings_by_category[category_name]['stocks']:
                    holdings_by_category[category_name]['stocks'].append(symbol)
    
    return render_template('investment/holdings_analysis.html',
                         title=_('Holdings Analysis'),
                         holdings_by_stock=holdings_by_stock,
                         holdings_by_category=holdings_by_category,
                         holdings_by_account=holdings_by_account)


@bp.route('/transactions/delete-all', methods=['POST'])
def delete_all_transactions():
    """删除所有交易记录"""
    try:
        from app.models.transaction import Transaction
        # from app.models.holding import CurrentHolding  # CurrentHolding model deleted
        from app import db
        
        # 删除所有持仓记录 - temporarily disabled
        # CurrentHolding.query.delete()
        
        # 删除所有交易记录
        deleted_count = Transaction.query.delete()
        
        db.session.commit()
        
        return jsonify({
            'success': True, 
            'message': f'Successfully deleted {deleted_count} transactions',
            'deleted_count': deleted_count
        })
        
    except Exception as e:
        db.session.rollback()
        return jsonify({
            'success': False, 
            'error': str(e)
        }), 500


@bp.route('/transactions/export')
def export_transactions():
    """导出交易记录为CSV文件"""
    try:
        from app.models.transaction import Transaction
        import csv
        import io
        from flask import Response
        from datetime import datetime
        
        # 获取账户ID参数
        account_id = request.args.get('account_id', type=int)
        
        # 构建查询
        query = Transaction.query
        if account_id:
            query = query.filter_by(account_id=account_id)
        
        transactions = query.order_by(Transaction.trade_date.desc()).all()
        
        # 创建CSV内容
        output = io.StringIO()
        writer = csv.writer(output)
        
        # 写入标题行
        writer.writerow([
            'Date', 'Type', 'Stock Symbol', 'Stock Name', 'Quantity', 
            'Price Per Share', 'Transaction Fee', 'Currency', 'Account', 
            'Member', 'Exchange Rate', 'Notes'
        ])
        
        # 写入数据行
        for txn in transactions:
            writer.writerow([
                txn.trade_date.strftime('%Y-%m-%d'),
                txn.type,
                txn.stock if txn.stock else '',
                '',  # 股票名称暂时不可用，需要从StocksCache查询
                float(txn.quantity),
                float(txn.price),
                float(txn.fee) if txn.fee else 0,
                txn.account.currency if txn.account else '',
                txn.account.name if txn.account else '',
                '',  # member信息暂时不可用
                '',  # exchange_rate字段已移除
                txn.notes or ''
            ])
        
        # 创建响应
        output.seek(0)
        filename = f"transactions_export_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
        
        return Response(
            output.getvalue(),
            mimetype='text/csv',
            headers={'Content-Disposition': f'attachment; filename={filename}'}
        )
        
    except Exception as e:
        from flask import flash, redirect, url_for
        flash(f'导出失败: {str(e)}', 'error')
        return redirect(url_for('main.transactions'))


@bp.route('/api/v1/transactions/<int:transaction_id>', methods=['DELETE'])
def delete_transaction(transaction_id):
    """删除单个交易记录"""
    try:
        # 查找交易记录
        transaction = Transaction.query.get_or_404(transaction_id)
        
        # 删除交易
        db.session.delete(transaction)
        db.session.commit()
        
        return jsonify({
            'success': True,
            'message': '交易记录删除成功'
        })
        
    except Exception as e:
        db.session.rollback()
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500

@bp.route('/api/v1/transactions/<int:transaction_id>', methods=['GET'])
def get_transaction(transaction_id):
    """获取单个交易记录"""
    try:
        transaction = Transaction.query.get_or_404(transaction_id)
        return jsonify({
            'success': True,
            'transaction': transaction.to_dict()
        })
    except Exception as e:
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500

@bp.route('/api/v1/transactions/<int:transaction_id>', methods=['PUT'])
def update_transaction(transaction_id):
    """更新单个交易记录"""
    try:
        transaction = Transaction.query.get_or_404(transaction_id)
        data = request.get_json()
        
        # 更新字段
        if 'trade_date' in data:
            from datetime import datetime
            transaction.trade_date = datetime.strptime(data['trade_date'], '%Y-%m-%d').date()
        if 'type' in data:
            transaction.type = data['type']
        if 'stock' in data:
            transaction.stock = data['stock']
        if 'quantity' in data:
            transaction.quantity = float(data['quantity']) if data['quantity'] else 0
        if 'price' in data:
            transaction.price = float(data['price']) if data['price'] else 0
        if 'currency' in data:
            transaction.currency = data['currency']
        if 'fee' in data:
            transaction.fee = float(data['fee']) if data['fee'] else 0
        if 'account_id' in data:
            transaction.account_id = data['account_id']
        if 'notes' in data:
            transaction.notes = data['notes']
        if 'amount' in data:
            transaction.amount = float(data['amount']) if data['amount'] else None
        
        db.session.commit()
        
        return jsonify({
            'success': True,
            'message': '交易记录更新成功',
            'transaction': transaction.to_dict()
        })
        
    except Exception as e:
        db.session.rollback()
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500

@bp.route('/api/accounts/<int:account_id>/transactions', methods=['DELETE'])
def delete_account_transactions(account_id):
    """删除指定账户的所有交易记录"""
    try:
        # 验证账户是否存在
        account = Account.query.get_or_404(account_id)
        
        # 删除该账户的所有交易记录
        deleted_count = Transaction.query.filter_by(account_id=account_id).delete()
        
        db.session.commit()
        
        return jsonify({
            'success': True,
            'message': f'Successfully deleted {deleted_count} transactions from account "{account.name}"',
            'deleted_count': deleted_count
        })
        
    except Exception as e:
        db.session.rollback()
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500


@bp.route('/api/import-csv', methods=['POST'])
def import_csv_api():
    """CSV导入API"""
    try:
        if 'file' not in request.files:
            return jsonify({'success': False, 'error': 'No file uploaded'}), 400
            
        file = request.files['file']
        if file.filename == '':
            return jsonify({'success': False, 'error': 'No file selected'}), 400
            
        if not file.filename.lower().endswith('.csv'):
            return jsonify({'success': False, 'error': 'Please upload a CSV file'}), 400
            
        account_id = request.form.get('account_id')
        if not account_id:
            return jsonify({'success': False, 'error': 'Account ID is required'}), 400
            
        # 验证账户是否存在
        account = Account.query.get(account_id)
        if not account:
            return jsonify({'success': False, 'error': 'Account not found'}), 404
            
        # 处理CSV文件
        import csv
        import io
        from datetime import datetime
        
        # 读取CSV内容
        stream = io.StringIO(file.stream.read().decode("UTF8"), newline=None)
        csv_reader = csv.DictReader(stream)
        
        imported_count = 0
        errors = []
        
        for row_num, row in enumerate(csv_reader, start=2):
            try:
                # 解析必要字段
                trade_date = datetime.strptime(row.get('Date', ''), '%Y-%m-%d').date()
                transaction_type = row.get('Type', '').upper()
                stock_symbol = row.get('Stock Symbol', '').strip().upper()
                quantity = float(row.get('Quantity', 0))
                price = float(row.get('Price per Share', 0))
                currency = row.get('Currency', 'CAD').upper()
                fee = float(row.get('Transaction Fee', 0) or 0)
                notes = row.get('Notes', '').strip()
                
                # 验证必要字段
                if not all([trade_date, transaction_type, stock_symbol, quantity > 0, price > 0]):
                    errors.append(f'Row {row_num}: Missing required fields')
                    continue
                    
                if transaction_type not in ['BUY', 'SELL']:
                    errors.append(f'Row {row_num}: Invalid transaction type (must be BUY or SELL)')
                    continue
                    
                # 查找或创建股票缓存记录
                stock_cache = StocksCache.query.filter_by(symbol=stock_symbol).first()
                if not stock_cache:
                    # 创建新的股票缓存记录
                    stock_cache = StocksCache(
                        symbol=stock_symbol,
                        name=stock_symbol,  # 使用符号作为默认名称
                        exchange='TSX' if currency == 'CAD' else 'NYSE'
                    )
                    db.session.add(stock_cache)
                    db.session.flush()
                
                # 创建交易记录 - 使用统一函数
                transaction = save_transaction_record(
                    account_id=account_id,
                    transaction_type=transaction_type,
                    quantity=quantity,
                    price=price,
                    currency=currency,
                    stock=stock_symbol,
                    fee=fee,
                    trade_date=trade_date,
                    notes=notes
                )
                imported_count += 1
                
            except Exception as e:
                errors.append(f'Row {row_num}: {str(e)}')
        
        db.session.commit()
        
        result = {
            'success': True,
            'imported_count': imported_count,
            'message': f'Successfully imported {imported_count} transactions'
        }
        
        if errors:
            result['errors'] = errors
            result['message'] += f' ({len(errors)} errors)'
            
        return jsonify(result)
        
    except Exception as e:
        db.session.rollback()
        return jsonify({
            'success': False,
            'error': f'Import failed: {str(e)}'
        }), 500