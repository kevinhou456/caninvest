"""
主要路由 - 页面视图
"""

from flask import render_template, request, jsonify, redirect, url_for, flash, session
from flask_babel import _
from app.main import bp
from app import db
from app.models.family import Family
from app.models.member import Member
from app.models.account import Account, AccountType, AccountMember
from app.models.transaction import Transaction
from app.models.stock import Stock, StockCategory
from app.models.import_task import ImportTask, OCRTask


@bp.route('/')
@bp.route('/index')
def index():
    """首页 - 直接重定向到仪表板"""
    return redirect(url_for('main.dashboard'))


@bp.route('/dashboard')
def dashboard():
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
    
    # 根据选择过滤账户
    if account_id:
        # 选择了特定账户
        accounts = Account.query.filter_by(id=account_id, family_id=family.id).all()
        filter_description = f"账户: {accounts[0].name}" if accounts else "未找到账户"
    elif member_id:
        # 选择了特定成员
        from app.models.account import AccountMember
        member_accounts = AccountMember.query.filter_by(member_id=member_id).all()
        account_ids = [am.account_id for am in member_accounts]
        accounts = Account.query.filter(Account.id.in_(account_ids), Account.family_id == family.id).all()
        
        from app.models.member import Member
        member = Member.query.get(member_id)
        filter_description = f"成员: {member.name}" if member else "未找到成员"
    else:
        # 显示所有成员数据
        accounts = Account.query.filter_by(family_id=family.id).all()
        filter_description = "全部成员"
    
    # 获取基本统计数据
    from app.models.member import Member
    members_count = Member.query.filter_by(family_id=family.id).count()
    accounts_count = len(accounts)
    
    # 计算交易数量（基于过滤的账户）
    if accounts:
        account_ids = [acc.id for acc in accounts]
        transactions_count = Transaction.query.filter(Transaction.account_id.in_(account_ids)).count()
    else:
        transactions_count = 0
    
    # 计算投资组合统计
    portfolio_stats = {
        'total_value': 0,
        'total_cost': 0,
        'total_gain': 0,
        'total_gain_percent': 0
    }
    
    account_balances = []
    for account in accounts:
        current_value = float(account.current_value or 0)
        total_cost = float(account.total_cost or 0)
        gain_loss = current_value - total_cost
        gain_loss_percent = (gain_loss / total_cost * 100) if total_cost > 0 else 0
        
        portfolio_stats['total_value'] += current_value
        portfolio_stats['total_cost'] += total_cost
        portfolio_stats['total_gain'] += gain_loss
        
        account_balances.append({
            'id': account.id,
            'name': account.name,
            'account_type': account.account_type,
            'current_value': current_value,
            'gain_loss': gain_loss,
            'gain_loss_percent': gain_loss_percent
        })
    
    # 计算总收益率
    if portfolio_stats['total_cost'] > 0:
        portfolio_stats['total_gain_percent'] = (portfolio_stats['total_gain'] / portfolio_stats['total_cost']) * 100
    
    # 获取最近的交易（基于过滤的账户）
    if accounts:
        account_ids = [acc.id for acc in accounts]
        recent_transactions = Transaction.query.filter(
            Transaction.account_id.in_(account_ids)
        ).order_by(Transaction.transaction_date.desc()).limit(10).all()
    else:
        recent_transactions = []
    
    # 投资组合分配数据（简化版）
    portfolio_allocation = None
    if accounts:
        labels = [account.name for account in accounts]
        data = [float(account.current_value or 0) for account in accounts]
        if any(value > 0 for value in data):
            portfolio_allocation = {'labels': labels, 'data': data}
    
    stats = {
        'members_count': members_count,
        'accounts_count': accounts_count,
        'transactions_count': transactions_count
    }
    
    return render_template('investment/dashboard.html', 
                         title=_('Dashboard'),
                         stats=stats,
                         portfolio_stats=portfolio_stats,
                         account_balances=account_balances,
                         portfolio_allocation=portfolio_allocation,
                         recent_transactions=recent_transactions,
                         filter_description=filter_description,
                         filtered_accounts_count=len(accounts))


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
    page = request.args.get('page', 1, type=int)
    account_id = request.args.get('account_id', type=int)
    transaction_type = request.args.get('type')
    
    query = Transaction.query
    if account_id:
        query = query.filter_by(account_id=account_id)
    if transaction_type:
        query = query.filter_by(transaction_type=transaction_type)
    
    transactions = query.order_by(Transaction.transaction_date.desc()).paginate(
        page=page, per_page=50, error_out=False
    )
    
    accounts = Account.query.all()
    
    return render_template('transactions/list.html',
                         title=_('Transactions'),
                         transactions=transactions,
                         accounts=accounts,
                         current_view='transactions')


@bp.route('/transactions/create')
def create_transaction():
    """创建交易记录页面"""
    accounts = Account.query.all()
    stocks = Stock.query.filter_by(is_active=True).all()
    
    return render_template('transactions/create.html',
                         title=_('Create Transaction'),
                         accounts=accounts,
                         stocks=stocks)


@bp.route('/stocks')
def stocks():
    """股票和分类管理页面"""
    page = request.args.get('page', 1, type=int)
    category_id = request.args.get('category_id', type=int)
    search = request.args.get('search', '')
    
    query = Stock.query
    if category_id:
        query = query.filter_by(category_id=category_id)
    if search:
        query = query.filter(Stock.symbol.contains(search) | Stock.name.contains(search))
    
    stocks = query.order_by(Stock.symbol).paginate(
        page=page, per_page=50, error_out=False
    )
    
    categories = StockCategory.query.all()
    
    return render_template('stocks/list.html',
                         title=_('Stocks & Categories'),
                         stocks=stocks,
                         categories=categories,
                         search=search)


@bp.route('/stocks/<symbol>')
def stock_detail(symbol):
    """股票详情页面"""
    stock = Stock.query.filter_by(symbol=symbol).first_or_404()
    return render_template('stocks/detail.html',
                         title=f"{stock.symbol} - {stock.name}",
                         stock=stock)


@bp.route('/categories')
def categories():
    """股票分类管理页面"""
    categories = StockCategory.query.order_by(StockCategory.sort_order).all()
    return render_template('categories/list.html',
                         title=_('Stock Categories'),
                         categories=categories)


@bp.route('/import-transactions')
def import_transactions():
    """数据导入页面"""
    accounts = Account.query.all()
    
    # 获取最近的导入任务
    recent_imports = ImportTask.query.order_by(ImportTask.created_at.desc()).limit(10).all()
    recent_ocr = OCRTask.query.order_by(OCRTask.created_at.desc()).limit(10).all()
    
    return render_template('imports/index.html',
                         title=_('Import Data'),
                         accounts=accounts,
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
        
        # 检查账户类型变更的合法性
        if account.is_joint and new_account_type.name in ['TFSA', 'RRSP', 'RESP', 'FHSA']:
            flash(_('Joint accounts cannot be changed to tax-advantaged account types (TFSA, RRSP, RESP, FHSA). These account types can only have single owners.'), 'error')
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
    """股票分类 - 占位符"""
    return "<h1>Stock Categories</h1><p>This feature is under development.</p>"

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
            extract('year', Transaction.transaction_date) == year
        ).all()
        
        buy_amount = sum(t.quantity * t.price_per_share for t in year_transactions if t.transaction_type == 'buy')
        sell_amount = sum(t.quantity * t.price_per_share for t in year_transactions if t.transaction_type == 'sell')
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
            extract('year', Transaction.transaction_date) == year,
            extract('month', Transaction.transaction_date) == month
        ).all()
        
        buy_amount = sum(t.quantity * t.price_per_share for t in month_transactions if t.transaction_type == 'buy')
        sell_amount = sum(t.quantity * t.price_per_share for t in month_transactions if t.transaction_type == 'sell')
        
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
                extract('year', Transaction.transaction_date) == year,
                extract('month', Transaction.transaction_date) >= start_month,
                extract('month', Transaction.transaction_date) <= end_month
            ).all()
            
            buy_amount = sum(t.quantity * t.price_per_share for t in quarter_transactions if t.transaction_type == 'buy')
            sell_amount = sum(t.quantity * t.price_per_share for t in quarter_transactions if t.transaction_type == 'sell')
            
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
            func.date(Transaction.transaction_date) == current_date
        ).all()
        
        buy_amount = sum(t.quantity * t.price_per_share for t in day_transactions if t.transaction_type == 'buy')
        sell_amount = sum(t.quantity * t.price_per_share for t in day_transactions if t.transaction_type == 'sell')
        
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
            symbol = transaction.stock.symbol if transaction.stock else 'CASH'
            if symbol not in account_holdings:
                account_holdings[symbol] = {
                    'quantity': 0,
                    'total_cost': 0,
                    'stock': transaction.stock
                }
            
            if transaction.transaction_type == 'buy':
                account_holdings[symbol]['quantity'] += transaction.quantity
                account_holdings[symbol]['total_cost'] += transaction.quantity * transaction.price_per_share
            elif transaction.transaction_type == 'sell':
                account_holdings[symbol]['quantity'] -= transaction.quantity
                account_holdings[symbol]['total_cost'] -= transaction.quantity * transaction.price_per_share
        
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