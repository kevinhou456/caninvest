"""
交易记录API
"""

from flask import request, jsonify
from flask_babel import _
from datetime import datetime
from app import db
from app.models.transaction import Transaction
from app.models.account import Account
# from app.models.stock import Stock  # Stock model deleted - using StocksCache instead
from app.models.stocks_cache import StocksCache
from app.models.member import Member
# from app.models.holding import CurrentHolding  # CurrentHolding model deleted
from . import bp

@bp.route('/accounts/<int:account_id>/transactions', methods=['GET'])
def get_account_transactions(account_id):
    """获取账户交易记录"""
    account = Account.query.get_or_404(account_id)
    
    # 获取查询参数
    page = request.args.get('page', 1, type=int)
    per_page = request.args.get('per_page', 50, type=int)
    start_date = request.args.get('start_date')
    end_date = request.args.get('end_date')
    transaction_type = request.args.get('type')
    stock_symbol = request.args.get('stock_symbol')
    
    # 构建查询
    query = Transaction.query.filter_by(account_id=account_id)
    
    if start_date:
        query = query.filter(Transaction.trade_date >= datetime.strptime(start_date, '%Y-%m-%d').date())
    if end_date:
        query = query.filter(Transaction.trade_date <= datetime.strptime(end_date, '%Y-%m-%d').date())
    if transaction_type and transaction_type in ['BUY', 'SELL']:
        query = query.filter_by(transaction_type=transaction_type)
    # TODO: Re-implement stock symbol filtering with new StocksCache model
    # if stock_symbol:
    #     query = query.join(Stock).filter(Stock.symbol.ilike(f'%{stock_symbol}%'))
    
    # 分页查询
    transactions = query.order_by(Transaction.trade_date.desc()).paginate(
        page=page, per_page=per_page, error_out=False
    )
    
    return jsonify({
        'transactions': [txn.to_dict() for txn in transactions.items],
        'pagination': {
            'page': transactions.page,
            'pages': transactions.pages,
            'per_page': transactions.per_page,
            'total': transactions.total,
            'has_next': transactions.has_next,
            'has_prev': transactions.has_prev
        }
    })

@bp.route('/accounts/<int:account_id>/transactions', methods=['POST'])
def create_transaction(account_id):
    """创建交易记录"""
    account = Account.query.get_or_404(account_id)
    data = request.get_json()
    
    if not data:
        return jsonify({'error': _('No data provided')}), 400
    
    # 验证必需字段
    required_fields = ['symbol', 'transaction_type', 'quantity', 'price_per_share', 'trade_date']
    for field in required_fields:
        if field not in data or data[field] is None:
            return jsonify({'error': _(f'{field} is required')}), 400
    
    # 验证交易类型
    if data['transaction_type'] not in ['BUY', 'SELL']:
        return jsonify({'error': _('Invalid transaction type')}), 400
    
    # 验证数值
    try:
        quantity = float(data['quantity'])
        price_per_share = float(data['price_per_share'])
        transaction_fee = float(data.get('transaction_fee', 0))
        
        if quantity <= 0 or price_per_share <= 0:
            return jsonify({'error': _('Quantity and price must be positive')}), 400
    except (ValueError, TypeError):
        return jsonify({'error': _('Invalid numeric values')}), 400
    
    # 解析日期
    try:
        trade_date = datetime.strptime(data['trade_date'], '%Y-%m-%d').date()
    except ValueError:
        return jsonify({'error': _('Invalid date format')}), 400
    
    # TODO: Re-implement stock creation with new StocksCache model
    # For now, we'll create a placeholder stock_id or disable transaction creation
    # stock = Stock.get_or_create(
    #     symbol=data['symbol'].upper(),
    #     name=data.get('stock_name'),
    #     exchange=data.get('exchange'),
    #     currency=account.currency
    # )
    # Temporarily disabled - need to implement stock lookup with StocksCache
    return jsonify({'error': _('Transaction creation temporarily disabled during system redesign')}), 503
    
    # 验证成员（如果提供）
    member_id = data.get('member_id')
    if member_id:
        member = Member.query.filter_by(id=member_id, family_id=account.family_id).first()
        if not member:
            return jsonify({'error': _('Invalid member')}), 400
    
    # TODO: Re-implement holding validation with new system
    # if data['transaction_type'] == 'SELL':
    #     holding = CurrentHolding.query.filter_by(account_id=account_id, stock_id=stock.id).first()
    #     if not holding or holding.total_shares < quantity:
    #         return jsonify({'error': _('Insufficient shares to sell')}), 400
    
    # 创建交易记录
    transaction = Transaction(
        account_id=account_id,
        stock_id=stock.id,
        member_id=member_id,
        transaction_type=data['transaction_type'],
        quantity=quantity,
        price_per_share=price_per_share,
        transaction_fee=transaction_fee,
        trade_date=trade_date,
        exchange_rate=data.get('exchange_rate'),
        notes=data.get('notes')
    )
    
    db.session.add(transaction)
    db.session.flush()  # 获取交易ID
    
    # TODO: Re-implement holdings update with new system
    # transaction.update_holdings()
    
    db.session.commit()
    
    return jsonify({
        'message': _('Transaction created successfully'),
        'transaction': transaction.to_dict()
    }), 201

@bp.route('/transactions/<int:transaction_id>', methods=['GET'])
def get_transaction(transaction_id):
    """获取交易详情"""
    transaction = Transaction.query.get_or_404(transaction_id)
    return jsonify({
        'success': True,
        'transaction': transaction.to_dict()
    })

@bp.route('/transactions/<int:transaction_id>', methods=['PUT'])
def update_transaction(transaction_id):
    """更新交易记录"""
    transaction = Transaction.query.get_or_404(transaction_id)
    data = request.get_json()
    
    if not data:
        return jsonify({'success': False, 'error': _('No data provided')}), 400
    
    try:
        # 更新字段
        if 'trade_date' in data:
            transaction.trade_date = datetime.strptime(data['trade_date'], '%Y-%m-%d').date()
        
        if 'type' in data:
            if data['type'] not in ['BUY', 'SELL']:
                return jsonify({'success': False, 'error': _('Invalid transaction type')}), 400
            transaction.type = data['type']
        
        if 'stock' in data:
            transaction.stock = data['stock'].upper()
        
        if 'quantity' in data:
            quantity = float(data['quantity'])
            if quantity <= 0:
                return jsonify({'success': False, 'error': _('Quantity must be positive')}), 400
            transaction.quantity = quantity
        
        if 'price' in data:
            price = float(data['price'])
            if price <= 0:
                return jsonify({'success': False, 'error': _('Price must be positive')}), 400
            transaction.price = price
        
        if 'currency' in data:
            if data['currency'] not in ['USD', 'CAD']:
                return jsonify({'success': False, 'error': _('Invalid currency')}), 400
            transaction.currency = data['currency']
        
        if 'fee' in data:
            transaction.fee = float(data['fee']) if data['fee'] else 0
        
        if 'account_id' in data:
            account = Account.query.get(data['account_id'])
            if not account:
                return jsonify({'success': False, 'error': _('Invalid account')}), 400
            transaction.account_id = data['account_id']
        
        if 'notes' in data:
            transaction.notes = data['notes']
        
        transaction.updated_at = datetime.utcnow()
        
        db.session.commit()
        
        return jsonify({
            'success': True,
            'message': _('Transaction updated successfully'),
            'transaction': transaction.to_dict()
        })
        
    except ValueError as e:
        return jsonify({'success': False, 'error': _('Invalid numeric values')}), 400
    except Exception as e:
        db.session.rollback()
        return jsonify({'success': False, 'error': str(e)}), 500

@bp.route('/transactions/<int:transaction_id>', methods=['DELETE'])
def delete_transaction(transaction_id):
    """删除交易记录"""
    transaction = Transaction.query.get_or_404(transaction_id)
    
    try:
        db.session.delete(transaction)
        db.session.commit()
        
        return jsonify({
            'success': True,
            'message': _('Transaction deleted successfully')
        })
        
    except Exception as e:
        db.session.rollback()
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500

@bp.route('/members/<int:member_id>/transactions', methods=['GET'])
def get_member_transactions(member_id):
    """获取成员的所有交易记录"""
    member = Member.query.get_or_404(member_id)
    
    # 获取查询参数
    page = request.args.get('page', 1, type=int)
    per_page = request.args.get('per_page', 50, type=int)
    start_date = request.args.get('start_date')
    end_date = request.args.get('end_date')
    
    # 构建查询
    query = Transaction.query.filter_by(member_id=member_id)
    
    if start_date:
        query = query.filter(Transaction.trade_date >= datetime.strptime(start_date, '%Y-%m-%d').date())
    if end_date:
        query = query.filter(Transaction.trade_date <= datetime.strptime(end_date, '%Y-%m-%d').date())
    
    # 分页查询
    transactions = query.order_by(Transaction.trade_date.desc()).paginate(
        page=page, per_page=per_page, error_out=False
    )
    
    return jsonify({
        'member': member.to_dict(),
        'transactions': [txn.to_dict() for txn in transactions.items],
        'pagination': {
            'page': transactions.page,
            'pages': transactions.pages,
            'per_page': transactions.per_page,
            'total': transactions.total
        }
    })

@bp.route('/transactions/batch-delete', methods=['POST'])
def batch_delete_transactions():
    """批量删除交易记录"""
    data = request.get_json()
    
    if not data or 'transaction_ids' not in data:
        return jsonify({'error': _('Transaction IDs required')}), 400
    
    transaction_ids = data['transaction_ids']
    if not isinstance(transaction_ids, list):
        return jsonify({'error': _('Invalid transaction IDs format')}), 400
    
    # 获取交易记录并分组（按账户和股票）
    transactions = Transaction.query.filter(Transaction.id.in_(transaction_ids)).all()
    
    if not transactions:
        return jsonify({'error': _('No transactions found')}), 404
    
    # 收集需要重新计算持仓的账户股票组合
    holdings_to_recalculate = set()
    
    for transaction in transactions:
        holdings_to_recalculate.add((transaction.account_id, transaction.stock_id))
        db.session.delete(transaction)
    
    # TODO: Re-implement holdings recalculation with new system
    # for account_id, stock_id in holdings_to_recalculate:
    #     CurrentHolding.recalculate_holding(account_id, stock_id)
    
    db.session.commit()
    
    return jsonify({
        'message': _('Transactions deleted successfully'),
        'deleted_count': len(transactions)
    })