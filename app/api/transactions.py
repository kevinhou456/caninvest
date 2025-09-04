"""
交易记录API
"""

from flask import request, jsonify
from flask_babel import _
from datetime import datetime
from app import db
from app.models.transaction import Transaction
from app.models.account import Account
from app.models.stock import Stock
from app.models.member import Member
from app.models.holding import CurrentHolding
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
        query = query.filter(Transaction.transaction_date >= datetime.strptime(start_date, '%Y-%m-%d').date())
    if end_date:
        query = query.filter(Transaction.transaction_date <= datetime.strptime(end_date, '%Y-%m-%d').date())
    if transaction_type and transaction_type in ['BUY', 'SELL']:
        query = query.filter_by(transaction_type=transaction_type)
    if stock_symbol:
        query = query.join(Stock).filter(Stock.symbol.ilike(f'%{stock_symbol}%'))
    
    # 分页查询
    transactions = query.order_by(Transaction.transaction_date.desc()).paginate(
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
    required_fields = ['symbol', 'transaction_type', 'quantity', 'price_per_share', 'transaction_date']
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
        transaction_date = datetime.strptime(data['transaction_date'], '%Y-%m-%d').date()
    except ValueError:
        return jsonify({'error': _('Invalid date format')}), 400
    
    # 获取或创建股票
    stock = Stock.get_or_create(
        symbol=data['symbol'].upper(),
        name=data.get('stock_name'),
        exchange=data.get('exchange'),
        currency=account.currency
    )
    
    # 验证成员（如果提供）
    member_id = data.get('member_id')
    if member_id:
        member = Member.query.filter_by(id=member_id, family_id=account.family_id).first()
        if not member:
            return jsonify({'error': _('Invalid member')}), 400
    
    # 对于卖出交易，检查是否有足够的持仓
    if data['transaction_type'] == 'SELL':
        holding = CurrentHolding.query.filter_by(account_id=account_id, stock_id=stock.id).first()
        if not holding or holding.total_shares < quantity:
            return jsonify({'error': _('Insufficient shares to sell')}), 400
    
    # 创建交易记录
    transaction = Transaction(
        account_id=account_id,
        stock_id=stock.id,
        member_id=member_id,
        transaction_type=data['transaction_type'],
        quantity=quantity,
        price_per_share=price_per_share,
        transaction_fee=transaction_fee,
        transaction_date=transaction_date,
        exchange_rate=data.get('exchange_rate'),
        notes=data.get('notes')
    )
    
    db.session.add(transaction)
    db.session.flush()  # 获取交易ID
    
    # 更新持仓
    transaction.update_holdings()
    
    db.session.commit()
    
    return jsonify({
        'message': _('Transaction created successfully'),
        'transaction': transaction.to_dict()
    }), 201

@bp.route('/transactions/<int:transaction_id>', methods=['GET'])
def get_transaction(transaction_id):
    """获取交易详情"""
    transaction = Transaction.query.get_or_404(transaction_id)
    return jsonify(transaction.to_dict())

@bp.route('/transactions/<int:transaction_id>', methods=['PUT'])
def update_transaction(transaction_id):
    """更新交易记录"""
    transaction = Transaction.query.get_or_404(transaction_id)
    data = request.get_json()
    
    if not data:
        return jsonify({'error': _('No data provided')}), 400
    
    # 保存原始数据用于回滚持仓
    original_data = {
        'transaction_type': transaction.transaction_type,
        'quantity': transaction.quantity,
        'price_per_share': transaction.price_per_share,
        'transaction_fee': transaction.transaction_fee
    }
    
    # 更新字段
    if 'quantity' in data:
        try:
            quantity = float(data['quantity'])
            if quantity <= 0:
                return jsonify({'error': _('Quantity must be positive')}), 400
            transaction.quantity = quantity
        except (ValueError, TypeError):
            return jsonify({'error': _('Invalid quantity')}), 400
    
    if 'price_per_share' in data:
        try:
            price = float(data['price_per_share'])
            if price <= 0:
                return jsonify({'error': _('Price must be positive')}), 400
            transaction.price_per_share = price
        except (ValueError, TypeError):
            return jsonify({'error': _('Invalid price')}), 400
    
    if 'transaction_fee' in data:
        try:
            transaction.transaction_fee = float(data['transaction_fee'])
        except (ValueError, TypeError):
            return jsonify({'error': _('Invalid transaction fee')}), 400
    
    if 'transaction_date' in data:
        try:
            transaction.transaction_date = datetime.strptime(data['transaction_date'], '%Y-%m-%d').date()
        except ValueError:
            return jsonify({'error': _('Invalid date format')}), 400
    
    if 'notes' in data:
        transaction.notes = data['notes']
    
    if 'exchange_rate' in data:
        transaction.exchange_rate = data.get('exchange_rate')
    
    # 重新计算持仓（如果数量或价格发生变化）
    if any(key in data for key in ['quantity', 'price_per_share', 'transaction_fee']):
        # 重新计算整个股票的持仓
        CurrentHolding.recalculate_holding(transaction.account_id, transaction.stock_id)
    
    db.session.commit()
    
    return jsonify({
        'message': _('Transaction updated successfully'),
        'transaction': transaction.to_dict()
    })

@bp.route('/transactions/<int:transaction_id>', methods=['DELETE'])
def delete_transaction(transaction_id):
    """删除交易记录"""
    transaction = Transaction.query.get_or_404(transaction_id)
    
    account_id = transaction.account_id
    stock_id = transaction.stock_id
    
    db.session.delete(transaction)
    
    # 重新计算持仓
    CurrentHolding.recalculate_holding(account_id, stock_id)
    
    db.session.commit()
    
    return jsonify({
        'message': _('Transaction deleted successfully')
    })

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
        query = query.filter(Transaction.transaction_date >= datetime.strptime(start_date, '%Y-%m-%d').date())
    if end_date:
        query = query.filter(Transaction.transaction_date <= datetime.strptime(end_date, '%Y-%m-%d').date())
    
    # 分页查询
    transactions = query.order_by(Transaction.transaction_date.desc()).paginate(
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
    
    # 重新计算持仓
    for account_id, stock_id in holdings_to_recalculate:
        CurrentHolding.recalculate_holding(account_id, stock_id)
    
    db.session.commit()
    
    return jsonify({
        'message': _('Transactions deleted successfully'),
        'deleted_count': len(transactions)
    })