"""
账户管理API
"""

from flask import request, jsonify
from flask_babel import _
from app import db
from app.models.account import Account, AccountType, AccountMember
from app.models.family import Family
from app.models.member import Member
from . import bp

@bp.route('/account-types', methods=['GET'])
def get_account_types():
    """获取所有账户类型"""
    account_types = AccountType.query.filter_by(is_active=True).all()
    return jsonify({
        'account_types': [at.to_dict() for at in account_types]
    })

@bp.route('/families/<int:family_id>/accounts', methods=['GET'])
def get_family_accounts(family_id):
    """获取家庭账户"""
    family = Family.query.get_or_404(family_id)
    accounts = family.accounts.all()
    
    return jsonify({
        'accounts': [account.to_dict(include_summary=True) for account in accounts]
    })

@bp.route('/accounts', methods=['POST'])
def create_account():
    """创建账户 - 自动分配给默认家庭"""
    data = request.get_json()
    
    # 获取或创建默认家庭
    family = Family.query.first()
    if not family:
        family = Family(name="我的家庭")
        db.session.add(family)
        db.session.commit()
    
    family_id = family.id
    
    if not data or not data.get('name'):
        return jsonify({'error': _('Account name is required')}), 400
    
    # 验证账户类型
    account_type_id = data.get('account_type_id')
    if account_type_id:
        account_type = AccountType.query.get(account_type_id)
        if not account_type:
            return jsonify({'error': _('Invalid account type')}), 400
    
    account = Account(
        name=data['name'],
        family_id=family_id,
        account_type_id=account_type_id,
        is_joint=data.get('is_joint', False),
        currency=data.get('currency', 'CAD'),
        account_number=data.get('account_number'),
        broker_name=data.get('broker_name')
    )
    
    db.session.add(account)
    db.session.commit()
    
    return jsonify(account.to_dict()), 201

@bp.route('/families/<int:family_id>/accounts', methods=['POST'])
def create_family_account(family_id):
    """为指定家庭创建账户"""
    family = Family.query.get_or_404(family_id)
    data = request.get_json()
    
    if not data or not data.get('name'):
        return jsonify({'error': _('Account name is required')}), 400
    
    # 验证账户类型
    account_type_id = data.get('account_type_id')
    if account_type_id:
        account_type = AccountType.query.get(account_type_id)
        if not account_type:
            return jsonify({'error': _('Invalid account type')}), 400
    
    account = Account(
        name=data['name'],
        family_id=family_id,
        account_type_id=account_type_id,
        is_joint=data.get('is_joint', False),
        currency=data.get('currency', 'CAD'),
        account_number=data.get('account_number'),
        broker_name=data.get('broker_name')
    )
    
    db.session.add(account)
    db.session.flush()  # 获取账户ID
    
    # 添加账户成员
    members_data = data.get('members', [])
    if not members_data:
        return jsonify({'error': _('At least one member is required')}), 400
    
    total_percentage = 0
    for member_data in members_data:
        member_id = member_data.get('member_id')
        ownership_percentage = member_data.get('ownership_percentage', 100.0)
        is_primary = member_data.get('is_primary', False)
        
        # 验证成员
        member = Member.query.filter_by(id=member_id, family_id=family_id).first()
        if not member:
            return jsonify({'error': f'Invalid member ID: {member_id}'}), 400
        
        # 创建账户成员关系
        account_member = AccountMember(
            account_id=account.id,
            member_id=member_id,
            ownership_percentage=ownership_percentage,
            is_primary=is_primary
        )
        db.session.add(account_member)
        total_percentage += ownership_percentage
    
    # 验证出资比例总和
    if abs(total_percentage - 100.0) > 0.01:
        return jsonify({'error': _('Ownership percentages must sum to 100%')}), 400
    
    db.session.commit()
    
    return jsonify({
        'message': _('Account created successfully'),
        'account': account.to_dict(include_summary=True)
    }), 201

@bp.route('/accounts/<int:account_id>', methods=['GET'])
def get_account(account_id):
    """获取账户详情"""
    account = Account.query.get_or_404(account_id)
    
    # 获取持仓摘要
    holdings_summary = account.get_holdings_summary()
    
    # 获取最近交易
    from app.models.transaction import Transaction
    recent_transactions = Transaction.get_transactions_by_account(account_id, limit=20)
    
    result = account.to_dict(include_summary=True)
    result['holdings_summary'] = holdings_summary
    result['recent_transactions'] = [txn.to_dict() for txn in recent_transactions]
    
    return jsonify(result)

@bp.route('/accounts/<int:account_id>', methods=['PUT'])
def update_account(account_id):
    """更新账户"""
    account = Account.query.get_or_404(account_id)
    data = request.get_json()
    
    if not data:
        return jsonify({'error': _('No data provided')}), 400
    
    # 更新基本信息
    if 'name' in data:
        account.name = data['name']
    if 'account_type_id' in data:
        if data['account_type_id']:
            account_type = AccountType.query.get(data['account_type_id'])
            if not account_type:
                return jsonify({'error': _('Invalid account type')}), 400
            account.account_type_id = data['account_type_id']
        else:
            account.account_type_id = None
    if 'account_number' in data:
        account.account_number = data['account_number']
    if 'broker_name' in data:
        account.broker_name = data['broker_name']
    
    # 更新成员关系（如果提供）
    if 'members' in data:
        # 删除现有关系
        AccountMember.query.filter_by(account_id=account_id).delete()
        
        # 添加新关系
        total_percentage = 0
        for member_data in data['members']:
            member_id = member_data.get('member_id')
            ownership_percentage = member_data.get('ownership_percentage', 100.0)
            is_primary = member_data.get('is_primary', False)
            
            # 验证成员属于同一家庭
            member = Member.query.filter_by(id=member_id, family_id=account.family_id).first()
            if not member:
                return jsonify({'error': f'Invalid member ID: {member_id}'}), 400
            
            account_member = AccountMember(
                account_id=account_id,
                member_id=member_id,
                ownership_percentage=ownership_percentage,
                is_primary=is_primary
            )
            db.session.add(account_member)
            total_percentage += ownership_percentage
        
        # 验证出资比例总和
        if abs(total_percentage - 100.0) > 0.01:
            return jsonify({'error': _('Ownership percentages must sum to 100%')}), 400
    
    db.session.commit()
    
    return jsonify({
        'message': _('Account updated successfully'),
        'account': account.to_dict(include_summary=True)
    })

@bp.route('/accounts/<int:account_id>', methods=['DELETE'])
def delete_account(account_id):
    """删除账户"""
    account = Account.query.get_or_404(account_id)
    
    # 检查是否有交易记录
    if account.transactions.count() > 0:
        return jsonify({
            'error': _('Cannot delete account with existing transactions')
        }), 400
    
    db.session.delete(account)
    db.session.commit()
    
    return jsonify({
        'message': _('Account deleted successfully')
    })

@bp.route('/accounts/<int:account_id>/holdings', methods=['GET'])
def get_account_holdings(account_id):
    """获取账户持仓"""
    account = Account.query.get_or_404(account_id)
    
    # from app.models.holding import CurrentHolding  # CurrentHolding model deleted
    # holdings = CurrentHolding.get_holdings_by_account(account_id)  # Temporarily disabled
    holdings = []  # TODO: Re-implement with new holding system
    
    return jsonify({
        'account': account.to_dict(),
        'holdings': [holding.to_dict() for holding in holdings],
        'summary': account.get_holdings_summary()
    })

@bp.route('/accounts/<int:account_id>/performance', methods=['GET'])
def get_account_performance(account_id):
    """获取账户表现"""
    account = Account.query.get_or_404(account_id)
    
    # 获取时间范围参数
    start_date = request.args.get('start_date')
    end_date = request.args.get('end_date')
    period = request.args.get('period', 'daily')  # daily, weekly, monthly
    
    from datetime import datetime, timedelta
    
    if not start_date:
        # 默认显示最近3个月
        start_date = (datetime.now() - timedelta(days=90)).date()
    else:
        start_date = datetime.strptime(start_date, '%Y-%m-%d').date()
    
    if not end_date:
        end_date = datetime.now().date()
    else:
        end_date = datetime.strptime(end_date, '%Y-%m-%d').date()
    
    # 这里应该实现历史价值计算逻辑
    # 简化版本：返回当前数据
    performance_data = {
        'account': account.to_dict(),
        'current_value': float(account.current_value or 0),
        'total_cost': float(account.total_cost or 0),
        'unrealized_gain': float(account.unrealized_gain or 0),
        'unrealized_gain_percent': account.unrealized_gain_percent or 0,
        'realized_gain': float(account.realized_gain or 0),
        'historical_data': {
            'dates': [],
            'values': [],
            'returns': []
        }
    }
    
    return jsonify(performance_data)