"""
家庭管理API
"""

from flask import request, jsonify
from flask_babel import _
from app import db
from app.models.family import Family
from app.models.member import Member
from . import bp

@bp.route('/families', methods=['GET'])
def get_families():
    """获取所有家庭"""
    families = Family.query.all()
    return jsonify({
        'families': [family.to_dict() for family in families]
    })

@bp.route('/families', methods=['POST'])
def create_family():
    """创建家庭"""
    data = request.get_json()
    
    if not data or not data.get('name'):
        return jsonify({'error': _('Family name is required')}), 400
    
    family = Family(name=data['name'])
    db.session.add(family)
    db.session.commit()
    
    return jsonify({
        'message': _('Family created successfully'),
        'family': family.to_dict()
    }), 201

@bp.route('/families/<int:family_id>', methods=['GET'])
def get_family(family_id):
    """获取特定家庭"""
    family = Family.query.get_or_404(family_id)
    
    # 获取投资组合摘要
    portfolio_summary = family.get_portfolio_summary()
    
    result = family.to_dict()
    result['portfolio_summary'] = portfolio_summary
    result['members'] = [member.to_dict() for member in family.members]
    result['accounts'] = [account.to_dict(include_summary=True) for account in family.accounts]
    
    return jsonify(result)

@bp.route('/families/<int:family_id>', methods=['PUT'])
def update_family(family_id):
    """更新家庭信息"""
    family = Family.query.get_or_404(family_id)
    data = request.get_json()
    
    if not data:
        return jsonify({'error': _('No data provided')}), 400
    
    if 'name' in data:
        family.name = data['name']
    
    db.session.commit()
    
    return jsonify({
        'message': _('Family updated successfully'),
        'family': family.to_dict()
    })

@bp.route('/families/<int:family_id>', methods=['DELETE'])
def delete_family(family_id):
    """删除家庭"""
    family = Family.query.get_or_404(family_id)
    
    # 检查是否有关联数据
    if family.members.count() > 0 or family.accounts.count() > 0:
        return jsonify({
            'error': _('Cannot delete family with existing members or accounts')
        }), 400
    
    db.session.delete(family)
    db.session.commit()
    
    return jsonify({
        'message': _('Family deleted successfully')
    })

@bp.route('/families/<int:family_id>/dashboard', methods=['GET'])
def get_family_dashboard(family_id):
    """获取家庭仪表板数据"""
    family = Family.query.get_or_404(family_id)
    
    # 获取投资组合摘要
    portfolio_summary = family.get_portfolio_summary()
    
    # 获取最近交易
    from app.models.transaction import Transaction
    recent_transactions = Transaction.query.join(
        Transaction.account
    ).filter(
        Account.family_id == family_id
    ).order_by(Transaction.transaction_date.desc()).limit(10).all()
    
    # 获取持仓分布（按分类）
    from app.models.holding import CurrentHolding
    holdings_by_category = {}
    
    for account in family.accounts:
        for holding in account.holdings:
            if holding.total_shares > 0 and holding.stock and holding.stock.category:
                category_name = holding.stock.category.get_localized_name()
                current_value = holding.current_value or holding.cost_value
                
                if category_name not in holdings_by_category:
                    holdings_by_category[category_name] = {
                        'name': category_name,
                        'color': holding.stock.category.color,
                        'value': 0,
                        'count': 0
                    }
                
                holdings_by_category[category_name]['value'] += current_value
                holdings_by_category[category_name]['count'] += 1
    
    # 计算百分比
    total_value = sum(cat['value'] for cat in holdings_by_category.values())
    for category in holdings_by_category.values():
        category['percentage'] = (category['value'] / total_value * 100) if total_value > 0 else 0
    
    return jsonify({
        'family': family.to_dict(),
        'portfolio_summary': portfolio_summary,
        'recent_transactions': [txn.to_dict() for txn in recent_transactions],
        'holdings_by_category': list(holdings_by_category.values()),
        'performance_data': {
            # 这里可以添加历史性能数据
            'dates': [],
            'values': []
        }
    })