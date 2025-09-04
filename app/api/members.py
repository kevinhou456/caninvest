"""
家庭成员管理API
"""

from flask import request, jsonify
from flask_babel import _
from datetime import datetime
from app import db
from app.models.member import Member
from app.models.family import Family
from . import bp

@bp.route('/families/<int:family_id>/members', methods=['GET'])
def get_family_members(family_id):
    """获取家庭成员"""
    family = Family.query.get_or_404(family_id)
    members = family.members.all()
    
    return jsonify({
        'members': [member.to_dict() for member in members]
    })

@bp.route('/families/<int:family_id>/members', methods=['POST'])
def create_member(family_id):
    """创建成员"""
    family = Family.query.get_or_404(family_id)
    data = request.get_json()
    
    if not data or not data.get('name'):
        return jsonify({'error': _('Member name is required')}), 400
    
    member = Member(
        family_id=family_id,
        name=data['name'],
        email=data.get('email'),
        sin_number=data.get('sin_number'),
        preferred_language=data.get('preferred_language', 'en'),
        timezone=data.get('timezone', 'UTC')
    )
    
    # 处理生日
    if data.get('date_of_birth'):
        try:
            member.date_of_birth = datetime.strptime(
                data['date_of_birth'], '%Y-%m-%d'
            ).date()
        except ValueError:
            return jsonify({'error': _('Invalid date format')}), 400
    
    db.session.add(member)
    db.session.commit()
    
    return jsonify({
        'message': _('Member created successfully'),
        'member': member.to_dict()
    }), 201

@bp.route('/members/<int:member_id>', methods=['GET'])
def get_member(member_id):
    """获取成员详情"""
    member = Member.query.get_or_404(member_id)
    
    # 获取成员的投资组合摘要
    portfolio_summary = member.get_portfolio_summary()
    
    result = member.to_dict()
    result['portfolio_summary'] = portfolio_summary
    result['accounts'] = [am.account.to_dict() for am in member.account_members]
    
    return jsonify(result)

@bp.route('/members/<int:member_id>', methods=['PUT'])
def update_member(member_id):
    """更新成员信息"""
    member = Member.query.get_or_404(member_id)
    data = request.get_json()
    
    if not data:
        return jsonify({'error': _('No data provided')}), 400
    
    # 更新字段
    if 'name' in data:
        member.name = data['name']
    if 'email' in data:
        member.email = data['email']
    if 'sin_number' in data:
        member.sin_number = data['sin_number']
    if 'preferred_language' in data:
        member.preferred_language = data['preferred_language']
    if 'timezone' in data:
        member.timezone = data['timezone']
    
    # 处理生日
    if 'date_of_birth' in data:
        if data['date_of_birth']:
            try:
                member.date_of_birth = datetime.strptime(
                    data['date_of_birth'], '%Y-%m-%d'
                ).date()
            except ValueError:
                return jsonify({'error': _('Invalid date format')}), 400
        else:
            member.date_of_birth = None
    
    db.session.commit()
    
    return jsonify({
        'message': _('Member updated successfully'),
        'member': member.to_dict()
    })

@bp.route('/members/<int:member_id>', methods=['DELETE'])
def delete_member(member_id):
    """删除成员"""
    member = Member.query.get_or_404(member_id)
    
    # 检查是否有关联的账户或交易
    if member.account_members.count() > 0:
        return jsonify({
            'error': _('Cannot delete member with existing accounts')
        }), 400
    
    if member.transactions.count() > 0:
        return jsonify({
            'error': _('Cannot delete member with existing transactions')
        }), 400
    
    db.session.delete(member)
    db.session.commit()
    
    return jsonify({
        'message': _('Member deleted successfully')
    })

@bp.route('/members/<int:member_id>/portfolio', methods=['GET'])
def get_member_portfolio(member_id):
    """获取成员投资组合"""
    member = Member.query.get_or_404(member_id)
    
    # 获取成员持仓
    from app.models.holding import CurrentHolding
    holdings = CurrentHolding.get_holdings_by_member(member_id)
    
    # 获取成员交易历史
    from app.models.transaction import Transaction
    transactions = Transaction.get_transactions_by_member(member_id, limit=50)
    
    # 获取供款摘要
    from app.models.contribution import Contribution
    current_year = datetime.now().year
    contribution_summary = Contribution.get_contribution_summary(member_id, current_year)
    
    return jsonify({
        'member': member.to_dict(),
        'holdings': holdings,
        'recent_transactions': [txn.to_dict() for txn in transactions],
        'contribution_summary': contribution_summary,
        'portfolio_summary': member.get_portfolio_summary()
    })

@bp.route('/members/<int:member_id>/contributions', methods=['GET'])
def get_member_contributions(member_id):
    """获取成员供款记录"""
    member = Member.query.get_or_404(member_id)
    
    year = request.args.get('year', type=int)
    account_type = request.args.get('account_type')
    
    from app.models.contribution import Contribution
    
    if year:
        contribution_summary = Contribution.get_contribution_summary(member_id, year)
    else:
        # 获取当前年度的供款摘要
        current_year = datetime.now().year
        contribution_summary = Contribution.get_contribution_summary(member_id, current_year)
    
    return jsonify({
        'member': member.to_dict(),
        'contribution_summary': contribution_summary
    })

@bp.route('/members/<int:member_id>/set-language', methods=['POST'])
def set_member_language(member_id):
    """设置成员语言偏好"""
    member = Member.query.get_or_404(member_id)
    data = request.get_json()
    
    language = data.get('language')
    if not language or language not in ['en', 'zh_CN']:
        return jsonify({'error': _('Invalid language')}), 400
    
    member.preferred_language = language
    db.session.commit()
    
    return jsonify({
        'message': _('Language preference updated'),
        'member': member.to_dict()
    })