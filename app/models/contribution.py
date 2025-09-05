"""
供款记录模型（用于TFSA、RRSP等税收优惠账户）
"""

from datetime import datetime
from app import db

class Contribution(db.Model):
    """供款记录模型"""
    
    __tablename__ = 'contributions'
    
    id = db.Column(db.Integer, primary_key=True)
    account_id = db.Column(db.Integer, db.ForeignKey('accounts.id'), nullable=False, comment='账户ID')
    member_id = db.Column(db.Integer, db.ForeignKey('members.id'), nullable=False, comment='成员ID')
    year = db.Column(db.Integer, nullable=False, comment='供款年度')
    contribution_amount = db.Column(db.Numeric(15, 2), nullable=False, comment='供款金额')
    contribution_date = db.Column(db.Date, nullable=False, comment='供款日期')
    contribution_type = db.Column(db.String(20), default='CASH', comment='供款类型: CASH/TRANSFER/CARRY_FORWARD')
    notes = db.Column(db.Text, comment='备注')
    created_at = db.Column(db.DateTime, default=datetime.utcnow, comment='创建时间')
    updated_at = db.Column(db.DateTime, default=datetime.utcnow, onupdate=datetime.utcnow, comment='更新时间')
    
    def __repr__(self):
        return f'<Contribution {self.member.name if self.member else ""} {self.year} ${self.contribution_amount}>'
    
    def to_dict(self):
        return {
            'id': self.id,
            'account_id': self.account_id,
            'account': {
                'id': self.account.id,
                'name': self.account.name,
                'account_type': self.account.account_type.name if self.account.account_type else None
            } if self.account else None,
            'member_id': self.member_id,
            'member': {
                'id': self.member.id,
                'name': self.member.name
            } if self.member else None,
            'year': self.year,
            'contribution_amount': float(self.contribution_amount),
            'contribution_date': self.contribution_date.isoformat(),
            'contribution_type': self.contribution_type,
            'notes': self.notes,
            'created_at': self.created_at.isoformat(),
            'updated_at': self.updated_at.isoformat()
        }
    
    @staticmethod
    def get_member_contributions(member_id, account_type_name=None, year=None):
        """获取成员的供款记录"""
        query = Contribution.query.filter_by(member_id=member_id)
        
        if account_type_name:
            from app.models.account import AccountType
            account_type = AccountType.query.filter_by(name=account_type_name).first()
            if account_type:
                query = query.join(Contribution.account).filter(
                    Account.account_type_id == account_type.id
                )
        
        if year:
            query = query.filter_by(year=year)
        
        return query.order_by(Contribution.contribution_date.desc()).all()
    
    @staticmethod
    def calculate_total_contributions(member_id, account_type_name, year):
        """计算成员在指定年度的总供款"""
        contributions = Contribution.get_member_contributions(member_id, account_type_name, year)
        return sum(c.contribution_amount for c in contributions)
    
    @staticmethod
    def calculate_remaining_room(member_id, account_type_name, year):
        """计算剩余供款额度"""
        from app.models.account import AccountType
        
        account_type = AccountType.query.filter_by(name=account_type_name).first()
        if not account_type or not account_type.annual_contribution_limit:
            return None
        
        total_contributed = Contribution.calculate_total_contributions(
            member_id, account_type_name, year
        )
        
        return max(0, account_type.annual_contribution_limit - total_contributed)
    
    @staticmethod
    def get_contribution_summary(member_id, year=None):
        """获取成员供款摘要"""
        if not year:
            year = datetime.now().year
        
        from app.models.account import AccountType
        
        summary = {
            'year': year,
            'member_id': member_id,
            'by_account_type': {},
            'total_contributions': 0
        }
        
        # 获取所有税收优惠账户类型
        account_types = AccountType.query.filter_by(tax_advantaged=True).all()
        
        for account_type in account_types:
            contributions = Contribution.get_member_contributions(
                member_id, account_type.name, year
            )
            
            total_contributed = sum(c.contribution_amount for c in contributions)
            remaining_room = max(0, (account_type.annual_contribution_limit or 0) - total_contributed)
            
            summary['by_account_type'][account_type.name] = {
                'account_type_id': account_type.id,
                'annual_limit': float(account_type.annual_contribution_limit or 0),
                'total_contributed': float(total_contributed),
                'remaining_room': float(remaining_room),
                'utilization_rate': (total_contributed / account_type.annual_contribution_limit * 100) 
                                  if account_type.annual_contribution_limit and account_type.annual_contribution_limit > 0 else 0,
                'contribution_count': len(contributions),
                'contributions': [c.to_dict() for c in contributions]
            }
            
            summary['total_contributions'] += total_contributed
        
        return summary
    
    @staticmethod
    def get_family_contribution_summary(family_id, year=None):
        """获取家庭供款摘要"""
        if not year:
            year = datetime.now().year
        
        from app.models.member import Member
        
        members = Member.query.filter_by(family_id=family_id).all()
        
        family_summary = {
            'year': year,
            'family_id': family_id,
            'members': {},
            'totals': {
                'total_family_contributions': 0,
                'by_account_type': {}
            }
        }
        
        for member in members:
            member_summary = Contribution.get_contribution_summary(member.id, year)
            family_summary['members'][member.id] = {
                'member_name': member.name,
                'summary': member_summary
            }
            
            family_summary['totals']['total_family_contributions'] += member_summary['total_contributions']
            
            # 汇总各账户类型
            for account_type_name, data in member_summary['by_account_type'].items():
                if account_type_name not in family_summary['totals']['by_account_type']:
                    family_summary['totals']['by_account_type'][account_type_name] = {
                        'total_contributed': 0,
                        'total_remaining_room': 0,
                        'member_count': 0
                    }
                
                family_summary['totals']['by_account_type'][account_type_name]['total_contributed'] += data['total_contributed']
                family_summary['totals']['by_account_type'][account_type_name]['total_remaining_room'] += data['remaining_room']
                if data['total_contributed'] > 0:
                    family_summary['totals']['by_account_type'][account_type_name]['member_count'] += 1
        
        return family_summary
    
    @staticmethod
    def auto_create_from_transactions(account_id, year=None):
        """从交易记录自动创建供款记录"""
        if not year:
            year = datetime.now().year
        
        from app.models.account import Account
        from app.models.transaction import Transaction
        from datetime import date
        
        account = Account.query.get(account_id)
        if not account or not account.account_type or not account.account_type.tax_advantaged:
            return []
        
        # 获取当年的买入交易
        start_date = date(year, 1, 1)
        end_date = date(year, 12, 31)
        
        buy_transactions = Transaction.query.filter(
            Transaction.account_id == account_id,
            Transaction.transaction_type == 'BUY',
            Transaction.trade_date >= start_date,
            Transaction.trade_date <= end_date
        ).all()
        
        contributions_created = []
        
        for transaction in buy_transactions:
            # 检查是否已存在对应的供款记录
            existing = Contribution.query.filter(
                Contribution.account_id == account_id,
                Contribution.member_id == transaction.member_id,
                Contribution.year == year,
                Contribution.contribution_date == transaction.trade_date,
                Contribution.contribution_amount == transaction.net_amount
            ).first()
            
            if not existing:
                contribution = Contribution(
                    account_id=account_id,
                    member_id=transaction.member_id,
                    year=year,
                    contribution_amount=transaction.net_amount,
                    contribution_date=transaction.trade_date,
                    contribution_type='CASH',
                    notes=f'Auto-created from transaction {transaction.id}'
                )
                db.session.add(contribution)
                contributions_created.append(contribution)
        
        return contributions_created