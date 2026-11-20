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