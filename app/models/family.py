"""
家庭模型
"""

from datetime import datetime
from app import db

class Family(db.Model):
    """家庭模型 - 系统顶层组织单位"""
    
    __tablename__ = 'families'
    
    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String(100), nullable=False, comment='家庭名称')
    created_at = db.Column(db.DateTime, default=datetime.utcnow, comment='创建时间')
    updated_at = db.Column(db.DateTime, default=datetime.utcnow, onupdate=datetime.utcnow, comment='更新时间')
    
    # 关系
    members = db.relationship('Member', backref='family', lazy='dynamic', cascade='all, delete-orphan')
    accounts = db.relationship('Account', backref='family', lazy='dynamic', cascade='all, delete-orphan')
    
    def __repr__(self):
        return f'<Family {self.name}>'
    
    def to_dict(self):
        """转换为字典格式"""
        return {
            'id': self.id,
            'name': self.name,
            'member_count': self.members.count(),
            'account_count': self.accounts.count(),
            'created_at': self.created_at.isoformat(),
            'updated_at': self.updated_at.isoformat()
        }
    
    @property
    def total_value(self):
        """家庭总资产"""
        total = 0
        for account in self.accounts:
            total += account.current_value or 0
        return total
    
    @property
    def total_unrealized_gain(self):
        """家庭总未实现收益"""
        total = 0
        for account in self.accounts:
            total += account.unrealized_gain or 0
        return total
    
    @property
    def total_realized_gain(self):
        """家庭总已实现收益"""
        total = 0
        for account in self.accounts:
            total += account.realized_gain or 0
        return total
    
    def get_portfolio_summary(self):
        """获取投资组合摘要"""
        # 计算总存款和取款
        total_deposits = 0
        total_withdrawals = 0
        
        for account in self.accounts:
            for transaction in account.transactions:
                if transaction.type == 'DEPOSIT':
                    # 对于存款，使用amount字段（如果存在）或者quantity * price
                    if transaction.amount:
                        total_deposits += float(transaction.amount)
                    else:
                        total_deposits += float(transaction.quantity * transaction.price)
                elif transaction.type == 'WITHDRAWAL':
                    # 对于取款，使用amount字段（如果存在）或者quantity * price
                    if transaction.amount:
                        total_withdrawals += float(transaction.amount)
                    else:
                        total_withdrawals += float(transaction.quantity * transaction.price)
        
        summary = {
            'total_value': self.total_value,
            'unrealized_gain': self.total_unrealized_gain,
            'realized_gain': self.total_realized_gain,
            'total_deposits': total_deposits,
            'total_withdrawals': total_withdrawals,
            'account_count': self.accounts.count(),
            'member_count': self.members.count(),
            'accounts_by_type': {},
            'currency_breakdown': {'CAD': 0, 'USD': 0}
        }
        
        # 按账户类型统计
        for account in self.accounts:
            account_type = account.account_type.name if account.account_type else 'Unknown'
            if account_type not in summary['accounts_by_type']:
                summary['accounts_by_type'][account_type] = {
                    'count': 0,
                    'total_value': 0
                }
            summary['accounts_by_type'][account_type]['count'] += 1
            summary['accounts_by_type'][account_type]['total_value'] += account.current_value or 0
            
            # 按货币统计(跳过，因为账户可以包含多种货币)
        
        return summary