"""
账户模型
"""

from datetime import datetime
from app import db

class AccountType(db.Model):
    """账户类型模型"""
    
    __tablename__ = 'account_types'
    
    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String(50), nullable=False, unique=True, comment='账户类型名称')
    tax_advantaged = db.Column(db.Boolean, default=False, comment='是否税收优惠')
    annual_contribution_limit = db.Column(db.Numeric(15, 2), comment='年度供款限额')
    description = db.Column(db.Text, comment='描述')
    is_active = db.Column(db.Boolean, default=True, comment='是否激活')
    
    # 关系
    accounts = db.relationship('Account', backref='account_type', lazy='dynamic')
    
    def __repr__(self):
        return f'<AccountType {self.name}>'
    
    def to_dict(self):
        return {
            'id': self.id,
            'name': self.name,
            'tax_advantaged': self.tax_advantaged,
            'annual_contribution_limit': float(self.annual_contribution_limit) if self.annual_contribution_limit else None,
            'description': self.description,
            'is_active': self.is_active
        }

class Account(db.Model):
    """账户模型"""
    
    __tablename__ = 'accounts'
    
    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String(100), nullable=False, comment='账户名称')
    account_type_id = db.Column(db.Integer, db.ForeignKey('account_types.id'), comment='账户类型ID')
    family_id = db.Column(db.Integer, db.ForeignKey('families.id'), nullable=False, comment='家庭ID')
    is_joint = db.Column(db.Boolean, default=False, comment='是否联名账户')
    account_number = db.Column(db.String(50), comment='账户号码')
    broker_name = db.Column(db.String(100), comment='券商名称')
    created_at = db.Column(db.DateTime, default=datetime.utcnow, comment='创建时间')
    updated_at = db.Column(db.DateTime, default=datetime.utcnow, onupdate=datetime.utcnow, comment='更新时间')
    
    # 关系
    account_members = db.relationship('AccountMember', backref='account', lazy='dynamic', cascade='all, delete-orphan')
    transactions = db.relationship('Transaction', back_populates='account', lazy='dynamic', cascade='all, delete-orphan')
    # holdings = db.relationship('CurrentHolding', backref='account', lazy='dynamic', cascade='all, delete-orphan')  # CurrentHolding model deleted
    contributions = db.relationship('Contribution', backref='account', lazy='dynamic', cascade='all, delete-orphan')
    
    def __repr__(self):
        return f'<Account {self.name}>'
    
    def to_dict(self, include_summary=False):
        result = {
            'id': self.id,
            'name': self.name,
            'account_type_id': self.account_type_id,
            'account_type': self.account_type.to_dict() if self.account_type else None,
            'family_id': self.family_id,
            'is_joint': self.is_joint,
            'account_number': self.account_number,
            'broker_name': self.broker_name,
            'created_at': self.created_at.isoformat(),
            'updated_at': self.updated_at.isoformat()
        }
        
        if include_summary:
            result.update({
                'current_value': float(self.current_value or 0),
                'unrealized_gain': float(self.unrealized_gain or 0),
                'realized_gain': float(self.realized_gain or 0),
                'transaction_count': self.transactions.count(),
                'holding_count': 0,  # TODO: Re-implement with new holding system
                'members': [am.to_dict() for am in self.account_members]
            })
        
        return result
    
    @property
    def current_value(self):
        """当前市值 - temporarily disabled"""
        # TODO: Re-implement with new holding system
        # total = 0
        # for holding in self.holdings:
        #     if holding.current_price:
        #         total += holding.total_shares * holding.current_price
        # return total
        return 0
    
    @property
    def total_cost(self):
        """总成本 - temporarily disabled"""
        # TODO: Re-implement with new holding system
        # total = 0
        # for holding in self.holdings:
        #     total += holding.total_shares * holding.average_cost
        # return total
        return 0
    
    @property
    def unrealized_gain(self):
        """未实现收益"""
        return self.current_value - self.total_cost
    
    @property
    def unrealized_gain_percent(self):
        """未实现收益率"""
        if self.total_cost == 0:
            return 0
        return (self.unrealized_gain / self.total_cost) * 100
    
    @property
    def realized_gain(self):
        """已实现收益"""
        total = 0
        for transaction in self.transactions:
            if transaction.transaction_type == 'SELL':
                # 简化计算：卖出金额 - 平均成本
                total += transaction.total_amount - \
                        (transaction.quantity * transaction.average_buy_price)
        return total
    
    def get_members(self):
        """获取账户成员"""
        return [am.member for am in self.account_members]
    
    def add_member(self, member, ownership_percentage=100.0, is_primary=False):
        """添加成员到账户"""
        account_member = AccountMember(
            account_id=self.id,
            member_id=member.id,
            ownership_percentage=ownership_percentage,
            is_primary=is_primary
        )
        db.session.add(account_member)
        return account_member
    
    def get_holdings_summary(self):
        """获取持仓摘要 - temporarily disabled"""
        # TODO: Re-implement with new holding system
        return {
            'holdings': [],
            'summary': {
                'total_value': 0,
                'total_cost': 0,
                'total_unrealized_gain': 0,
                'total_unrealized_gain_percent': 0,
                'holding_count': 0
            }
        }

class AccountMember(db.Model):
    """账户成员关系模型"""
    
    __tablename__ = 'account_members'
    
    id = db.Column(db.Integer, primary_key=True)
    account_id = db.Column(db.Integer, db.ForeignKey('accounts.id'), nullable=False)
    member_id = db.Column(db.Integer, db.ForeignKey('members.id'), nullable=False)
    ownership_percentage = db.Column(db.Numeric(5, 2), default=100.00, comment='出资比例')
    is_primary = db.Column(db.Boolean, default=False, comment='是否主账户持有人')
    created_at = db.Column(db.DateTime, default=datetime.utcnow, comment='创建时间')
    
    __table_args__ = (
        db.UniqueConstraint('account_id', 'member_id', name='uq_account_member'),
    )
    
    # Relationships
    member = db.relationship('Member', backref='account_memberships', lazy='select')
    
    def __repr__(self):
        return f'<AccountMember account_id={self.account_id} member_id={self.member_id}>'
    
    def to_dict(self):
        return {
            'id': self.id,
            'account_id': self.account_id,
            'member_id': self.member_id,
            'member': self.member.to_dict() if self.member else None,
            'ownership_percentage': float(self.ownership_percentage),
            'is_primary': self.is_primary,
            'created_at': self.created_at.isoformat()
        }