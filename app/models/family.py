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
    
    def get_portfolio_summary(self):
        """家庭账户概况（只含数量信息；金额统一由 PortfolioService / AssetValuationService 计算）"""
        summary = {
            'account_count': self.accounts.count(),
            'member_count': self.members.count(),
            'accounts_by_type': {},
        }
        for account in self.accounts:
            account_type = account.account_type.name if account.account_type else 'Unknown'
            summary['accounts_by_type'].setdefault(account_type, {'count': 0})
            summary['accounts_by_type'][account_type]['count'] += 1
        return summary