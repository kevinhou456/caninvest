"""
成员模型
"""

from datetime import datetime
from app import db

class Member(db.Model):
    """家庭成员模型"""
    
    __tablename__ = 'members'
    
    id = db.Column(db.Integer, primary_key=True)
    family_id = db.Column(db.Integer, db.ForeignKey('families.id'), nullable=False, comment='家庭ID')
    name = db.Column(db.String(100), nullable=False, comment='成员姓名')
    created_at = db.Column(db.DateTime, default=datetime.utcnow, comment='创建时间')
    updated_at = db.Column(db.DateTime, default=datetime.utcnow, onupdate=datetime.utcnow, comment='更新时间')
    
    def __repr__(self):
        return f'<Member {self.name}>'
    
    def to_dict(self):
        """转换为字典格式"""
        return {
            'id': self.id,
            'family_id': self.family_id,
            'name': self.name,
            'created_at': self.created_at.isoformat(),
            'updated_at': self.updated_at.isoformat()
        }
    
    def get_accounts(self):
        """获取成员相关的账户"""
        from app.models.account import Account, AccountMember
        
        # 通过AccountMember中间表获取此成员的所有账户
        account_members = AccountMember.query.filter_by(member_id=self.id).all()
        accounts = []
        
        for am in account_members:
            account_data = {
                'id': am.account.id,
                'name': am.account.name,
                'account_type': am.account.account_type.name if am.account.account_type else None,
                'ownership_percentage': float(am.ownership_percentage),
                'is_primary': am.is_primary
            }
            accounts.append(account_data)
        
        return accounts