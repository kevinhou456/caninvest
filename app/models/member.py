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
    email = db.Column(db.String(120), comment='邮箱')
    date_of_birth = db.Column(db.Date, comment='生日')
    sin_number = db.Column(db.String(20), comment='社会保险号')
    preferred_language = db.Column(db.String(10), default='en', comment='偏好语言')
    timezone = db.Column(db.String(50), default='UTC', comment='时区')
    created_at = db.Column(db.DateTime, default=datetime.utcnow, comment='创建时间')
    updated_at = db.Column(db.DateTime, default=datetime.utcnow, onupdate=datetime.utcnow, comment='更新时间')
    
    # 关系
    account_members = db.relationship('AccountMember', backref='member', lazy='dynamic', cascade='all, delete-orphan')
    transactions = db.relationship('Transaction', backref='member', lazy='dynamic')
    contributions = db.relationship('Contribution', backref='member', lazy='dynamic', cascade='all, delete-orphan')
    
    def __repr__(self):
        return f'<Member {self.name}>'
    
    def to_dict(self):
        """转换为字典格式"""
        return {
            'id': self.id,
            'family_id': self.family_id,
            'name': self.name,
            'email': self.email,
            'date_of_birth': self.date_of_birth.isoformat() if self.date_of_birth else None,
            'preferred_language': self.preferred_language,
            'timezone': self.timezone,
            'created_at': self.created_at.isoformat(),
            'updated_at': self.updated_at.isoformat()
        }
    
    @property
    def age(self):
        """计算年龄"""
        if not self.date_of_birth:
            return None
        today = datetime.now().date()
        age = today.year - self.date_of_birth.year
        if today.month < self.date_of_birth.month or \
           (today.month == self.date_of_birth.month and today.day < self.date_of_birth.day):
            age -= 1
        return age
    
    def get_accounts(self):
        """获取成员的所有账户，按类型和联名状态排序"""
        from app.models.account import Account
        
        # 获取直接拥有的账户和联名账户
        account_ids = [am.account_id for am in self.account_members]
        accounts = Account.query.filter(Account.id.in_(account_ids)).all()
        
        # 手动预加载account_members关系，确保访问不会引发额外查询
        for account in accounts:
            # 触发关系加载
            _ = account.account_members.all()
        
        # 排序逻辑：先普通账户(Taxable)，然后其他账户，最后联名账户
        def sort_key(account):
            account_type = account.account_type.name if account.account_type else 'Unknown'
            
            # 排序优先级：
            # 1. 联名账户放在最后
            # 2. Taxable账户优先
            # 3. 其他税收优惠账户按名称排序
            # 4. 同类型内按账户名排序
            
            if account.is_joint:
                return (2, account_type, account.name)  # 联名账户优先级最低
            elif account_type == 'Taxable':
                return (0, account_type, account.name)  # 普通账户优先级最高
            else:
                return (1, account_type, account.name)  # 其他账户中等优先级
        
        accounts.sort(key=sort_key)
        return accounts
    
    def get_total_value(self, currency='CAD'):
        """获取成员总资产（按出资比例计算）"""
        total = 0
        for account_member in self.account_members:
            account = account_member.account
            if account.currency == currency:
                ownership_ratio = account_member.ownership_percentage / 100.0
                total += (account.current_value or 0) * ownership_ratio
        return total
    
    def get_contribution_room(self, account_type_name, year=None):
        """获取税收优惠账户供款额度"""
        if not year:
            year = datetime.now().year
        
        from app.models.account import AccountType
        account_type = AccountType.query.filter_by(name=account_type_name).first()
        if not account_type:
            return None
        
        # 计算该成员在指定年度的供款总额
        total_contribution = 0
        for account_member in self.account_members:
            account = account_member.account
            if account.account_type_id == account_type.id:
                # 按出资比例计算贡献
                ownership_ratio = account_member.ownership_percentage / 100.0
                member_contributions = account.contributions.filter_by(
                    member_id=self.id, 
                    year=year
                ).all()
                for contrib in member_contributions:
                    total_contribution += contrib.contribution_amount * ownership_ratio
        
        # 返回剩余额度
        annual_limit = account_type.annual_contribution_limit or 0
        return max(0, annual_limit - total_contribution)
    
    def get_portfolio_summary(self):
        """获取成员投资组合摘要"""
        accounts = self.get_accounts()
        
        summary = {
            'total_value_cad': 0,
            'total_value_usd': 0,
            'unrealized_gain': 0,
            'realized_gain': 0,
            'account_count': len(accounts),
            'accounts_by_type': {},
            'contribution_rooms': {}
        }
        
        # 计算各项数据
        for account_member in self.account_members:
            account = account_member.account
            ownership_ratio = account_member.ownership_percentage / 100.0
            
            # 按货币统计
            if account.currency == 'CAD':
                summary['total_value_cad'] += (account.current_value or 0) * ownership_ratio
            elif account.currency == 'USD':
                summary['total_value_usd'] += (account.current_value or 0) * ownership_ratio
            
            # 收益统计
            summary['unrealized_gain'] += (account.unrealized_gain or 0) * ownership_ratio
            summary['realized_gain'] += (account.realized_gain or 0) * ownership_ratio
            
            # 按账户类型统计
            account_type = account.account_type.name if account.account_type else 'Unknown'
            if account_type not in summary['accounts_by_type']:
                summary['accounts_by_type'][account_type] = {
                    'count': 0,
                    'total_value': 0
                }
            summary['accounts_by_type'][account_type]['count'] += 1
            summary['accounts_by_type'][account_type]['total_value'] += \
                (account.current_value or 0) * ownership_ratio
        
        # 计算供款额度
        current_year = datetime.now().year
        for account_type in ['TFSA', 'RRSP']:
            summary['contribution_rooms'][account_type] = self.get_contribution_room(account_type, current_year)
        
        return summary