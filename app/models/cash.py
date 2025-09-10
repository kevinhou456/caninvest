"""
现金表模型
"""

from datetime import datetime
from decimal import Decimal
from app import db


class Cash(db.Model):
    """现金表模型"""
    
    __tablename__ = 'cash'
    
    id = db.Column(db.Integer, primary_key=True)
    account_id = db.Column(db.Integer, db.ForeignKey('accounts.id'), nullable=False, comment='账户ID')
    usd = db.Column(db.Numeric(15, 2), nullable=False, default=0, comment='美元现金')
    cad = db.Column(db.Numeric(15, 2), nullable=False, default=0, comment='加元现金')
    created_at = db.Column(db.DateTime, default=datetime.utcnow, comment='创建时间')
    updated_at = db.Column(db.DateTime, default=datetime.utcnow, onupdate=datetime.utcnow, comment='更新时间')
    
    # 关系
    account = db.relationship('Account', backref=db.backref('cash_records', lazy='dynamic'))
    
    def __repr__(self):
        return f'<Cash Account{self.account_id} USD:{self.usd} CAD:{self.cad}>'
    
    def to_dict(self):
        return {
            'id': self.id,
            'account_id': self.account_id,
            'usd': float(self.usd) if self.usd else 0,
            'cad': float(self.cad) if self.cad else 0,
            'total_cad': self.get_total_cad(),
            'created_at': self.created_at.isoformat(),
            'updated_at': self.updated_at.isoformat()
        }
    
    def get_total_cad(self, exchange_rate=None):
        """获取总现金（换算为CAD）"""
        if exchange_rate is None:
            # 使用默认汇率 1.35
            exchange_rate = Decimal('1.35')
        
        usd_in_cad = (self.usd or Decimal('0')) * exchange_rate
        total_cad = (self.cad or Decimal('0')) + usd_in_cad
        return float(total_cad)
    
    @classmethod
    def get_account_cash(cls, account_id):
        """获取账户现金记录"""
        return cls.query.filter_by(account_id=account_id).first()
    
    @classmethod
    def get_or_create(cls, account_id):
        """获取或创建账户现金记录"""
        cash = cls.query.filter_by(account_id=account_id).first()
        if not cash:
            cash = cls(account_id=account_id, usd=0, cad=0)
            db.session.add(cash)
            db.session.commit()
        return cash
    
    @classmethod
    def update_cash(cls, account_id, usd=None, cad=None):
        """更新账户现金"""
        cash = cls.get_or_create(account_id)
        
        if usd is not None:
            cash.usd = Decimal(str(usd))
        if cad is not None:
            cash.cad = Decimal(str(cad))
        
        cash.updated_at = datetime.utcnow()
        db.session.commit()
        return cash
    
    @classmethod
    def get_total_cash_by_accounts(cls, account_ids, exchange_rate=None):
        """获取多个账户的总现金"""
        if exchange_rate is None:
            exchange_rate = Decimal('1.35')
        
        cash_records = cls.query.filter(cls.account_id.in_(account_ids)).all()
        
        total_usd = sum(cash.usd or Decimal('0') for cash in cash_records)
        total_cad = sum(cash.cad or Decimal('0') for cash in cash_records)
        
        # 换算为CAD总值
        total_cad_equivalent = total_cad + (total_usd * exchange_rate)
        
        return {
            'usd': float(total_usd),
            'cad': float(total_cad),
            'total_cad': float(total_cad_equivalent),
            'usd_in_cad': float(total_usd * exchange_rate)
        }