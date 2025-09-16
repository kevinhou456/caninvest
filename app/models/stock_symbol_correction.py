"""
股票代码矫正模型
用于记录股票代码的矫正记录，避免重复矫正
"""

from app import db
from datetime import datetime


class StockSymbolCorrection(db.Model):
    """股票代码矫正表"""
    __tablename__ = 'stock_symbol_corrections'
    
    # 联合主键：原股票代码 + 货币
    original_symbol = db.Column(db.String(20), primary_key=True, comment='原股票代码')
    currency = db.Column(db.String(3), primary_key=True, comment='货币代码')
    
    # 矫正后的股票代码
    corrected_symbol = db.Column(db.String(20), nullable=False, comment='矫正后的股票代码')
    
    # 时间戳和备注
    created_at = db.Column(db.DateTime, default=datetime.utcnow, comment='创建时间')
    updated_at = db.Column(db.DateTime, default=datetime.utcnow, onupdate=datetime.utcnow, comment='更新时间')
    note = db.Column(db.Text, comment='备注')
    
    def __repr__(self):
        return f'<StockSymbolCorrection {self.original_symbol}({self.currency}) -> {self.corrected_symbol}>'
    
    @classmethod
    def add_correction(cls, original_symbol, currency, corrected_symbol, note=None):
        """添加或更新股票代码矫正记录"""
        correction = cls.query.filter_by(
            original_symbol=original_symbol.upper(),
            currency=currency.upper()
        ).first()
        
        if correction:
            # 更新现有记录
            correction.corrected_symbol = corrected_symbol.upper()
            correction.updated_at = datetime.utcnow()
            if note:
                correction.note = note
        else:
            # 创建新记录
            correction = cls(
                original_symbol=original_symbol.upper(),
                currency=currency.upper(),
                corrected_symbol=corrected_symbol.upper(),
                note=note
            )
            db.session.add(correction)
        
        db.session.commit()
        return correction
    
    @classmethod
    def get_corrected_symbol(cls, original_symbol, currency):
        """获取矫正后的股票代码，如果没有矫正记录则返回原代码"""
        correction = cls.query.filter_by(
            original_symbol=original_symbol.upper(),
            currency=currency.upper()
        ).first()
        
        if correction:
            # print(f"股票代码矫正: {original_symbol}({currency}) -> {correction.corrected_symbol}")
            return correction.corrected_symbol
        
        return original_symbol.upper()
    
    @classmethod
    def has_correction(cls, original_symbol, currency):
        """检查是否有矫正记录"""
        return cls.query.filter_by(
            original_symbol=original_symbol.upper(),
            currency=currency.upper()
        ).first() is not None
    
    def to_dict(self):
        """转换为字典格式"""
        return {
            'original_symbol': self.original_symbol,
            'currency': self.currency,
            'corrected_symbol': self.corrected_symbol,
            'created_at': self.created_at.isoformat() if self.created_at else None,
            'updated_at': self.updated_at.isoformat() if self.updated_at else None,
            'note': self.note
        }