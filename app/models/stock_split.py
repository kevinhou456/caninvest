"""
股票拆股/合股记录（按股票登记，不按账户登记；属于外部数据的本地存储，不是计算结果缓存）
"""
from datetime import datetime
from app import db


class StockSplit(db.Model):
    __tablename__ = 'stock_splits'

    id = db.Column(db.Integer, primary_key=True)
    symbol = db.Column(db.String(20), nullable=False)
    currency = db.Column(db.String(3), nullable=False)
    split_date = db.Column(db.Date, nullable=False, comment='生效日（当天起的价格和交易都是拆股后的）')
    ratio = db.Column(db.Numeric(18, 8), nullable=False, comment='新股数/旧股数：1 拆 8 记 8；10 合 1 记 0.1')
    source = db.Column(db.String(20), nullable=False, default='yahoo')
    created_at = db.Column(db.DateTime, default=datetime.utcnow)

    __table_args__ = (
        db.UniqueConstraint('symbol', 'currency', 'split_date', name='uq_stock_split_symbol_currency_date'),
    )

    def to_dict(self):
        return {
            'id': self.id,
            'symbol': self.symbol,
            'currency': self.currency,
            'split_date': self.split_date.isoformat() if self.split_date else None,
            'ratio': float(self.ratio),
            'source': self.source,
        }
