"""
股票历史价格缓存模型
"""

from app import db
from datetime import datetime
from decimal import Decimal
from sqlalchemy import Index


class StockPriceHistory(db.Model):
    """股票历史价格缓存表"""
    __tablename__ = 'stock_price_history'
    
    id = db.Column(db.Integer, primary_key=True)
    symbol = db.Column(db.String(20), nullable=False, index=True)
    trade_date = db.Column(db.Date, nullable=False, index=True)
    
    # 价格数据 - 使用Decimal确保精度
    open_price = db.Column(db.Numeric(precision=15, scale=4), nullable=True)
    high_price = db.Column(db.Numeric(precision=15, scale=4), nullable=True)
    low_price = db.Column(db.Numeric(precision=15, scale=4), nullable=True)
    close_price = db.Column(db.Numeric(precision=15, scale=4), nullable=False)
    adjusted_close = db.Column(db.Numeric(precision=15, scale=4), nullable=True)
    
    # 交易量
    volume = db.Column(db.BigInteger, nullable=True)
    
    # 元数据
    currency = db.Column(db.String(3), nullable=False, default='USD')
    data_source = db.Column(db.String(20), nullable=False, default='yahoo')
    created_at = db.Column(db.DateTime, nullable=False, default=datetime.utcnow)
    updated_at = db.Column(db.DateTime, nullable=False, default=datetime.utcnow, onupdate=datetime.utcnow)
    
    # 复合索引 - 优化查询性能
    __table_args__ = (
        Index('idx_symbol_date', 'symbol', 'trade_date'),
        Index('idx_symbol_date_currency', 'symbol', 'trade_date', 'currency'),
        db.UniqueConstraint('symbol', 'trade_date', 'currency', name='uq_symbol_date_currency'),
    )
    
    def __repr__(self):
        return f'<StockPriceHistory {self.symbol} {self.trade_date} {self.close_price}>'
    
    def to_dict(self):
        """转换为字典格式"""
        return {
            'id': self.id,
            'symbol': self.symbol,
            'trade_date': self.trade_date.isoformat() if self.trade_date else None,
            'open_price': float(self.open_price) if self.open_price else None,
            'high_price': float(self.high_price) if self.high_price else None,
            'low_price': float(self.low_price) if self.low_price else None,
            'close_price': float(self.close_price) if self.close_price else None,
            'adjusted_close': float(self.adjusted_close) if self.adjusted_close else None,
            'volume': self.volume,
            'currency': self.currency,
            'data_source': self.data_source,
            'created_at': self.created_at.isoformat() if self.created_at else None,
            'updated_at': self.updated_at.isoformat() if self.updated_at else None,
        }
    
    @classmethod
    def get_price_range(cls, symbol, start_date, end_date, currency='USD'):
        """
        获取指定时间范围的价格数据
        
        参数:
            symbol: 股票代码
            start_date: 开始日期
            end_date: 结束日期 
            currency: 货币代码
            
        返回:
            查询结果列表
        """
        return cls.query.filter(
            cls.symbol == symbol.upper(),
            cls.currency == currency.upper(),
            cls.trade_date >= start_date,
            cls.trade_date <= end_date
        ).order_by(cls.trade_date.asc()).all()
    
    @classmethod
    def get_latest_date(cls, symbol, currency='USD'):
        """
        获取指定股票的最新缓存日期
        
        参数:
            symbol: 股票代码
            currency: 货币代码
            
        返回:
            最新日期或None
        """
        result = cls.query.filter(
            cls.symbol == symbol.upper(),
            cls.currency == currency.upper()
        ).order_by(cls.trade_date.desc()).first()
        
        return result.trade_date if result else None
    
    @classmethod
    def bulk_upsert(cls, price_data_list):
        """
        批量插入或更新价格数据
        
        参数:
            price_data_list: 价格数据列表，每个元素包含symbol, trade_date, prices等
        """
        try:
            # 使用no_autoflush避免中间查询触发自动刷新
            with db.session.no_autoflush:
                for data in price_data_list:
                    # 检查是否已存在
                    existing = cls.query.filter_by(
                        symbol=data['symbol'].upper(),
                        trade_date=data['trade_date'],
                        currency=data.get('currency', 'USD').upper()
                    ).first()

                    if existing:
                        # 更新现有记录
                        existing.open_price = data.get('open_price')
                        existing.high_price = data.get('high_price')
                        existing.low_price = data.get('low_price')
                        existing.close_price = data['close_price']
                        existing.adjusted_close = data.get('adjusted_close')
                        existing.volume = data.get('volume')
                        existing.updated_at = datetime.utcnow()
                    else:
                        # 创建新记录
                        new_record = cls(
                            symbol=data['symbol'].upper(),
                            trade_date=data['trade_date'],
                            open_price=data.get('open_price'),
                            high_price=data.get('high_price'),
                            low_price=data.get('low_price'),
                            close_price=data['close_price'],
                            adjusted_close=data.get('adjusted_close'),
                            volume=data.get('volume'),
                            currency=data.get('currency', 'USD').upper(),
                            data_source=data.get('data_source', 'yahoo')
                        )
                        db.session.add(new_record)

            db.session.commit()
            return True
            
        except Exception as e:
            db.session.rollback()
            print(f"批量插入价格数据失败: {str(e)}")
            return False