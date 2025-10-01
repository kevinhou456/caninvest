"""
股票缓存模型
"""

from datetime import datetime, timedelta
from app import db

class StocksCache(db.Model):
    """股票缓存模型"""
    
    __tablename__ = 'stocks_cache'
    
    id = db.Column(db.Integer, primary_key=True)
    symbol = db.Column(db.String(20), nullable=False, comment='股票代码')
    name = db.Column(db.String(255), comment='股票名称')
    exchange = db.Column(db.String(50), comment='交易所')
    currency = db.Column(db.String(10), nullable=False, default='USD', comment='交易货币')
    
    # 设置(symbol, currency)联合唯一约束
    __table_args__ = (
        db.UniqueConstraint('symbol', 'currency', name='unique_symbol_currency'),
    )
    category_id = db.Column(db.Integer, db.ForeignKey('stock_categories.id'), comment='分类ID')
    current_price = db.Column(db.Numeric(15, 4), comment='当前价格')
    price_updated_at = db.Column(db.DateTime, comment='价格更新时间')
    first_trade_date = db.Column(db.Date, comment='首次交易日期（IPO日期）')
    created_at = db.Column(db.DateTime, default=datetime.utcnow, comment='创建时间')
    updated_at = db.Column(db.DateTime, default=datetime.utcnow, onupdate=datetime.utcnow, comment='更新时间')
    
    def __repr__(self):
        return f'<StocksCache {self.symbol} {self.name}>'
    
    def to_dict(self):
        return {
            'id': self.id,
            'symbol': self.symbol,
            'name': self.name,
            'exchange': self.exchange,
            'currency': self.currency,
            'category_id': self.category_id,
            'current_price': float(self.current_price) if self.current_price else None,
            'price_updated_at': self.price_updated_at.isoformat() if self.price_updated_at else None,
            'first_trade_date': self.first_trade_date.isoformat() if self.first_trade_date else None,
            'created_at': self.created_at.isoformat(),
            'updated_at': self.updated_at.isoformat()
        }
    
    def needs_price_update(self):
        """检查是否需要更新价格"""
        if not self.price_updated_at:
            return True
        
        now = datetime.utcnow()
        time_diff = now - self.price_updated_at
        
        # 判断是否在交易时间段内
        # 简化处理：美股交易时间 9:30-16:00 ET (UTC-5/-4)
        # 加拿大股市交易时间 9:30-16:00 ET (UTC-5/-4)
        if self.is_trading_hours(now):
            # 交易时间内，15分钟更新一次
            return time_diff > timedelta(minutes=15)
        else:
            # 非交易时间，1小时更新一次
            return time_diff > timedelta(hours=1)
    
    def is_trading_hours(self, current_time=None):
        """判断是否在交易时间内"""
        if current_time is None:
            current_time = datetime.utcnow()
        
        # 简化处理：周一到周五，美东时间9:30-16:00
        # 这里暂不考虑节假日和夏令时复杂性
        weekday = current_time.weekday()  # 0=Monday, 6=Sunday
        
        if weekday >= 5:  # 周末
            return False
        
        # 粗略估算美东时间（UTC-5）
        et_time = current_time - timedelta(hours=5)
        hour = et_time.hour
        minute = et_time.minute
        
        # 9:30 - 16:00
        if hour < 9 or hour > 16:
            return False
        if hour == 9 and minute < 30:
            return False
        
        return True
    
    @classmethod
    def get_or_create(cls, symbol, currency, name=None, exchange=None):
        """获取或创建股票缓存记录"""
        stock = cls.query.filter_by(symbol=symbol.upper(), currency=currency).first()
        
        if not stock:
            stock = cls(
                symbol=symbol.upper(),
                currency=currency,
                name=name,
                exchange=exchange
            )
            db.session.add(stock)
            db.session.commit()
        
        return stock
    
    @classmethod
    def update_price(cls, symbol, currency, price):
        """更新股票价格"""
        stock = cls.query.filter_by(symbol=symbol.upper(), currency=currency).first()
        if stock:
            stock.current_price = price
            stock.price_updated_at = datetime.utcnow()
            db.session.commit()
        return stock
    
    @classmethod
    def get_stocks_needing_update(cls):
        """获取需要更新价格的股票列表"""
        stocks = cls.query.all()
        return [stock for stock in stocks if stock.needs_price_update()]