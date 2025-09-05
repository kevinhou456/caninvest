"""
股票价格缓存模型
"""

from datetime import datetime, timedelta
from app import db

class StockPriceCache(db.Model):
    """股票价格缓存模型"""
    
    __tablename__ = 'stock_price_cache'
    
    id = db.Column(db.Integer, primary_key=True)
    # TODO: Update to reference new stocks_cache table
    # stock_id = db.Column(db.Integer, db.ForeignKey('stocks.id'), nullable=False, comment='股票ID')
    stock_id = db.Column(db.Integer, nullable=False, comment='股票ID - temporarily removed FK constraint')
    price_type = db.Column(db.String(20), nullable=False, comment='价格类型: current/daily/weekly/monthly')
    price = db.Column(db.Numeric(15, 4), nullable=False, comment='价格')
    price_change = db.Column(db.Numeric(15, 4), comment='价格变化')
    price_change_percent = db.Column(db.Numeric(8, 4), comment='价格变化百分比')
    volume = db.Column(db.BigInteger, comment='交易量')
    market_cap = db.Column(db.BigInteger, comment='市值')
    date = db.Column(db.Date, nullable=False, comment='价格日期')
    last_updated = db.Column(db.DateTime, default=datetime.utcnow, comment='缓存更新时间')
    expires_at = db.Column(db.DateTime, comment='缓存过期时间')
    
    __table_args__ = (
        db.UniqueConstraint('stock_id', 'price_type', 'date', name='uq_stock_price_type_date'),
        db.Index('idx_stock_price_updated', 'stock_id', 'last_updated'),
        db.Index('idx_price_expires', 'expires_at')
    )
    
    def __repr__(self):
        return f'<StockPriceCache {self.stock.symbol if self.stock else ""} {self.price_type} ${self.price}>'
    
    def to_dict(self):
        return {
            'id': self.id,
            'stock_id': self.stock_id,
            'stock_symbol': self.stock.symbol if self.stock else None,
            'price_type': self.price_type,
            'price': float(self.price),
            'price_change': float(self.price_change) if self.price_change else None,
            'price_change_percent': float(self.price_change_percent) if self.price_change_percent else None,
            'volume': self.volume,
            'market_cap': self.market_cap,
            'date': self.date.isoformat(),
            'last_updated': self.last_updated.isoformat(),
            'expires_at': self.expires_at.isoformat() if self.expires_at else None,
            'is_expired': self.is_expired
        }
    
    @property
    def is_expired(self):
        """检查缓存是否过期"""
        if not self.expires_at:
            return False
        return datetime.utcnow() > self.expires_at
    
    @staticmethod
    def get_current_price(stock_id):
        """获取股票当前价格"""
        cache = StockPriceCache.query.filter_by(
            stock_id=stock_id,
            price_type='current'
        ).order_by(StockPriceCache.last_updated.desc()).first()
        
        if cache and not cache.is_expired:
            return cache
        
        return None
    
    @staticmethod
    def get_historical_prices(stock_id, price_type='daily', days=30):
        """获取历史价格"""
        start_date = datetime.now().date() - timedelta(days=days)
        
        prices = StockPriceCache.query.filter(
            StockPriceCache.stock_id == stock_id,
            StockPriceCache.price_type == price_type,
            StockPriceCache.date >= start_date
        ).order_by(StockPriceCache.date).all()
        
        return prices
    
    @staticmethod
    def cache_price(stock_id, price_data, price_type='current', ttl_minutes=15):
        """缓存价格数据"""
        # from app.models.stock import Stock  # Stock model deleted
        
        stock = Stock.query.get(stock_id)
        if not stock:
            return None
        
        # 设置过期时间
        expires_at = datetime.utcnow() + timedelta(minutes=ttl_minutes)
        
        # 对于当日价格，使用唯一约束处理
        if price_type in ['current', 'daily']:
            date_val = datetime.now().date()
            
            existing = StockPriceCache.query.filter_by(
                stock_id=stock_id,
                price_type=price_type,
                date=date_val
            ).first()
            
            if existing:
                # 更新现有记录
                existing.price = price_data.get('price')
                existing.price_change = price_data.get('change')
                existing.price_change_percent = price_data.get('change_percent')
                existing.volume = price_data.get('volume')
                existing.market_cap = price_data.get('market_cap')
                existing.last_updated = datetime.utcnow()
                existing.expires_at = expires_at
                return existing
        
        # 创建新记录
        cache = StockPriceCache(
            stock_id=stock_id,
            price_type=price_type,
            price=price_data.get('price'),
            price_change=price_data.get('change'),
            price_change_percent=price_data.get('change_percent'),
            volume=price_data.get('volume'),
            market_cap=price_data.get('market_cap'),
            date=price_data.get('date', datetime.now().date()),
            expires_at=expires_at
        )
        
        db.session.add(cache)
        return cache
    
    @staticmethod
    def cleanup_expired():
        """清理过期的缓存"""
        expired_count = StockPriceCache.query.filter(
            StockPriceCache.expires_at < datetime.utcnow()
        ).delete()
        
        return expired_count

class PriceUpdateLog(db.Model):
    """价格更新日志模型（防止频繁请求）"""
    
    __tablename__ = 'price_update_log'
    
    id = db.Column(db.Integer, primary_key=True)
    # TODO: Update to reference new stocks_cache table
    # stock_id = db.Column(db.Integer, db.ForeignKey('stocks.id'), nullable=False, comment='股票ID')
    stock_id = db.Column(db.Integer, nullable=False, comment='股票ID - temporarily removed FK constraint')
    data_source = db.Column(db.String(50), nullable=False, comment='数据源')
    request_count = db.Column(db.Integer, default=1, comment='当日请求次数')
    last_request_time = db.Column(db.DateTime, default=datetime.utcnow, comment='最后请求时间')
    date = db.Column(db.Date, default=lambda: datetime.now().date(), comment='日期')
    is_blocked = db.Column(db.Boolean, default=False, comment='是否被封锁')
    blocked_until = db.Column(db.DateTime, comment='封锁解除时间')
    error_message = db.Column(db.Text, comment='错误信息')
    
    __table_args__ = (
        db.UniqueConstraint('stock_id', 'data_source', 'date', name='uq_stock_source_date'),
    )
    
    def __repr__(self):
        return f'<PriceUpdateLog {self.stock.symbol if self.stock else ""} {self.data_source} {self.request_count}>'
    
    def to_dict(self):
        return {
            'id': self.id,
            'stock_id': self.stock_id,
            'stock_symbol': self.stock.symbol if self.stock else None,
            'data_source': self.data_source,
            'request_count': self.request_count,
            'last_request_time': self.last_request_time.isoformat(),
            'date': self.date.isoformat(),
            'is_blocked': self.is_blocked,
            'blocked_until': self.blocked_until.isoformat() if self.blocked_until else None,
            'error_message': self.error_message
        }
    
    @property
    def is_rate_limited(self):
        """检查是否达到速率限制"""
        # 检查是否被封锁
        if self.is_blocked and self.blocked_until:
            if datetime.utcnow() < self.blocked_until:
                return True
            else:
                # 解除封锁
                self.is_blocked = False
                self.blocked_until = None
        
        # 检查请求次数限制
        from flask import current_app
        max_requests = current_app.config.get('MAX_DAILY_PRICE_REQUESTS', 200)
        
        return self.request_count >= max_requests
    
    @staticmethod
    def log_request(stock_id, data_source, success=True, error_message=None):
        """记录价格请求"""
        today = datetime.now().date()
        
        log = PriceUpdateLog.query.filter_by(
            stock_id=stock_id,
            data_source=data_source,
            date=today
        ).first()
        
        if not log:
            log = PriceUpdateLog(
                stock_id=stock_id,
                data_source=data_source,
                date=today,
                request_count=0
            )
            db.session.add(log)
        
        log.request_count += 1
        log.last_request_time = datetime.utcnow()
        
        if not success:
            log.error_message = error_message
            
            # 如果连续失败，可能需要临时封锁
            if 'rate limit' in (error_message or '').lower():
                log.is_blocked = True
                log.blocked_until = datetime.utcnow() + timedelta(hours=1)
        
        return log
    
    @staticmethod
    def can_request_price(stock_id, data_source):
        """检查是否可以请求价格"""
        today = datetime.now().date()
        
        log = PriceUpdateLog.query.filter_by(
            stock_id=stock_id,
            data_source=data_source,
            date=today
        ).first()
        
        if not log:
            return True
        
        return not log.is_rate_limited
    
    @staticmethod
    def get_request_stats(data_source=None, days=7):
        """获取请求统计"""
        start_date = datetime.now().date() - timedelta(days=days)
        
        query = PriceUpdateLog.query.filter(
            PriceUpdateLog.date >= start_date
        )
        
        if data_source:
            query = query.filter_by(data_source=data_source)
        
        logs = query.all()
        
        stats = {
            'total_requests': sum(log.request_count for log in logs),
            'total_stocks': len(set(log.stock_id for log in logs)),
            'by_source': {},
            'by_date': {},
            'blocked_count': sum(1 for log in logs if log.is_blocked)
        }
        
        for log in logs:
            # 按数据源统计
            if log.data_source not in stats['by_source']:
                stats['by_source'][log.data_source] = {
                    'request_count': 0,
                    'stock_count': 0,
                    'blocked_count': 0
                }
            stats['by_source'][log.data_source]['request_count'] += log.request_count
            stats['by_source'][log.data_source]['stock_count'] += 1
            if log.is_blocked:
                stats['by_source'][log.data_source]['blocked_count'] += 1
            
            # 按日期统计
            date_str = log.date.isoformat()
            if date_str not in stats['by_date']:
                stats['by_date'][date_str] = 0
            stats['by_date'][date_str] += log.request_count
        
        return stats