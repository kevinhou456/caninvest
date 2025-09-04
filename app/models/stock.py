"""
股票和分类模型
"""

from datetime import datetime
from flask_babel import get_locale
from app import db

class StockCategory(db.Model):
    """股票分类模型"""
    
    __tablename__ = 'stock_categories'
    
    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String(100), nullable=False, comment='分类名称')
    description = db.Column(db.Text, comment='描述')
    parent_id = db.Column(db.Integer, db.ForeignKey('stock_categories.id'), comment='父分类ID')
    color = db.Column(db.String(7), default='#4A90E2', comment='显示颜色')
    icon = db.Column(db.String(50), default='fas fa-tag', comment='图标')
    is_system = db.Column(db.Boolean, default=False, comment='是否系统预设')
    is_active = db.Column(db.Boolean, default=True, comment='是否激活')
    sort_order = db.Column(db.Integer, default=0, comment='排序')
    created_by = db.Column(db.Integer, db.ForeignKey('members.id'), comment='创建者')
    created_at = db.Column(db.DateTime, default=datetime.utcnow, comment='创建时间')
    updated_at = db.Column(db.DateTime, default=datetime.utcnow, onupdate=datetime.utcnow, comment='更新时间')
    
    # 关系
    children = db.relationship('StockCategory', backref=db.backref('parent', remote_side=[id]), lazy='dynamic')
    translations = db.relationship('StockCategoryI18n', backref='category', lazy='dynamic', cascade='all, delete-orphan')
    stocks = db.relationship('Stock', backref='category', lazy='dynamic')
    
    def __repr__(self):
        return f'<StockCategory {self.name}>'
    
    def to_dict(self, include_translations=False):
        result = {
            'id': self.id,
            'name': self.name,
            'description': self.description,
            'parent_id': self.parent_id,
            'color': self.color,
            'icon': self.icon,
            'is_system': self.is_system,
            'is_active': self.is_active,
            'sort_order': self.sort_order,
            'created_by': self.created_by,
            'created_at': self.created_at.isoformat(),
            'updated_at': self.updated_at.isoformat(),
            'stock_count': self.stocks.count()
        }
        
        if include_translations:
            result['translations'] = {
                t.language_code: {'name': t.name, 'description': t.description}
                for t in self.translations
            }
        
        return result
    
    def get_localized_name(self, language=None):
        """获取本地化名称"""
        if not language:
            try:
                language = get_locale().language
            except:
                language = 'en'
        
        # 查找对应语言的翻译
        translation = self.translations.filter_by(language_code=language).first()
        if translation and translation.name:
            return translation.name
        
        # 回退到英文
        if language != 'en':
            english_translation = self.translations.filter_by(language_code='en').first()
            if english_translation and english_translation.name:
                return english_translation.name
        
        # 最后回退到原始名称
        return self.name
    
    def get_localized_description(self, language=None):
        """获取本地化描述"""
        if not language:
            try:
                language = get_locale().language
            except:
                language = 'en'
        
        translation = self.translations.filter_by(language_code=language).first()
        if translation and translation.description:
            return translation.description
        
        if language != 'en':
            english_translation = self.translations.filter_by(language_code='en').first()
            if english_translation and english_translation.description:
                return english_translation.description
        
        return self.description or ''

class StockCategoryI18n(db.Model):
    """股票分类多语言表"""
    
    __tablename__ = 'stock_categories_i18n'
    
    id = db.Column(db.Integer, primary_key=True)
    category_id = db.Column(db.Integer, db.ForeignKey('stock_categories.id'), nullable=False)
    language_code = db.Column(db.String(10), nullable=False, comment='语言代码')
    name = db.Column(db.String(100), nullable=False, comment='翻译名称')
    description = db.Column(db.Text, comment='翻译描述')
    created_at = db.Column(db.DateTime, default=datetime.utcnow, comment='创建时间')
    updated_at = db.Column(db.DateTime, default=datetime.utcnow, onupdate=datetime.utcnow, comment='更新时间')
    
    __table_args__ = (
        db.UniqueConstraint('category_id', 'language_code', name='uq_category_language'),
    )
    
    def __repr__(self):
        return f'<StockCategoryI18n {self.category_id}:{self.language_code}>'

class Stock(db.Model):
    """股票模型"""
    
    __tablename__ = 'stocks'
    
    id = db.Column(db.Integer, primary_key=True)
    symbol = db.Column(db.String(20), nullable=False, unique=True, comment='股票代码')
    name = db.Column(db.String(200), nullable=False, comment='股票名称')
    exchange = db.Column(db.String(20), comment='交易所')
    currency = db.Column(db.String(3), default='USD', comment='货币')
    category_id = db.Column(db.Integer, db.ForeignKey('stock_categories.id'), comment='分类ID')
    sector = db.Column(db.String(100), comment='行业')
    market_cap = db.Column(db.BigInteger, comment='市值')
    is_active = db.Column(db.Boolean, default=True, comment='是否激活')
    created_at = db.Column(db.DateTime, default=datetime.utcnow, comment='创建时间')
    updated_at = db.Column(db.DateTime, default=datetime.utcnow, onupdate=datetime.utcnow, comment='更新时间')
    
    # 关系
    transactions = db.relationship('Transaction', backref='stock', lazy='dynamic')
    holdings = db.relationship('CurrentHolding', backref='stock', lazy='dynamic')
    price_cache = db.relationship('StockPriceCache', backref='stock', lazy='dynamic', cascade='all, delete-orphan')
    
    def __repr__(self):
        return f'<Stock {self.symbol}>'
    
    def to_dict(self, include_price=False):
        result = {
            'id': self.id,
            'symbol': self.symbol,
            'name': self.name,
            'exchange': self.exchange,
            'currency': self.currency,
            'category_id': self.category_id,
            'category': self.category.to_dict() if self.category else None,
            'sector': self.sector,
            'market_cap': self.market_cap,
            'is_active': self.is_active,
            'created_at': self.created_at.isoformat(),
            'updated_at': self.updated_at.isoformat()
        }
        
        if include_price:
            latest_price = self.get_latest_price()
            result.update({
                'current_price': latest_price['price'] if latest_price else None,
                'price_change': latest_price['change'] if latest_price else None,
                'price_change_percent': latest_price['change_percent'] if latest_price else None,
                'last_updated': latest_price['last_updated'] if latest_price else None
            })
        
        return result
    
    def get_latest_price(self):
        """获取最新价格"""
        from app.models.price_cache import StockPriceCache
        latest = self.price_cache.filter_by(price_type='current').order_by(
            db.desc(StockPriceCache.last_updated)
        ).first()
        
        if latest:
            return {
                'price': float(latest.price),
                'change': float(latest.price_change) if latest.price_change else None,
                'change_percent': float(latest.price_change_percent) if latest.price_change_percent else None,
                'volume': latest.volume,
                'last_updated': latest.last_updated.isoformat()
            }
        return None
    
    def get_price_history(self, days=30):
        """获取价格历史"""
        from datetime import datetime, timedelta
        from app.models.price_cache import StockPriceCache
        
        start_date = datetime.now() - timedelta(days=days)
        history = self.price_cache.filter(
            StockPriceCache.price_type == 'daily',
            StockPriceCache.date >= start_date.date()
        ).order_by(StockPriceCache.date).all()
        
        return [
            {
                'date': h.date.isoformat(),
                'price': float(h.price),
                'volume': h.volume
            }
            for h in history
        ]
    
    @staticmethod
    def search(query, limit=10):
        """搜索股票"""
        return Stock.query.filter(
            db.or_(
                Stock.symbol.ilike(f'%{query}%'),
                Stock.name.ilike(f'%{query}%')
            )
        ).filter_by(is_active=True).limit(limit).all()
    
    @staticmethod  
    def get_or_create(symbol, name=None, exchange=None, currency='USD'):
        """获取或创建股票记录"""
        stock = Stock.query.filter_by(symbol=symbol.upper()).first()
        
        if not stock:
            stock = Stock(
                symbol=symbol.upper(),
                name=name or symbol.upper(),
                exchange=exchange,
                currency=currency
            )
            db.session.add(stock)
            db.session.flush()  # 获取ID但不提交
        
        return stock