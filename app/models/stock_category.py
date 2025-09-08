from datetime import datetime
from app import db

class StockCategory(db.Model):
    __tablename__ = 'stock_categories'
    
    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String(100), nullable=False, unique=True, comment='分类名称')
    name_en = db.Column(db.String(100), nullable=True, comment='英文名称')
    description = db.Column(db.Text, comment='分类描述')
    color = db.Column(db.String(20), default='#007bff', comment='分类颜色')
    created_at = db.Column(db.DateTime, default=datetime.utcnow, comment='创建时间')
    updated_at = db.Column(db.DateTime, default=datetime.utcnow, onupdate=datetime.utcnow, comment='更新时间')
    
    # 反向关联到股票
    stocks = db.relationship('StocksCache', backref='category', lazy='dynamic')
    
    def __repr__(self):
        return f'<StockCategory {self.name}>'
    
    def to_dict(self):
        return {
            'id': self.id,
            'name': self.name,
            'name_en': self.name_en,
            'description': self.description,
            'color': self.color,
            'stock_count': self.stocks.count(),
            'created_at': self.created_at.isoformat() if self.created_at else None,
            'updated_at': self.updated_at.isoformat() if self.updated_at else None
        }
    
    @staticmethod
    def get_all_with_counts():
        """获取所有分类及其股票数量"""
        from sqlalchemy import func
        from app.models.stocks_cache import StocksCache
        
        result = db.session.query(
            StockCategory.id,
            StockCategory.name,
            StockCategory.name_en,
            StockCategory.description,
            StockCategory.color,
            StockCategory.created_at,
            func.count(StocksCache.id).label('stock_count')
        ).outerjoin(
            StocksCache, StockCategory.id == StocksCache.category_id
        ).group_by(
            StockCategory.id
        ).all()
        
        return [
            {
                'id': row.id,
                'name': row.name,
                'name_en': row.name_en,
                'description': row.description,
                'color': row.color,
                'stock_count': row.stock_count,
                'created_at': row.created_at.isoformat() if row.created_at else None
            }
            for row in result
        ]
    
    @staticmethod
    def create_default_categories():
        """创建默认分类"""
        default_categories = [
            {'name': '科技股', 'name_en': 'Technology', 'color': '#007bff', 'description': '科技类股票'},
            {'name': '金融股', 'name_en': 'Financial', 'color': '#28a745', 'description': '金融类股票'},
            {'name': '消费股', 'name_en': 'Consumer', 'color': '#dc3545', 'description': '消费类股票'},
            {'name': '医疗股', 'name_en': 'Healthcare', 'color': '#6f42c1', 'description': '医疗保健类股票'},
            {'name': '能源股', 'name_en': 'Energy', 'color': '#fd7e14', 'description': '能源类股票'},
            {'name': '房地产', 'name_en': 'Real Estate', 'color': '#20c997', 'description': '房地产相关股票'},
            {'name': 'ETF', 'name_en': 'ETF', 'color': '#6c757d', 'description': '交易型开放式指数基金'},
        ]
        
        for cat_data in default_categories:
            existing = StockCategory.query.filter_by(name=cat_data['name']).first()
            if not existing:
                category = StockCategory(**cat_data)
                db.session.add(category)
        
        db.session.commit()