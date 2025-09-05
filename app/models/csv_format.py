"""
CSV格式映射模型
"""

from datetime import datetime
from app import db
import json

class CsvFormat(db.Model):
    """CSV格式映射模型"""
    
    __tablename__ = 'csv_formats'
    
    id = db.Column(db.Integer, primary_key=True)
    format_name = db.Column(db.String(100), nullable=False, comment='格式名称')
    header_fingerprint = db.Column(db.String(255), nullable=False, unique=True, comment='表头指纹')
    column_headers = db.Column(db.Text, nullable=False, comment='原始表头JSON')
    column_mappings = db.Column(db.Text, nullable=False, comment='列映射JSON')
    usage_count = db.Column(db.Integer, default=1, comment='使用次数')
    created_at = db.Column(db.DateTime, default=datetime.utcnow, comment='创建时间')
    updated_at = db.Column(db.DateTime, default=datetime.utcnow, onupdate=datetime.utcnow, comment='更新时间')
    
    def __repr__(self):
        return f'<CsvFormat {self.format_name}>'
    
    @property
    def mappings(self):
        """获取映射字典"""
        if self.column_mappings:
            return json.loads(self.column_mappings)
        return {}
    
    @mappings.setter
    def mappings(self, value):
        """设置映射字典"""
        self.column_mappings = json.dumps(value, ensure_ascii=False)
    
    @property 
    def headers(self):
        """获取原始表头列表"""
        if self.column_headers:
            return json.loads(self.column_headers)
        return []
    
    @headers.setter
    def headers(self, value):
        """设置原始表头列表"""
        self.column_headers = json.dumps(value, ensure_ascii=False)
    
    def to_dict(self):
        return {
            'id': self.id,
            'format_name': self.format_name,
            'column_mappings': self.mappings,
            'usage_count': self.usage_count,
            'created_at': self.created_at.isoformat(),
            'updated_at': self.updated_at.isoformat()
        }
    
    @classmethod
    def get_by_name(cls, format_name):
        """根据格式名称获取格式"""
        return cls.query.filter_by(format_name=format_name).first()
    
    @staticmethod
    def generate_fingerprint(headers):
        """生成表头指纹 - 使用排序后的表头哈希"""
        import hashlib
        
        # 标准化表头：去除空格、转小写、排序
        normalized = sorted([h.strip().lower().replace(' ', '_') for h in headers])
        fingerprint_str = '|'.join(normalized)
        
        # 生成SHA256哈希
        return hashlib.sha256(fingerprint_str.encode()).hexdigest()[:32]
    
    @classmethod
    def find_by_headers(cls, headers):
        """通过表头查找匹配的格式"""
        fingerprint = cls.generate_fingerprint(headers)
        return cls.query.filter_by(header_fingerprint=fingerprint).first()
    
    @classmethod
    def create_or_update(cls, format_name, headers, column_mappings):
        """创建或更新格式"""
        fingerprint = cls.generate_fingerprint(headers)
        existing = cls.query.filter_by(header_fingerprint=fingerprint).first()
        
        if existing:
            # 更新现有格式
            existing.mappings = column_mappings
            existing.usage_count += 1
            existing.updated_at = datetime.utcnow()
            # 如果用户提供了新的格式名称，使用它
            if format_name and format_name.strip():
                existing.format_name = format_name.strip()
            return existing
        else:
            # 创建新格式
            if not format_name or not format_name.strip():
                format_name = f"Auto-detected format {fingerprint[:8]}"
            
            new_format = cls(
                format_name=format_name.strip(),
                header_fingerprint=fingerprint,
                column_headers=json.dumps(headers, ensure_ascii=False),
                column_mappings=json.dumps(column_mappings, ensure_ascii=False)
            )
            db.session.add(new_format)
            return new_format
    
    @classmethod
    def get_popular_formats(cls, limit=10):
        """获取热门格式"""
        return cls.query.order_by(cls.usage_count.desc()).limit(limit).all()