from datetime import datetime
from app import db


class PerformanceDailyCache(db.Model):
    """每账户每日资产快照缓存，用于加速 Performance Comparison 计算。

    缓存原则：
    - 以 (account_id, cache_date) 为唯一键
    - 始终存储 proportion=1 的原始值，调用方负责乘以 proportion
    - 历史数据（非今日）永久有效，除非被主动失效
    - 今日数据有 TTL（由 PerformanceCacheService 判断）
    - 失效方式：直接删除对应行（硬删除）
    """

    __tablename__ = 'performance_daily_cache'

    id = db.Column(db.Integer, primary_key=True)
    account_id = db.Column(db.Integer, db.ForeignKey('accounts.id', ondelete='CASCADE'),
                           nullable=False)
    cache_date = db.Column(db.Date, nullable=False)
    stock_value = db.Column(db.Numeric(18, 4), nullable=False, default=0)
    cash_value = db.Column(db.Numeric(18, 4), nullable=False, default=0)
    total_assets = db.Column(db.Numeric(18, 4), nullable=False, default=0)
    daily_flow = db.Column(db.Numeric(18, 4), nullable=False, default=0)
    created_at = db.Column(db.DateTime, default=datetime.utcnow)

    __table_args__ = (
        db.UniqueConstraint('account_id', 'cache_date',
                            name='uq_performance_daily_cache_account_date'),
        db.Index('idx_performance_daily_cache_account_date', 'account_id', 'cache_date'),
    )

    def __repr__(self):
        return f'<PerformanceDailyCache account={self.account_id} date={self.cache_date}>'
