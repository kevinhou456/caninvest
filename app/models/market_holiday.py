"""
市场节假日模型
动态检测和缓存节假日信息
"""
from app import db
from datetime import date
from sqlalchemy import UniqueConstraint


class MarketHoliday(db.Model):
    """市场节假日表 - 动态检测节假日"""
    __tablename__ = 'market_holidays'

    id = db.Column(db.Integer, primary_key=True)
    holiday_date = db.Column(db.Date, nullable=False, comment='节假日期')
    market = db.Column(db.String(5), nullable=False, comment='市场代码 (US/CA)')
    confidence_level = db.Column(db.Integer, default=1, comment='置信度 (检测到的股票数量)')
    detected_at = db.Column(db.DateTime, default=db.func.current_timestamp(), comment='检测时间')

    # 复合唯一约束
    __table_args__ = (
        UniqueConstraint('holiday_date', 'market', name='unique_holiday_market'),
    )

    def to_dict(self):
        return {
            'id': self.id,
            'holiday_date': self.holiday_date,
            'market': self.market,
            'confidence_level': self.confidence_level,
            'detected_at': self.detected_at
        }

    @classmethod
    def is_holiday(cls, target_date: date, market: str) -> bool:
        """检查指定日期和市场是否为节假日"""
        return cls.query.filter_by(
            holiday_date=target_date,
            market=market
        ).first() is not None

    @classmethod
    def add_holiday_detection(cls, target_date: date, market: str, symbol: str = None):
        """添加或更新节假日检测"""
        existing = cls.query.filter_by(
            holiday_date=target_date,
            market=market
        ).first()

        if existing:
            # 增加置信度
            existing.confidence_level += 1
        else:
            # 新增节假日记录
            holiday = cls(
                holiday_date=target_date,
                market=market,
                confidence_level=1
            )
            db.session.add(holiday)

        db.session.commit()

    @classmethod
    def get_market_holidays(cls, market: str, year: int = None):
        """获取指定市场的节假日列表"""
        query = cls.query.filter_by(market=market)

        if year:
            start_date = date(year, 1, 1)
            end_date = date(year, 12, 31)
            query = query.filter(
                cls.holiday_date >= start_date,
                cls.holiday_date <= end_date
            )

        return query.all()


class StockHolidayAttempt(db.Model):
    """股票节假日检测尝试记录"""
    __tablename__ = 'stock_holiday_attempts'

    id = db.Column(db.Integer, primary_key=True)
    symbol = db.Column(db.String(20), nullable=False, comment='股票代码')
    market = db.Column(db.String(5), nullable=False, comment='市场代码')
    attempt_date = db.Column(db.Date, nullable=False, comment='尝试查询的日期')
    has_data = db.Column(db.Boolean, nullable=False, comment='是否有数据')
    attempted_at = db.Column(db.DateTime, default=db.func.current_timestamp(), comment='尝试时间')

    # 复合唯一约束
    __table_args__ = (
        UniqueConstraint('symbol', 'market', 'attempt_date', name='unique_stock_attempt'),
    )

    @classmethod
    def record_attempt(cls, symbol: str, market: str, attempt_date: date, has_data: bool):
        """记录股票节假日检测尝试"""
        if not attempt_date or attempt_date.weekday() >= 5:
            return

        existing = cls.query.filter_by(
            symbol=symbol,
            market=market,
            attempt_date=attempt_date
        ).first()

        if not existing:
            attempt = cls(
                symbol=symbol,
                market=market,
                attempt_date=attempt_date,
                has_data=has_data
            )
            db.session.add(attempt)
            db.session.commit()
        else:
            if has_data and not existing.has_data:
                existing.has_data = True
                db.session.commit()

    @classmethod
    def should_promote_to_holiday(cls, target_date: date, market: str, threshold: int = 5) -> bool:
        """检查是否应该将某个日期推广为节假日"""
        # 统计该日期该市场的失败尝试次数
        failed_count = cls.query.filter_by(
            market=market,
            attempt_date=target_date,
            has_data=False
        ).count()

        return failed_count >= threshold

    def to_dict(self):
        return {
            'id': self.id,
            'symbol': self.symbol,
            'market': self.market,
            'attempt_date': self.attempt_date,
            'has_data': self.has_data,
            'attempted_at': self.attempted_at
        }
