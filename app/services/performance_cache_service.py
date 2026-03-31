"""
Performance Daily Cache Service

管理 performance_daily_cache 表的读写和失效逻辑。

设计原则：
- 缓存存储 proportion=1 的原始账户资产数据
- 历史数据（非今日）永不过期，直到被主动失效
- 今日数据有 TTL（30分钟），TTL 由 created_at 判断
- 失效方式：硬删除对应行
- 组合账户的合计由调用方在内存中求和，不单独缓存
"""

import logging
from datetime import datetime, date, timedelta, timezone
from decimal import Decimal
from typing import Dict, List, Optional, Tuple

from app import db
from app.models.performance_daily_cache import PerformanceDailyCache

logger = logging.getLogger(__name__)

# 今日缓存 TTL（秒）
TODAY_TTL_SECONDS = 1800  # 30 分钟


class PerformanceCacheService:

    def get_cached_rows(self, account_id: int, dates: List[date]) -> Dict[date, PerformanceDailyCache]:
        """返回 {date: row} 字典，只包含已缓存且有效的日期。

        今日数据超过 TTL 则视为无效（不包含在结果中）。
        """
        if not dates:
            return {}

        rows = PerformanceDailyCache.query.filter(
            PerformanceDailyCache.account_id == account_id,
            PerformanceDailyCache.cache_date.in_(dates)
        ).all()

        today = date.today()
        now = datetime.utcnow()
        result: Dict[date, PerformanceDailyCache] = {}

        for row in rows:
            if row.cache_date == today:
                # 今日缓存检查 TTL
                age_seconds = (now - row.created_at).total_seconds()
                if age_seconds > TODAY_TTL_SECONDS:
                    continue  # 过期，不返回
            result[row.cache_date] = row

        return result

    def save_rows(self, account_id: int, data: Dict[date, dict]) -> None:
        """批量写入缓存行（upsert）。

        data 格式：{date: {'stock_value': Decimal, 'cash_value': Decimal,
                            'total_assets': Decimal, 'daily_flow': Decimal}}
        """
        if not data:
            return

        dates = list(data.keys())
        existing = {
            row.cache_date: row
            for row in PerformanceDailyCache.query.filter(
                PerformanceDailyCache.account_id == account_id,
                PerformanceDailyCache.cache_date.in_(dates)
            ).all()
        }

        now = datetime.utcnow()
        for d, values in data.items():
            row = existing.get(d)
            if row is None:
                row = PerformanceDailyCache(account_id=account_id, cache_date=d)
                db.session.add(row)
            row.stock_value = values.get('stock_value', Decimal('0'))
            row.cash_value = values.get('cash_value', Decimal('0'))
            row.total_assets = values.get('total_assets', Decimal('0'))
            row.daily_flow = values.get('daily_flow', Decimal('0'))
            row.created_at = now  # 刷新时间戳（TTL 用）

        try:
            db.session.commit()
        except Exception as e:
            db.session.rollback()
            logger.warning(f"PerformanceCacheService.save_rows failed for account {account_id}: {e}")

    def invalidate_from_date(self, account_id: int, from_date: date) -> int:
        """删除账户从 from_date 起（含）的所有缓存行。

        用于：新增/修改/删除交易后失效受影响日期。
        返回删除行数。
        """
        try:
            count = PerformanceDailyCache.query.filter(
                PerformanceDailyCache.account_id == account_id,
                PerformanceDailyCache.cache_date >= from_date
            ).delete(synchronize_session=False)
            db.session.commit()
            logger.debug(f"Invalidated {count} cache rows for account {account_id} from {from_date}")
            return count
        except Exception as e:
            db.session.rollback()
            logger.warning(f"PerformanceCacheService.invalidate_from_date failed: {e}")
            return 0

    def invalidate_account(self, account_id: int) -> int:
        """删除账户的全部缓存行。

        用于：清空该账户所有交易记录时。
        """
        try:
            count = PerformanceDailyCache.query.filter(
                PerformanceDailyCache.account_id == account_id
            ).delete(synchronize_session=False)
            db.session.commit()
            logger.debug(f"Invalidated all {count} cache rows for account {account_id}")
            return count
        except Exception as e:
            db.session.rollback()
            logger.warning(f"PerformanceCacheService.invalidate_account failed: {e}")
            return 0

    def invalidate_date_all_accounts(self, target_date: date) -> int:
        """删除所有账户在 target_date 的缓存行。

        用于：该日期股价批量更新后。
        """
        try:
            count = PerformanceDailyCache.query.filter(
                PerformanceDailyCache.cache_date == target_date
            ).delete(synchronize_session=False)
            db.session.commit()
            logger.debug(f"Invalidated {count} cache rows for date {target_date} across all accounts")
            return count
        except Exception as e:
            db.session.rollback()
            logger.warning(f"PerformanceCacheService.invalidate_date_all_accounts failed: {e}")
            return 0

    def invalidate_all(self) -> int:
        """清空全表缓存。用于全量重建或数据迁移场景。"""
        try:
            count = PerformanceDailyCache.query.delete(synchronize_session=False)
            db.session.commit()
            logger.info(f"Cleared all {count} performance cache rows")
            return count
        except Exception as e:
            db.session.rollback()
            logger.warning(f"PerformanceCacheService.invalidate_all failed: {e}")
            return 0


# 单例
performance_cache_service = PerformanceCacheService()
