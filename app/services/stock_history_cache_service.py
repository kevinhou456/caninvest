"""
股票历史价格缓存服务
高度可扩展且易于维护的设计
"""

import logging
import requests
import time
from datetime import datetime, timedelta, date
from typing import Dict, List, Optional, Tuple
from functools import lru_cache
from decimal import Decimal
from flask import current_app
from app import db
from app.models.stock_price_history import StockPriceHistory
from app.services.stock_price_service import StockPriceService

logger = logging.getLogger(__name__)


class StockHistoryCacheService:
    """
    股票历史价格缓存服务
    
    设计原则:
    1. 单一职责：专门处理历史价格缓存
    2. 开放封闭：易于扩展新的数据源
    3. 依赖倒置：依赖抽象而非具体实现
    4. 无重复代码：统一的数据处理和缓存逻辑
    """
    
    _GLOBAL_FETCH_REGISTRY: Dict[tuple, Dict] = {}
    _GLOBAL_FAILURE_REGISTRY: Dict[tuple, Dict] = {}
    _GLOBAL_NO_DATA_REGISTRY: Dict[tuple, List[Tuple[date, date]]] = {}

    def __init__(self):
        self.stock_service = StockPriceService()
        self.cache_days_threshold = 7  # 缓存过期天数
        self.fetch_cooldown_hours = 6  # 同一股票重复抓取的冷却时间
        self.fetch_failure_cooldown_hours = 12  # 失败后等待时间
        self._recent_fetch_registry = self._GLOBAL_FETCH_REGISTRY
        self._recent_failure_registry = self._GLOBAL_FAILURE_REGISTRY
        self._no_data_registry = self._GLOBAL_NO_DATA_REGISTRY
        self.prefetch_buffer_days = 30  # 向两侧预取的缓冲天数
        self.min_prefetch_span_days = 365  # 每次至少抓取一年的数据
        
    def get_cached_history(self, symbol: str, start_date: date, end_date: date, 
                          currency: str = 'USD', force_refresh: bool = False) -> List[Dict]:
        """
        获取缓存的历史价格数据（主入口方法）
        
        参数:
            symbol: 股票代码
            start_date: 开始日期
            end_date: 结束日期
            currency: 货币代码
            force_refresh: 是否强制刷新
            
        返回:
            历史价格数据列表
        """
        symbol = symbol.upper()
        currency = currency.upper()
        
        try:
            today = date.today()
            adjusted_start = min(start_date, today)
            adjusted_end = min(end_date, today)

            if adjusted_start > adjusted_end:
                return []

            # 1. 评估缓存状态
            cache_gaps = self._analyze_cache_gaps(symbol, adjusted_start, adjusted_end, currency)

            # 2. 如果需要刷新或有缺失，获取新数据
            if force_refresh or cache_gaps['needs_update']:
                self._update_cache_data(symbol, cache_gaps, currency, force_refresh=force_refresh)

            # 3. 从缓存返回数据
            cached_data = self._get_cached_data(symbol, adjusted_start, adjusted_end, currency)
            return cached_data
            
        except Exception as e:
            logger.error(f"获取缓存历史数据失败 {symbol}: {str(e)}")
            return []
    
    def _analyze_cache_gaps(self, symbol: str, start_date: date, end_date: date, 
                           currency: str) -> Dict:
        """
        分析缓存缺口，确定需要更新的数据范围
        
        返回:
            包含缓存分析结果的字典
        """
        # 获取当前缓存的日期范围
        latest_cached_date = StockPriceHistory.get_latest_date(symbol, currency)
        
        # 获取缓存中的所有日期
        cached_records = StockPriceHistory.query.filter(
            StockPriceHistory.symbol == symbol,
            StockPriceHistory.currency == currency,
            StockPriceHistory.trade_date >= start_date,
            StockPriceHistory.trade_date <= end_date
        ).all()
        
        cached_dates = {record.trade_date for record in cached_records}
        
        # 分析结果
        analysis = {
            'needs_update': False,
            'missing_ranges': [],
            'latest_cached_date': latest_cached_date,
            'total_cached_days': len(cached_dates),
            'cache_coverage': 0.0,
            'requested_range': (start_date, end_date)
        }
        
        # 计算应该有的交易日数量（仅考虑工作日）
        total_trading_days = self._count_trading_days(symbol, currency, start_date, end_date)
        if total_trading_days > 0:
            analysis['cache_coverage'] = len(cached_dates) / total_trading_days

        # 精确识别缺失的日期区间
        missing_ranges = self._detect_missing_ranges(symbol, currency, start_date, end_date, cached_dates)
        missing_ranges = self._subtract_no_data_ranges(symbol, currency, missing_ranges)

        # 如果没有任何缓存数据，整段都视为缺失
        if not latest_cached_date and not missing_ranges and not self._is_range_fully_no_data(symbol, currency, start_date, end_date):
            missing_ranges = [(start_date, end_date)]

        # 对缺失区间进行去噪：忽略距离最新缓存日期在3天以内的未来区间
        today = date.today()
        pruned_ranges = []
        for m_start, m_end in missing_ranges:
            # 调整区间，确保不包含未来日期
            adjusted_end = min(m_end, today)
            if adjusted_end < m_start:
                continue

            # 若仅缺失最近的几个交易日，则忽略（通常是当天尚未收盘）
            if latest_cached_date and m_start > latest_cached_date and (adjusted_end - latest_cached_date).days <= 3:
                continue

            pruned_ranges.append((m_start, adjusted_end))

        analysis['missing_ranges'] = pruned_ranges

        # 判断是否需要更新
        if pruned_ranges:
            analysis['needs_update'] = True
        else:
            analysis['needs_update'] = False

        return analysis

    def _count_trading_days(self, symbol: str, currency: str, start_date: date, end_date: date) -> int:
        """计算指定日期范围内的工作日数量（周一至周五）"""
        market = self._get_market(symbol, currency)
        count = 0
        current = start_date
        while current <= end_date:
            if current.weekday() < 5 and not self._is_market_holiday_by_market(market, current):
                count += 1
            current += timedelta(days=1)
        return count

    def _detect_missing_ranges(self, symbol: str, currency: str, start_date: date, end_date: date, cached_dates: set) -> List[Tuple[date, date]]:
        """识别指定范围内缺失的交易日区间"""
        market = self._get_market(symbol, currency)
        missing_ranges = []
        current_start = None
        last_missing_day = None

        current = start_date
        while current <= end_date:
            if current.weekday() < 5 and not self._is_market_holiday_by_market(market, current):
                if current not in cached_dates:
                    if current_start is None:
                        current_start = current
                    last_missing_day = current
                else:
                    if current_start is not None and last_missing_day is not None:
                        missing_ranges.append((current_start, last_missing_day))
                        current_start = None
                        last_missing_day = None
            current += timedelta(days=1)

        if current_start is not None and last_missing_day is not None:
            missing_ranges.append((current_start, last_missing_day))

        return missing_ranges

    def _get_market(self, symbol: str, currency: str) -> str:
        symbol = (symbol or '').upper()
        currency = (currency or '').upper()

        tsx_suffixes = ('.TO', '.TSX', '.TSXV', '.V', '.CN', '-T')
        if any(symbol.endswith(suffix) for suffix in tsx_suffixes):
            return 'CA'
        if currency == 'CAD':
            return 'CA'
        return 'US'

    def _is_market_holiday_by_market(self, market: str, target_date: date) -> bool:
        market = (market or 'US').upper()
        if market == 'CA':
            return target_date in self._get_canadian_holidays(target_date.year)
        return target_date in self._get_us_holidays(target_date.year)

    @staticmethod
    @lru_cache(maxsize=16)
    def _get_us_holidays(year: int) -> set:
        from datetime import date
        holidays = set()

        def observed(day: date) -> date:
            if day.weekday() == 5:  # Saturday
                return day - timedelta(days=1)
            if day.weekday() == 6:  # Sunday
                return day + timedelta(days=1)
            return day

        def nth_weekday(month: int, weekday: int, n: int) -> date:
            d = date(year, month, 1)
            while d.weekday() != weekday:
                d += timedelta(days=1)
            d += timedelta(weeks=n - 1)
            return d

        def last_weekday(month: int, weekday: int) -> date:
            if month == 12:
                d = date(year, month, 31)
            else:
                d = date(year, month + 1, 1) - timedelta(days=1)
            while d.weekday() != weekday:
                d -= timedelta(days=1)
            return d

        # Fixed / observed days
        holidays.add(observed(date(year, 1, 1)))   # New Year's Day
        holidays.add(nth_weekday(1, 0, 3))         # Martin Luther King Jr. Day (3rd Monday Jan)
        holidays.add(nth_weekday(2, 0, 3))         # Presidents' Day (3rd Monday Feb)
        holidays.add(last_weekday(5, 0))           # Memorial Day (last Monday May)
        holidays.add(observed(date(year, 6, 19)))  # Juneteenth National Independence Day
        holidays.add(observed(date(year, 7, 4)))   # Independence Day
        holidays.add(nth_weekday(9, 0, 1))         # Labor Day
        holidays.add(nth_weekday(11, 3, 4))        # Thanksgiving (4th Thursday Nov)
        holidays.add(observed(date(year, 12, 25))) # Christmas Day

        # Good Friday
        holidays.add(StockHistoryCacheService._good_friday(year))

        return holidays

    @staticmethod
    @lru_cache(maxsize=16)
    def _get_canadian_holidays(year: int) -> set:
        from datetime import date
        holidays = set()

        def observed(day: date) -> date:
            if day.weekday() == 5:
                return day + timedelta(days=2)
            if day.weekday() == 6:
                return day + timedelta(days=1)
            return day

        def nth_weekday(month: int, weekday: int, n: int) -> date:
            d = date(year, month, 1)
            while d.weekday() != weekday:
                d += timedelta(days=1)
            d += timedelta(weeks=n - 1)
            return d

        def last_weekday_before(month: int, day: int, weekday: int) -> date:
            d = date(year, month, day)
            while d.weekday() != weekday:
                d -= timedelta(days=1)
            return d

        holidays.add(observed(date(year, 1, 1)))           # New Year's Day
        holidays.add(nth_weekday(2, 0, 3))                 # Family Day (3rd Monday Feb)
        holidays.add(StockHistoryCacheService._good_friday(year))
        holidays.add(last_weekday_before(5, 25, 0))         # Victoria Day (Monday preceding May 25)
        holidays.add(observed(date(year, 7, 1)))           # Canada Day
        holidays.add(nth_weekday(8, 0, 1))                 # Civic Holiday (1st Monday Aug)
        holidays.add(nth_weekday(9, 0, 1))                 # Labour Day
        holidays.add(nth_weekday(10, 0, 2))                # Thanksgiving (2nd Monday Oct)
        holidays.add(observed(date(year, 12, 25)))         # Christmas

        boxing_day = date(year, 12, 26)
        observed_boxing_day = observed(boxing_day)
        holidays.add(observed_boxing_day)                  # Boxing Day (observed)

        # 如果Boxing Day补假落在周一，交易所通常也会在周二休市
        if observed_boxing_day.weekday() == 0:  # Monday
            holidays.add(observed_boxing_day + timedelta(days=1))

        return holidays

    @staticmethod
    def _good_friday(year: int) -> date:
        """计算西方教会的耶稣受难日（Good Friday）"""
        # Anonymous Gregorian algorithm for Easter
        a = year % 19
        b = year // 100
        c = year % 100
        d = b // 4
        e = b % 4
        f = (b + 8) // 25
        g = (b - f + 1) // 3
        h = (19 * a + b - d - g + 15) % 30
        i = c // 4
        k = c % 4
        l = (32 + 2 * e + 2 * i - h - k) % 7
        m = (a + 11 * h + 22 * l) // 451
        month = (h + l - 7 * m + 114) // 31
        day = ((h + l - 7 * m + 114) % 31) + 1

        easter = date(year, month, day)
        return easter - timedelta(days=2)
    
    def _merge_ranges(self, ranges: List[Tuple[date, date]]) -> List[Tuple[date, date]]:
        """合并重叠或相邻的日期区间，减少外部请求次数"""
        if not ranges:
            return []

        sorted_ranges = sorted(ranges, key=lambda r: r[0])
        merged = [sorted_ranges[0]]

        for current_start, current_end in sorted_ranges[1:]:
            last_start, last_end = merged[-1]
            if current_start <= last_end + timedelta(days=1):
                merged[-1] = (last_start, max(last_end, current_end))
            else:
                merged.append((current_start, current_end))

        return merged

    def _mark_no_data_range(self, symbol: str, currency: str, start_date: date, end_date: date):
        if not symbol or not currency or start_date is None or end_date is None:
            return
        if start_date > end_date:
            return
        key = (symbol.upper(), currency.upper())
        ranges = self._no_data_registry.get(key, [])
        ranges.append((start_date, end_date))
        self._no_data_registry[key] = self._merge_ranges(ranges)

    def _get_no_data_ranges(self, symbol: str, currency: str) -> List[Tuple[date, date]]:
        key = (symbol.upper(), currency.upper())
        return list(self._no_data_registry.get(key, []))

    def _subtract_no_data_ranges(self, symbol: str, currency: str,
                                 ranges: List[Tuple[date, date]]) -> List[Tuple[date, date]]:
        if not ranges:
            return []
        no_data_ranges = self._get_no_data_ranges(symbol, currency)
        if not no_data_ranges:
            return ranges

        adjusted: List[Tuple[date, date]] = []
        for range_start, range_end in ranges:
            segments = [(range_start, range_end)]
            for nd_start, nd_end in no_data_ranges:
                new_segments: List[Tuple[date, date]] = []
                for seg_start, seg_end in segments:
                    if nd_end < seg_start or nd_start > seg_end:
                        new_segments.append((seg_start, seg_end))
                        continue

                    if nd_start > seg_start:
                        left_end = nd_start - timedelta(days=1)
                        if seg_start <= left_end:
                            new_segments.append((seg_start, left_end))

                    if nd_end < seg_end:
                        right_start = nd_end + timedelta(days=1)
                        if right_start <= seg_end:
                            new_segments.append((right_start, seg_end))

                segments = [segment for segment in new_segments if segment[0] <= segment[1]]
                if not segments:
                    break

            adjusted.extend(segments)

        return adjusted

    def _is_range_fully_no_data(self, symbol: str, currency: str,
                               start_date: date, end_date: date) -> bool:
        coverage = self._subtract_no_data_ranges(symbol, currency, [(start_date, end_date)])
        return not coverage

    def _register_response_metadata(self, symbol: str, currency: str,
                                     response_info: Optional[Dict],
                                     fetch_start: date, fetch_end: date):
        if not response_info:
            return

        for nd_range in response_info.get('no_data_ranges', []) or []:
            nd_start, nd_end = nd_range
            if nd_start is None or nd_end is None:
                continue
            start = max(fetch_start, nd_start)
            end = min(fetch_end, nd_end)
            if start <= end:
                self._mark_no_data_range(symbol, currency, start, end)

        first_trade_date = response_info.get('first_trade_date')
        if isinstance(first_trade_date, date):
            if fetch_start < first_trade_date:
                self._mark_no_data_range(symbol, currency,
                                         fetch_start,
                                         first_trade_date - timedelta(days=1))

    def _should_skip_fetch(self, symbol: str, currency: str, start_date: date,
                           end_date: date, force_refresh: bool) -> bool:
        """判断是否应跳过重复的外部抓取"""
        if force_refresh:
            return False

        key = (symbol, currency)
        record = self._recent_fetch_registry.get(key)
        if not record or not record.get('timestamp'):
            return False

        if datetime.utcnow() - record['timestamp'] > timedelta(hours=self.fetch_cooldown_hours):
            return False

        if record.get('start') is None or record.get('end') is None:
            return False

        # 若上次抓取的范围已覆盖此次范围，则跳过
        if record['start'] <= start_date and record['end'] >= end_date:
            return True

        failure_record = self._recent_failure_registry.get(key)
        if failure_record and failure_record.get('timestamp'):
            if datetime.utcnow() - failure_record['timestamp'] <= timedelta(hours=self.fetch_failure_cooldown_hours):
                if failure_record['start'] <= start_date and failure_record['end'] >= end_date:
                    return True

        return False

    def _record_fetch(self, symbol: str, currency: str, start_date: date, end_date: date, success: bool):
        key = (symbol, currency)
        target_registry = self._recent_fetch_registry if success else self._recent_failure_registry
        record = target_registry.get(key, {'start': None, 'end': None, 'timestamp': None})

        record['start'] = start_date if record['start'] is None else min(record['start'], start_date)
        record['end'] = end_date if record['end'] is None else max(record['end'], end_date)
        record['timestamp'] = datetime.utcnow()

        target_registry[key] = record

        if success and key in self._recent_failure_registry:
            del self._recent_failure_registry[key]

    def _expand_fetch_range(self, start_date: date, end_date: date) -> Tuple[date, date]:
        """扩大需要抓取的区间，确保每次至少抓取一年并带缓冲"""
        today = date.today()
        buffer = timedelta(days=self.prefetch_buffer_days)

        effective_end = min(end_date, today)
        effective_start = min(start_date, today)

        fetch_end = min(today, effective_end + buffer)
        fetch_start = fetch_end - timedelta(days=self.min_prefetch_span_days - 1)

        buffered_start = effective_start - buffer
        fetch_start = min(fetch_start, buffered_start)

        if fetch_end > today:
            fetch_end = today
        if fetch_start > fetch_end:
            fetch_start = fetch_end

        return fetch_start, fetch_end

    def _update_cache_data(self, symbol: str, cache_gaps: Dict, currency: str,
                           force_refresh: bool = False):
        """
        更新缓存数据
        
        参数:
            symbol: 股票代码
            cache_gaps: 缓存分析结果
            currency: 货币代码
        """
        missing_ranges = cache_gaps.get('missing_ranges', [])

        # 如果强制刷新但没有明确缺口，则覆盖整个请求范围
        if force_refresh and not missing_ranges:
            requested_range = cache_gaps.get('requested_range')
            if requested_range:
                missing_ranges = [requested_range]

        merged_ranges = self._merge_ranges(missing_ranges)

        for start_date, end_date in merged_ranges:
            try:
                fetch_start, fetch_end = self._expand_fetch_range(start_date, end_date)

                if self._should_skip_fetch(symbol, currency, fetch_start, fetch_end, force_refresh):
                    continue

                print(f"[Yahoo Fetch] {symbol}({currency}) {fetch_start} -> {fetch_end} (原始缺口 {start_date}->{end_date})")
                # 从Yahoo Finance获取数据
                raw_data, response_info = self.stock_service.get_stock_history(symbol, fetch_start, fetch_end)
                self._register_response_metadata(symbol, currency, response_info, fetch_start, fetch_end)

                success = False

                if raw_data:
                    # 转换并保存数据
                    processed_data = self._process_raw_data(symbol, raw_data, currency)
                    success = StockPriceHistory.bulk_upsert(processed_data)

                    if success:
                        self._record_fetch(symbol, currency, fetch_start, fetch_end, success=True)
                    else:
                        self._record_fetch(symbol, currency, fetch_start, fetch_end, success=False)
                else:
                    if response_info and response_info.get('no_data_ranges'):
                        success = True
                        self._record_fetch(symbol, currency, fetch_start, fetch_end, success=True)
                    else:
                        self._record_fetch(symbol, currency, fetch_start, fetch_end, success=False)

            except Exception:
                continue
    
    def _process_raw_data(self, symbol: str, raw_data: Dict, currency: str) -> List[Dict]:
        """
        处理原始数据为标准格式
        
        参数:
            symbol: 股票代码
            raw_data: 原始数据
            currency: 货币代码
            
        返回:
            标准化的价格数据列表
        """
        processed_data = []
        
        for date_str, price_info in raw_data.items():
            try:
                trade_date = datetime.strptime(date_str, '%Y-%m-%d').date()
                
                # 构建标准化数据记录
                record = {
                    'symbol': symbol,
                    'trade_date': trade_date,
                    'close_price': Decimal(str(price_info.get('close', 0))),
                    'currency': currency,
                    'data_source': 'yahoo'
                }
                
                # 添加可选字段（如果可用）
                optional_fields = ['open', 'high', 'low', 'volume']
                for field in optional_fields:
                    if field in price_info and price_info[field] is not None:
                        if field == 'volume':
                            record['volume'] = int(price_info[field])
                        else:
                            record[f'{field}_price'] = Decimal(str(price_info[field]))
                
                processed_data.append(record)
                
            except (ValueError, TypeError) as e:
                logger.debug(f"处理日期 {date_str} 的数据失败: {str(e)}")
                continue
        
        return processed_data
    
    def _get_cached_data(self, symbol: str, start_date: date, end_date: date, 
                        currency: str) -> List[Dict]:
        """
        从缓存获取数据
        
        参数:
            symbol: 股票代码
            start_date: 开始日期
            end_date: 结束日期
            currency: 货币代码
            
        返回:
            历史价格数据列表
        """
        try:
            cached_records = StockPriceHistory.get_price_range(
                symbol, start_date, end_date, currency
            )
            
            # 转换为字典格式
            result = []
            for record in cached_records:
                result.append({
                    'date': record.trade_date.strftime('%Y-%m-%d'),
                    'close': float(record.close_price),
                    'open': float(record.open_price) if record.open_price else None,
                    'high': float(record.high_price) if record.high_price else None,
                    'low': float(record.low_price) if record.low_price else None,
                    'volume': record.volume
                })
            
            return result
            
        except Exception as e:
            logger.error(f"从缓存获取数据失败 {symbol}: {str(e)}")
            return []
    
    def get_cache_statistics(self, symbol: str = None, currency: str = 'USD') -> Dict:
        """
        获取缓存统计信息
        
        参数:
            symbol: 股票代码（可选，为空则统计所有）
            currency: 货币代码
            
        返回:
            缓存统计信息
        """
        try:
            query = StockPriceHistory.query
            
            if symbol:
                query = query.filter(StockPriceHistory.symbol == symbol.upper())
            if currency:
                query = query.filter(StockPriceHistory.currency == currency.upper())
            
            total_records = query.count()
            
            # 获取日期范围
            if total_records > 0:
                earliest = query.order_by(StockPriceHistory.trade_date.asc()).first()
                latest = query.order_by(StockPriceHistory.trade_date.desc()).first()
                
                date_range = {
                    'earliest_date': earliest.trade_date.isoformat(),
                    'latest_date': latest.trade_date.isoformat(),
                    'days_span': (latest.trade_date - earliest.trade_date).days
                }
            else:
                date_range = {
                    'earliest_date': None,
                    'latest_date': None,
                    'days_span': 0
                }
            
            # 按股票统计
            symbol_stats = {}
            if not symbol:
                symbol_counts = db.session.query(
                    StockPriceHistory.symbol,
                    db.func.count(StockPriceHistory.id).label('count')
                ).group_by(StockPriceHistory.symbol).all()
                
                symbol_stats = {sym: count for sym, count in symbol_counts}
            
            return {
                'total_records': total_records,
                'date_range': date_range,
                'symbol_statistics': symbol_stats,
                'query_parameters': {
                    'symbol': symbol,
                    'currency': currency
                }
            }
            
        except Exception as e:
            logger.error(f"获取缓存统计失败: {str(e)}")
            return {}

    def is_known_no_data(self, symbol: str, start_date: date, end_date: date, currency: str = 'USD') -> bool:
        """判断指定区间是否已被标记为无数据范围"""
        if not symbol or start_date is None or end_date is None:
            return False
        if start_date > end_date:
            return False
        return self._is_range_fully_no_data(symbol.upper(), currency.upper(), start_date, end_date)

    def cleanup_old_cache(self, days_to_keep: int = 365) -> Dict:
        """
        清理旧的缓存数据
        
        参数:
            days_to_keep: 保留的天数
            
        返回:
            清理结果
        """
        try:
            cutoff_date = date.today() - timedelta(days=days_to_keep)
            
            # 查找要删除的记录
            old_records = StockPriceHistory.query.filter(
                StockPriceHistory.trade_date < cutoff_date
            ).all()
            
            deleted_count = len(old_records)
            
            # 删除记录
            if old_records:
                for record in old_records:
                    db.session.delete(record)
                
                db.session.commit()
                logger.debug(f"清理了 {deleted_count} 条旧缓存记录")
            
            return {
                'success': True,
                'deleted_count': deleted_count,
                'cutoff_date': cutoff_date.isoformat()
            }
            
        except Exception as e:
            db.session.rollback()
            logger.error(f"清理缓存失败: {str(e)}")
            return {
                'success': False,
                'error': str(e),
                'deleted_count': 0
            }
