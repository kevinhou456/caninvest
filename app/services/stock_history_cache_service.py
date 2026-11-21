"""
简化的股票历史价格缓存服务
基于数据库状态的简单缓存逻辑，不搞复杂的全局状态管理
"""
from typing import List, Dict, Tuple, Optional, Set
from datetime import date, datetime, timedelta, timezone
from functools import lru_cache
from bisect import bisect_left
from sqlalchemy import func
from app import db
from app.models.stock_price_history import StockPriceHistory
from app.models.market_holiday import MarketHoliday, StockHolidayAttempt
from app.models.stocks_cache import StocksCache
from app.models.market_holiday import MarketHoliday, StockHolidayAttempt
from app.services.stock_price_service import StockPriceService
import logging

logger = logging.getLogger(__name__)


class StockHistoryCacheService:
    """
    Simple stock history cache service.

    Design:
    1. DB is the single source of truth.
    2. Always decide to fetch based on DB state.
    3. Avoid global in-memory state.
    4. Simple flow: read DB -> find gaps -> fetch -> store.
    """

    def is_known_no_data(self, symbol: str, start_date: date, end_date: date, currency: str = 'USD') -> bool:
        """Return True if the date range is fully known to have no data."""
        if not symbol or start_date is None or end_date is None:
            return False
        if start_date > end_date:
            return False

        symbol = symbol.upper()
        currency = currency.upper()
        market = self._get_market(symbol, currency)
        effective_end = self._get_effective_end_for_expectation(symbol, currency, end_date)
        if start_date > effective_end:
            return True

        known_no_data_dates = self._get_known_no_data_dates(symbol, market, start_date, effective_end)
        holidays = MarketHoliday.holiday_set()

        current = start_date
        while current <= effective_end:
            if current.weekday() < 5 and (current, market) not in holidays and current not in known_no_data_dates:
                return False
            current += timedelta(days=1)

        return True

    # 网络查询 IPO 日期失败后的重试间隔（进程内共享）
    IPO_RETRY_SECONDS = 6 * 3600
    _ipo_lookup_failed_at: Dict[str, datetime] = {}

    def __init__(self):
        self.stock_service = StockPriceService()
        # 简单的防抖：避免短时间内重复外部请求同一标的
        self._recent_fetch_guard: Dict[tuple[str, str], datetime] = {}

    def get_cached_history(self, symbol: str, start_date: date, end_date: date,
                          currency: str = 'USD', force_refresh: bool = False) -> List[Dict]:
        """
        获取缓存的历史价格数据（主入口方法）
        保持与原接口兼容
        """
        return self.get_history(symbol, start_date, end_date, currency, force_refresh)

    def get_history(self, symbol: str, start_date: date, end_date: date,
                   currency: str = 'USD', force_refresh: bool = False) -> List[Dict]:
        """
        获取股票历史价格，先查数据库，缺失的才从API获取
        """
        symbol = symbol.upper()
        currency = currency.upper()

        # 调整到今天为止
        today = date.today()
        end_date = min(end_date, today)

        if start_date > end_date:
            return []

        # 固定价格证券（如投资储蓄账户）：不查库、不访问外部接口
        from app.utils.fixed_price import get_fixed_price, fixed_price_history
        if get_fixed_price(symbol, currency) is not None:
            return fixed_price_history(symbol, currency, start_date, end_date)

        # 1. 从数据库获取现有数据
        existing_data = self._get_from_database(symbol, start_date, end_date, currency)

        has_missing = self._has_missing_data(symbol, start_date, end_date, currency, existing_data)

        # 2. 如果无缺失且未强制刷新，直接返回缓存
        if not force_refresh and not has_missing:
            return existing_data

        # 3. 否则刷新缺失数据
        gaps = self._find_missing_gaps(symbol, start_date, end_date, currency, existing_data, force_refresh)

        if gaps:
            now = datetime.utcnow()
            guard_key = (symbol, currency)
            last_fetch = self._recent_fetch_guard.get(guard_key)
            # 10 分钟内已尝试过，不再打外部请求，避免频繁被封
            if not force_refresh and last_fetch and (now - last_fetch).total_seconds() < 600:
                logger.info(f"[cache][skip] {symbol}({currency}) 最近已刷新，跳过外部请求")
                return existing_data

            logger.info(f"发现 {symbol} 有 {len(gaps)} 个数据缺口，需要刷新")
            for gap_start, gap_end in gaps:
                refresh_message = (
                    f"[cache][fetch] {symbol}({currency}) {gap_start}->{gap_end} 请求最新历史价格"
                )
                logger.info(refresh_message)
                print(refresh_message)
                self._fetch_and_save(symbol, gap_start, gap_end, currency)

            self._recent_fetch_guard[guard_key] = now
            # 重新从数据库获取完整数据
            existing_data = self._get_from_database(symbol, start_date, end_date, currency)

        return existing_data

    def _get_from_database(self, symbol: str, start_date: date, end_date: date, currency: str) -> List[Dict]:
        """从数据库获取历史价格数据

        同一请求内某只股票被查询多次时（>= FULL_HISTORY_AFTER 次），一次性读取其全部历史，
        之后按日期区间在内存里切片；偶尔查一两次的仍按区间直接查库，避免无谓地读全量。
        每次返回新建的 dict，调用方修改不会影响记忆数据。
        """
        from app.utils.query_memo import bump, peek
        key = (symbol, currency)
        loaded = peek('price_history', key)
        if loaded is None and bump('price_history_hits', key) >= self.FULL_HISTORY_AFTER:
            loaded = self._load_symbol_history(symbol, currency)
        if loaded is not None:
            dates, rows, normalized = loaded
            lo = bisect_left(dates, start_date)
            hi = bisect_left(dates, end_date + timedelta(days=1))
            out = []
            for i in range(lo, hi):
                # 按需转换并记住：用到哪一行才转换哪一行，之后返回副本
                if normalized[i] is None:
                    normalized[i] = self._normalize_history_row(rows[i])
                out.append(dict(normalized[i]))
            return out
        rows = db.session.query(*self._HISTORY_COLUMNS).filter(
            StockPriceHistory.symbol == symbol,
            StockPriceHistory.currency == currency,
            StockPriceHistory.trade_date >= start_date,
            StockPriceHistory.trade_date <= end_date
        ).order_by(StockPriceHistory.trade_date.asc()).all()
        return [self._normalize_history_row(row) for row in rows]

    # 同一请求内同一只股票第几次查询起改为读取全部历史
    FULL_HISTORY_AFTER = 3

    _HISTORY_COLUMNS = (
        StockPriceHistory.id, StockPriceHistory.symbol, StockPriceHistory.currency,
        StockPriceHistory.trade_date, StockPriceHistory.open_price, StockPriceHistory.high_price,
        StockPriceHistory.low_price, StockPriceHistory.close_price, StockPriceHistory.adjusted_close,
        StockPriceHistory.volume, StockPriceHistory.data_source,
        StockPriceHistory.created_at, StockPriceHistory.updated_at,
    )

    def _load_symbol_history(self, symbol: str, currency: str):
        """(交易日列表, 原始行列表, 按需转换结果列表)，按交易日升序；同一请求内每个 (symbol, currency) 只查一次。"""
        from app.utils.query_memo import memo

        def _query():
            rows = db.session.query(*self._HISTORY_COLUMNS).filter(
                StockPriceHistory.symbol == symbol,
                StockPriceHistory.currency == currency,
            ).order_by(StockPriceHistory.trade_date.asc()).all()
            return [row.trade_date for row in rows], rows, [None] * len(rows)

        return memo('price_history', (symbol, currency), _query)

    @staticmethod
    def _normalize_history_row(record) -> Dict:
        def _to_float(value):
            return float(value) if value is not None else None

        trade_date = record.trade_date.isoformat() if record.trade_date else None
        return {
            'id': record.id,
            'symbol': record.symbol,
            'currency': record.currency,
            'trade_date': trade_date,
            'date': trade_date,
            'open_price': _to_float(record.open_price),
            'open': _to_float(record.open_price),
            'high_price': _to_float(record.high_price),
            'high': _to_float(record.high_price),
            'low_price': _to_float(record.low_price),
            'low': _to_float(record.low_price),
            'close_price': _to_float(record.close_price),
            'close': _to_float(record.close_price),
            'adjusted_close': _to_float(record.adjusted_close),
            'adj_close': _to_float(record.adjusted_close),
            'volume': record.volume,
            'data_source': record.data_source,
            'created_at': record.created_at.isoformat() if record.created_at else None,
            'updated_at': record.updated_at.isoformat() if record.updated_at else None,
        }

    def _has_missing_data(self, symbol: str, start_date: date, end_date: date,
                         currency: str, existing_data: List[Dict]) -> bool:
        """简单判断是否有缺失数据"""
        if not existing_data:
            return True

        # 获取IPO日期调整起始日期
        ipo_date = self._get_ipo_date(symbol, currency)
        if ipo_date and start_date < ipo_date:
            start_date = ipo_date

        effective_end = self._get_effective_end_for_expectation(symbol, currency, end_date)
        if start_date > effective_end:
            return False

        existing_dates = set()
        for record in existing_data:
            if isinstance(record.get('trade_date'), str):
                trade_date = date.fromisoformat(record['trade_date'])
            else:
                trade_date = record.get('trade_date')
            if trade_date:
                existing_dates.add(trade_date)

        # 精确预估交易日数量（排除周末 + 已知节假日）
        market = self._get_market(symbol, currency)
        known_no_data_dates = self._get_known_no_data_dates(symbol, market, start_date, effective_end)
        holidays = MarketHoliday.holiday_set()
        last_expected = effective_end
        while last_expected >= start_date:
            if (last_expected.weekday() < 5 and
                    (last_expected, market) not in holidays and
                    last_expected not in known_no_data_dates):
                break
            last_expected -= timedelta(days=1)

        if last_expected >= start_date and last_expected not in existing_dates:
            return True

        expected_trading_days = 0
        current = start_date
        while current <= effective_end:
            if (current.weekday() < 5 and
                    (current, market) not in holidays and
                    current not in known_no_data_dates):
                expected_trading_days += 1
            current += timedelta(days=1)

        if expected_trading_days == 0:
            return False

        # 对短区间放宽：允许缺少 1 天；长区间要求至少 85%
        if expected_trading_days <= 10:
            return len(existing_data) < max(0, expected_trading_days - 1)
        return len(existing_data) < expected_trading_days * 0.85

    def _find_missing_gaps(self, symbol: str, start_date: date, end_date: date,
                          currency: str, existing_data: List[Dict], force_refresh: bool = False) -> List[Tuple[date, date]]:
        """找出数据库中缺失的日期范围"""

        # 获取IPO日期，避免pre-IPO查询
        ipo_date = self._get_ipo_date(symbol, currency)
        if ipo_date and start_date < ipo_date:
            start_date = ipo_date
            logger.info(f"{symbol} IPO日期为 {ipo_date}，调整查询起始日期")

        effective_end = self._get_effective_end_for_expectation(symbol, currency, end_date)
        if start_date > effective_end:
            return []

        # 如果强制刷新，返回整个范围，但也要经过扩展逻辑
        if force_refresh:
            gaps = [(start_date, effective_end)]
            # 应用扩展逻辑
            gaps = self._expand_short_gaps_for_holiday_detection(gaps)
            return gaps

        # 获取现有数据的日期集合
        existing_dates = set()
        for record in existing_data:
            if isinstance(record['trade_date'], str):
                trade_date = date.fromisoformat(record['trade_date'])
            else:
                trade_date = record['trade_date']
            existing_dates.add(trade_date)

        # 获取市场信息用于节假日检查
        market = self._get_market(symbol, currency)
        known_no_data_dates = self._get_known_no_data_dates(symbol, market, start_date, effective_end)

        # 找出缺失的交易日范围，跳过已知节假日
        gaps = []
        current_date = start_date
        gap_start = None
        holidays = MarketHoliday.holiday_set()

        while current_date <= effective_end:
            # 跳过周末
            if current_date.weekday() >= 5:  # 5=Saturday, 6=Sunday
                current_date += timedelta(days=1)
                continue

            # 跳过已知节假日
            if (current_date, market) in holidays:
                current_date += timedelta(days=1)
                continue

            if current_date in known_no_data_dates:
                current_date += timedelta(days=1)
                continue

            if current_date not in existing_dates:
                if gap_start is None:
                    gap_start = current_date
            else:
                if gap_start is not None:
                    gaps.append((gap_start, current_date - timedelta(days=1)))
                    gap_start = None

            current_date += timedelta(days=1)

        # 处理最后一个缺口
        if gap_start is not None:
            gaps.append((gap_start, effective_end))

        # 智能扩展短期缺口以检测节假日
        gaps = self._expand_short_gaps_for_holiday_detection(gaps)

        if gaps:
            self._log_missing_dates(symbol, currency, market, gaps, existing_dates, known_no_data_dates)

        return gaps

    def _get_effective_end_for_expectation(self, symbol: str, currency: str, end_date: date) -> date:
        """在交易时段内不强制要求当天收盘数据，避免误判缺口。"""
        market = self._get_market(symbol, currency)
        now_utc = datetime.utcnow().replace(tzinfo=timezone.utc)
        market_tz = self.stock_service._get_market_timezone(market)
        local_now = now_utc.astimezone(market_tz)
        market_date = local_now.date()

        effective_end = min(end_date, market_date)
        if effective_end < market_date:
            return effective_end

        # 若今天非交易日，回退到最近交易日
        if market_date.weekday() >= 5 or MarketHoliday.is_holiday(market_date, market):
            effective_end = market_date - timedelta(days=1)
            while effective_end.weekday() >= 5 or MarketHoliday.is_holiday(effective_end, market):
                effective_end -= timedelta(days=1)
            return effective_end

        # 未到收盘（盘中/开盘前）：仍使用上一交易日
        market_close = local_now.replace(hour=16, minute=0, second=0, microsecond=0)
        if local_now < market_close:
            effective_end = market_date - timedelta(days=1)
            while effective_end.weekday() >= 5 or MarketHoliday.is_holiday(effective_end, market):
                effective_end -= timedelta(days=1)
            return effective_end

        return market_date

    def _get_known_no_data_dates(self, symbol: str, market: str, start_date: date, end_date: date) -> Set[date]:
        """Return dates recorded as no-data attempts for this symbol/market."""
        try:
            from app.utils.query_memo import memo
            # 同一请求内每个 (symbol, market) 只查一次全部“无数据”日期，再按区间过滤
            all_dates = memo('no_data_dates', (symbol, market), lambda: frozenset(
                row.attempt_date for row in db.session.query(StockHolidayAttempt.attempt_date).filter(
                    StockHolidayAttempt.symbol == symbol,
                    StockHolidayAttempt.market == market,
                    StockHolidayAttempt.has_data == False,
                ).all()
            ))
            return {d for d in all_dates if start_date <= d <= end_date}
        except Exception as exc:
            logger.debug(f"Failed to load no-data attempts for {symbol}: {exc}")
            return set()

    def _log_missing_dates(self, symbol: str, currency: str, market: str,
                           gaps: List[Tuple[date, date]], existing_dates: Set[date],
                           known_no_data_dates: Optional[Set[date]] = None) -> None:
        """输出缺失的具体交易日，便于调试"""
        today = date.today()
        for gap_start, gap_end in gaps:
            missing_dates = []
            current = gap_start
            while current <= gap_end and current <= today:
                if current.weekday() >= 5:
                    current += timedelta(days=1)
                    continue
                if MarketHoliday.is_holiday(current, market):
                    current += timedelta(days=1)
                    continue
                if current in known_no_data_dates:
                    current += timedelta(days=1)
                    continue
                if current not in existing_dates:
                    missing_dates.append(current)
                current += timedelta(days=1)

            if missing_dates:
                preview = ', '.join(d.isoformat() for d in missing_dates[:10])
                if len(missing_dates) > 10:
                    preview += ', ...'
                message = (
                    f"[cache][gap] {symbol}({currency}) {gap_start}->{gap_end} "
                    f"missing {len(missing_dates)} trading days: {preview}"
                )
            else:
                message = (
                    f"[cache][gap] {symbol}({currency}) {gap_start}->{gap_end} "
                    "缺失的都是周末或已知节假日"
                )
            logger.warning(message)
            print(message)

    def get_cache_statistics(self, symbol: Optional[str] = None, currency: Optional[str] = None) -> Dict:
        """返回 stock_price_history 缓存的基本统计信息"""
        query = StockPriceHistory.query

        if symbol:
            query = query.filter(StockPriceHistory.symbol == symbol.upper())
        if currency:
            query = query.filter(StockPriceHistory.currency == currency.upper())

        total_records = query.count()
        distinct_symbols = query.with_entities(StockPriceHistory.symbol).distinct().count()

        earliest_record = query.order_by(StockPriceHistory.trade_date.asc()).first()
        latest_record = query.order_by(StockPriceHistory.trade_date.desc()).first()
        latest_update = query.order_by(StockPriceHistory.updated_at.desc()).first()

        return {
            'symbol_filter': symbol.upper() if symbol else None,
            'currency_filter': currency.upper() if currency else None,
            'total_records': total_records,
            'distinct_symbols': distinct_symbols,
            'date_range': {
                'earliest': earliest_record.trade_date.isoformat() if earliest_record else None,
                'latest': latest_record.trade_date.isoformat() if latest_record else None
            },
            'last_updated_at': latest_update.updated_at.isoformat() if latest_update and latest_update.updated_at else None
        }

    def _get_ipo_date(self, symbol: str, currency: str) -> Optional[date]:
        """获取股票IPO日期"""
        stock = StocksCache.get_cached(symbol, currency)
        if stock and stock.first_trade_date:
            return stock.first_trade_date

        # 如果没有IPO日期，尝试从网络查询。
        # 查不到的股票在一段时间内不再重复查询（以前每次取价格都会访问一次网页）。
        if stock:
            failed_at = StockHistoryCacheService._ipo_lookup_failed_at.get(symbol)
            if failed_at and (datetime.utcnow() - failed_at).total_seconds() < self.IPO_RETRY_SECONDS:
                return None
            ipo_date = self._query_ipo_online(symbol)
            if ipo_date:
                StockHistoryCacheService._ipo_lookup_failed_at.pop(symbol, None)
                stock.first_trade_date = ipo_date
                db.session.commit()
                logger.info(f"网络查询设置 {symbol} IPO日期: {ipo_date}")
                return ipo_date
            StockHistoryCacheService._ipo_lookup_failed_at[symbol] = datetime.utcnow()

        return None

    def _query_ipo_online(self, symbol: str) -> Optional[date]:
        """从网络查询IPO日期"""
        try:
            import requests
            import re

            clean_symbol = symbol.replace('.TO', '').replace('.V', '')
            url = f"https://finance.yahoo.com/quote/{clean_symbol}"
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
            }

            response = requests.get(url, headers=headers, timeout=10)
            if response.status_code == 200:
                content = response.text

                # 多种IPO日期查找模式 - 按优先级排序
                patterns = [
                    # 最准确的IPO日期格式
                    r'"ipoDate"\s*:\s*"([^"]+)"',
                    r'"firstTradeDateEpochUtc"\s*:\s*(\d+)',
                    # 明确的IPO相关文本
                    r'IPO\s*[:\s]*(\d{4}-\d{2}-\d{2})',
                    r'IPO\s*[:\s]*(\d{4})',
                    # 避免公司成立年份等误导性信息
                ]

                for pattern in patterns:
                    matches = re.findall(pattern, content, re.IGNORECASE)
                    for match in matches:
                        try:
                            # 处理时间戳格式
                            if match.isdigit() and len(match) >= 10:
                                timestamp = int(match)
                                ipo_date = datetime.fromtimestamp(timestamp).date()
                                # 验证IPO日期的合理性（不能太早，不能是未来）
                                if ipo_date >= date(1990, 1, 1) and ipo_date <= date.today():
                                    logger.info(f"从时间戳获取 {symbol} IPO日期: {ipo_date}")
                                    return ipo_date

                            # 处理完整日期格式 (YYYY-MM-DD)
                            elif re.match(r'\d{4}-\d{2}-\d{2}', match):
                                ipo_date = datetime.strptime(match, '%Y-%m-%d').date()
                                if ipo_date >= date(1990, 1, 1) and ipo_date <= date.today():
                                    logger.info(f"从完整日期获取 {symbol} IPO日期: {ipo_date}")
                                    return ipo_date

                            # 处理年份格式 - 但需要更严格的验证
                            elif match.isdigit() and len(match) == 4:
                                year = int(match)
                                # 更严格的年份验证：IPO年份应该在合理范围内
                                current_year = date.today().year
                                if 1990 <= year <= current_year and year >= current_year - 30:  # 最近30年内
                                    # 对于年份，设置为该年的6月1日（年中），而不是1月1日
                                    ipo_date = date(year, 6, 1)
                                    logger.info(f"从年份获取 {symbol} IPO日期: {ipo_date}")
                                    return ipo_date

                        except (ValueError, OverflowError):
                            continue

            return None
        except Exception as e:
            logger.debug(f"网络查询 {symbol} IPO日期失败: {str(e)}")
            return None

    def _expand_short_gaps_for_holiday_detection(self, gaps: List[Tuple[date, date]]) -> List[Tuple[date, date]]:
        """扩展短期缺口以进行节假日检测和缓存优化"""
        expanded_gaps = []
        today = date.today()

        for gap_start, gap_end in gaps:
            gap_days = (gap_end - gap_start).days + 1

            # 缓存优化：如果缺口小于一个月（30天），自动扩大获取区间
            if gap_days <= 30:
                # 前后各增加7天，避免一次性大窗口被限流
                expanded_start = gap_start - timedelta(days=7)
                expanded_end = gap_end + timedelta(days=7)
                
                # 确保扩展范围不超过今天
                expanded_end = min(expanded_end, today)
                
                # 确保起始日期不早于IPO日期（如果有的话）
                # 这里不检查IPO日期，因为调用方已经处理了IPO日期调整
                
                logger.info(f"🚀 缓存优化：扩展短期缺口 {gap_start}->{gap_end} ({gap_days}天) 为 {expanded_start}->{expanded_end} 以减少API调用")
                expanded_gaps.append((expanded_start, expanded_end))
            else:
                # 较长的缺口不扩展，直接使用原区间
                expanded_gaps.append((gap_start, gap_end))

        return expanded_gaps

    def _fetch_and_save(self, symbol: str, start_date: date, end_date: date, currency: str):
        """从Yahoo Finance获取数据并保存到数据库"""
        try:
            logger.info(f"[Yahoo Fetch] {symbol}({currency}) {start_date} -> {end_date}")

            raw_data, response_info = self.stock_service.get_stock_history(symbol, start_date, end_date, currency)
            market = self._get_market(symbol, currency)

            request_error = response_info.get('error') if isinstance(response_info, dict) else None

            if raw_data and 'timestamp' in raw_data and len(raw_data['timestamp']) > 0:
                # 先登记拆股，再还原实际价格：区间内的拆股看 Stock Splits 标记；
                # 区间在今天之前结束时，区间之后的拆股另外向 Yahoo 查一次拆股列表
                added = self._record_splits_from_raw(symbol, currency, raw_data)
                if end_date < date.today():
                    added += self._sync_splits_from_yahoo(symbol, currency)
                if added:
                    # 新登记的拆股早于已存的历史价格：那些价格可能是拆股调整后的，整段重拉并还原
                    self._refresh_if_history_predates(symbol, currency, max(added))
                processed_data = self._process_raw_data(symbol, raw_data, currency)
                if processed_data:
                    success = StockPriceHistory.bulk_upsert(processed_data)
                    if success:
                        logger.info(f"✅ 成功保存 {symbol} {len(processed_data)} 条价格记录")

                        # 分析获取的数据，识别可能的节假日
                        self._analyze_data_for_holidays(symbol, market, start_date, end_date, processed_data)
                    else:
                        logger.error(f"❌ 保存 {symbol} 价格数据失败")
                else:
                    logger.warning(f"⚠️ {symbol} 原始数据处理后为空")
            else:
                logger.warning(f"⚠️ {symbol} 在 {start_date}->{end_date} 期间无数据")
                if request_error:
                    logger.warning(
                        f"跳过节假日检测：{symbol} {start_date}->{end_date} 请求失败 ({request_error})"
                    )

        except Exception as e:
            logger.error(f"❌ 获取 {symbol} 价格数据失败: {str(e)}")

    def _analyze_data_for_holidays(self, symbol: str, market: str, start_date: date, end_date: date, processed_data: List[Dict]):
        """分析获取的数据，识别可能的节假日

        正确逻辑：只有当前后一个月都有数据，中间某天无数据时，才认为是节假日
        """
        try:
            # 获取所有数据的日期集合
            data_dates = set()
            for record in processed_data:
                data_dates.add(record['trade_date'])

            if not data_dates:
                return

            # 按日期排序
            sorted_dates = sorted(data_dates)
            earliest_date = sorted_dates[0]
            latest_date = sorted_dates[-1]

            missing_dates = self._get_missing_trading_days(data_dates, start_date, end_date)

            for missing_date in missing_dates:
                StockHolidayAttempt.record_attempt(symbol, market, missing_date, has_data=False)
                logger.info(f"🔍 {symbol} 在 {missing_date} 无数据，但前后有数据，可能是节假日")

                if StockHolidayAttempt.should_promote_to_holiday(missing_date, market, threshold=5):
                    MarketHoliday.add_holiday_detection(missing_date, market, symbol)
                    logger.info(f"🎉 检测到节假日: {missing_date} ({market}市场)")

            # 标记已有数据的日期
            for existing_date in data_dates:
                existing_attempt = StockHolidayAttempt.query.filter_by(
                    symbol=symbol,
                    market=market,
                    attempt_date=existing_date
                ).first()
                if existing_attempt and not existing_attempt.has_data:
                    StockHolidayAttempt.record_attempt(symbol, market, existing_date, has_data=True)

        except Exception as e:
            logger.error(f"分析节假日数据失败: {str(e)}")

    def _get_missing_trading_days(self, data_dates: Set[date], start_date: date, end_date: date) -> List[date]:
        """根据已有数据集合计算潜在缺失的交易日"""
        if not data_dates:
            return []

        total_days = max(1, (end_date - start_date).days)
        lookback_days = min(30, max(5, total_days // 4 if total_days >= 20 else total_days // 2))
        lookback_window = timedelta(days=lookback_days)

        analysis_start = start_date + lookback_window
        analysis_end = end_date - lookback_window

        if analysis_start > analysis_end:
            analysis_start = start_date
            analysis_end = end_date

        missing_dates: List[date] = []
        data_dates_set = set(data_dates)
        sorted_dates = sorted(data_dates_set)
        today = date.today()
        current_date = analysis_start

        while current_date <= analysis_end and current_date <= today:
            if current_date.weekday() < 5 and current_date not in data_dates_set:
                idx = bisect_left(sorted_dates, current_date)
                prev_date = sorted_dates[idx - 1] if idx - 1 >= 0 else None
                next_date = sorted_dates[idx] if idx < len(sorted_dates) else None

                if prev_date and next_date:
                    if (current_date - prev_date).days <= lookback_days and (next_date - current_date).days <= lookback_days:
                        missing_dates.append(current_date)

            current_date += timedelta(days=1)

        return missing_dates

    def _record_splits_from_raw(self, symbol: str, currency: str, raw_data: Dict) -> List[date]:
        """登记 Yahoo 返回的拆股标记（已登记的不重复）。返回新登记的拆股日。"""
        from app.services.split_service import parse_yahoo_splits, record_split
        added: List[date] = []
        for split_date, ratio in parse_yahoo_splits(raw_data.get('splits') or []):
            if record_split(symbol, currency, split_date, ratio, source='yahoo'):
                added.append(split_date)
                logger.warning(f"[split] 登记 {symbol}({currency}) {split_date} 拆股比例 {ratio}")
        if added:
            db.session.commit()
        return added

    def _refresh_if_history_predates(self, symbol: str, currency: str, split_date: date) -> None:
        has_earlier = db.session.query(StockPriceHistory.id).filter(
            StockPriceHistory.symbol == symbol.upper(), StockPriceHistory.currency == currency.upper(),
            StockPriceHistory.trade_date < split_date).first()
        if has_earlier:
            count = self.refresh_full_history(symbol, currency)
            logger.warning(f"[split] {symbol}({currency}) 重拉整段历史并还原实际价格：{count} 行")

    def refresh_full_history(self, symbol: str, currency: str) -> int:
        """重新拉取该股票数据库里已有的整段历史（最早一天到今天），按拆股表还原后覆盖保存。
        用于发现历史价格是“拆股调整后”的情况。返回保存的行数。"""
        symbol, currency = symbol.upper(), currency.upper()
        earliest = db.session.query(func.min(StockPriceHistory.trade_date)).filter(
            StockPriceHistory.symbol == symbol, StockPriceHistory.currency == currency).scalar()
        if earliest is None:
            return 0
        raw_data, _info = self.stock_service.get_stock_history(symbol, earliest, date.today(), currency)
        if not raw_data or not raw_data.get('timestamp'):
            return 0
        self._record_splits_from_raw(symbol, currency, raw_data)
        processed = self._process_raw_data(symbol, raw_data, currency)
        if not processed or not StockPriceHistory.bulk_upsert(processed):
            return 0
        # 以前存下的无效收盘价（空或 <= 0，如拆股当天）删除，交给缺口逻辑以后重新获取
        StockPriceHistory.query.filter(
            StockPriceHistory.symbol == symbol, StockPriceHistory.currency == currency,
            db.or_(StockPriceHistory.close_price.is_(None), StockPriceHistory.close_price <= 0)
        ).delete(synchronize_session=False)
        db.session.commit()
        return len(processed)

    # 每个 (股票, 币种) 每个进程每天最多查一次 Yahoo 拆股列表
    _split_sync_guard: Dict[Tuple[str, str], date] = {}

    def _sync_splits_from_yahoo(self, symbol: str, currency: str) -> List[date]:
        """向 Yahoo 查该股票的全部拆股并登记；返回新登记的拆股日。"""
        key = (symbol.upper(), currency.upper())
        if self._split_sync_guard.get(key) == date.today():
            return []
        self._split_sync_guard[key] = date.today()
        splits = self.stock_service.get_stock_splits(symbol)
        if splits is None:
            return []
        return self._record_splits_from_raw(symbol, currency, {'splits': splits})

    def sync_splits_and_repair(self, symbol: str, currency: str) -> List[date]:
        """登记该股票在 Yahoo 上的全部拆股；有新登记且早于已存历史价格时整段重拉还原。"""
        added = self._sync_splits_from_yahoo(symbol, currency)
        if added:
            self._refresh_if_history_predates(symbol, currency, max(added))
        return added

    def _process_raw_data(self, symbol: str, raw_data: Dict, currency: str) -> List[Dict]:
        """处理Yahoo Finance返回的原始数据：跳过没有有效收盘价的行；
        Yahoo 价格按此后的拆股调整过，按拆股表还原成当天的实际成交价。"""
        from app.services.split_service import to_actual
        processed_data = []

        timestamps = raw_data.get('timestamp', [])
        opens = raw_data.get('open', [])
        highs = raw_data.get('high', [])
        lows = raw_data.get('low', [])
        closes = raw_data.get('close', [])
        volumes = raw_data.get('volume', [])

        for i in range(len(timestamps)):
            try:
                trade_date = datetime.fromtimestamp(timestamps[i]).date()

                # 跳过周末
                if trade_date.weekday() >= 5:
                    continue

                # 没有有效收盘价（如拆股当天 Yahoo 只返回拆股标记）：不保存，避免把价格存成 0
                close = closes[i] if i < len(closes) else None
                if close is None or close != close or close <= 0:
                    continue

                def actual(values):
                    value = values[i] if i < len(values) and values[i] is not None else None
                    return to_actual(value, symbol, currency, trade_date) if value is not None else 0

                processed_data.append({
                    'symbol': symbol.upper(),
                    'currency': currency.upper(),
                    'trade_date': trade_date,
                    'open_price': actual(opens),
                    'high_price': actual(highs),
                    'low_price': actual(lows),
                    'close_price': to_actual(close, symbol, currency, trade_date),
                    'volume': volumes[i] if i < len(volumes) and volumes[i] is not None else 0,
                    'updated_at': datetime.utcnow()
                })
            except Exception as e:
                logger.warning(f"处理 {symbol} 第{i}条数据失败: {str(e)}")
                continue

        return processed_data

    # 兼容性方法：市场识别和节假日处理
    def _get_market(self, symbol: str, currency: str) -> str:
        """识别股票所属市场"""
        symbol = (symbol or '').upper()
        currency = (currency or '').upper()

        tsx_suffixes = ('.TO', '.TSX', '.TSXV', '.V', '.CN', '-T')
        if any(symbol.endswith(suffix) for suffix in tsx_suffixes):
            return 'CA'
        if currency == 'CAD':
            return 'CA'
        return 'US'

    def _is_market_holiday_by_market(self, market: str, target_date: date) -> bool:
        """判断指定市场在某天是否休市"""
        if not target_date:
            return False

        normalized_market = (market or 'US').upper()

        # 先检查数据库中是否记录为节假日
        if MarketHoliday.is_holiday(target_date, normalized_market):
            return True

        # 回退到内置节假日表
        if normalized_market == 'CA':
            return target_date in self._get_canadian_holidays(target_date.year)
        return target_date in self._get_us_holidays(target_date.year)

    @staticmethod
    @lru_cache(maxsize=16)
    def _get_us_holidays(year: int) -> Set[date]:
        """获取指定年份的美国主要市场休市日"""
        holidays: Set[date] = set()

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

        # 固定休市日
        holidays.add(observed(date(year, 1, 1)))  # New Year's Day
        holidays.add(nth_weekday(1, 0, 3))  # Martin Luther King Jr. Day (3rd Monday Jan)
        holidays.add(nth_weekday(2, 0, 3))  # Presidents' Day (3rd Monday Feb)
        holidays.add(last_weekday(5, 0))  # Memorial Day (last Monday May)
        holidays.add(observed(date(year, 7, 4)))  # Independence Day
        holidays.add(nth_weekday(9, 0, 1))  # Labor Day (1st Monday Sep)
        holidays.add(nth_weekday(11, 3, 4))  # Thanksgiving (4th Thursday Nov)
        holidays.add(observed(date(year, 12, 25)))  # Christmas Day

        # 变动性休市日
        try:
            from dateutil.easter import easter
            good_friday = easter(year) - timedelta(days=2)
            holidays.add(good_friday)
        except Exception:
            pass

        return holidays

    @staticmethod
    @lru_cache(maxsize=16)
    def _get_canadian_holidays(year: int) -> Set[date]:
        """获取指定年份的加拿大主要市场休市日"""
        holidays: Set[date] = set()

        def observed(day: date) -> date:
            if day.weekday() == 5:
                return day - timedelta(days=1)
            if day.weekday() == 6:
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

        # 固定休市日
        holidays.add(observed(date(year, 1, 1)))  # New Year's Day
        holidays.add(nth_weekday(2, 0, 3))  # Family Day (3rd Monday Feb)
        holidays.add(observed(date(year, 7, 1)))  # Canada Day
        holidays.add(nth_weekday(8, 0, 1))  # Civic Holiday (1st Monday Aug)
        holidays.add(nth_weekday(10, 0, 2))  # Thanksgiving (2nd Monday Oct)
        holidays.add(observed(date(year, 12, 25)))  # Christmas Day
        holidays.add(observed(date(year, 12, 26)))  # Boxing Day
        holidays.add(last_weekday(5, 0))  # Victoria Day (last Monday May)
        holidays.add(nth_weekday(9, 0, 1))  # Labour Day (1st Monday Sep)

        # Good Friday
        try:
            from dateutil.easter import easter
            holidays.add(easter(year) - timedelta(days=2))
        except Exception:
            pass

        return holidays

    # 为了保持兼容性，保留一些可能被调用的方法
    def cleanup_old_cache_data(self, days_to_keep: int = 365):
        """清理旧的缓存数据（保持兼容性）"""
        try:
            cutoff_date = date.today() - timedelta(days=days_to_keep)
            deleted_count = StockPriceHistory.query.filter(
                StockPriceHistory.updated_at < cutoff_date
            ).delete()

            from app import db
            db.session.commit()
            logger.info(f"清理了 {deleted_count} 条旧缓存记录")

            return {
                'success': True,
                'deleted_count': deleted_count,
                'cutoff_date': cutoff_date.isoformat()
            }

        except Exception as e:
            logger.error(f"清理缓存失败: {str(e)}")
            return {
                'success': False,
                'error': str(e),
                'deleted_count': 0
            }
