"""
股票价格获取和缓存服务 - 使用Yahoo Finance
"""

import logging
import re
import time
import yfinance as yf
import pandas as pd
from datetime import datetime, timedelta, timezone, date
from typing import Dict, List, Optional, Tuple
from decimal import Decimal
from flask import current_app
from zoneinfo import ZoneInfo

from app import db
from app.models.stocks_cache import StocksCache

logger = logging.getLogger(__name__)


def _log_yfinance_call(api_name: str, symbol: str, **kwargs):
    """Optional debug hook for yfinance calls; disabled by default to reduce noise."""
    try:
        enabled = bool(current_app.config.get('ENABLE_YFINANCE_DEBUG', False))
    except Exception:
        enabled = False

    if not enabled:
        return

    details = " ".join(f"{k}={v}" for k, v in kwargs.items() if v is not None)
    message = f"[yfinance] {api_name} symbol={symbol} {details}".strip()
    logger.debug(message)

logger = logging.getLogger(__name__)


class StockPriceService:
    """股票价格服务 - 使用Yahoo Finance"""
    
    def __init__(self):
        pass

    def _get_price_cache_ttl_seconds(self) -> int:
        try:
            return int(current_app.config.get('PRICE_CACHE_TTL', 900))
        except Exception:
            return 900

    def _get_market_timezone(self, market: str) -> ZoneInfo:
        # US/CA equity markets use America/New_York for trading hours
        return ZoneInfo("America/New_York")

    def _is_market_open(self, now_utc: datetime, market: str) -> bool:
        if not isinstance(now_utc, datetime):
            now_utc = datetime.utcnow().replace(tzinfo=timezone.utc)
        if now_utc.tzinfo is None:
            now_utc = now_utc.replace(tzinfo=timezone.utc)

        market_tz = self._get_market_timezone(market)
        local_time = now_utc.astimezone(market_tz)

        if local_time.weekday() >= 5:
            return False

        # Check market holidays
        try:
            from app.models.market_holiday import MarketHoliday
            mkt_key = 'CA' if market == 'TSX' else 'US'
            if MarketHoliday.query.filter_by(holiday_date=local_time.date(), market=mkt_key).first():
                return False
        except Exception:
            pass

        # Regular market hours: 9:30 - 16:00 ET
        if local_time.hour < 9 or local_time.hour > 16:
            return False
        if local_time.hour == 9 and local_time.minute < 30:
            return False
        if local_time.hour == 16 and local_time.minute > 0:
            return False

        return True

    def _should_refresh_price(self, stock: Optional[StocksCache], market: str, *, force_refresh: bool = False) -> bool:
        if force_refresh:
            return True
        if not stock:
            return True
        # Allow a one-time refresh for new symbols even outside trading hours.
        if not stock.price_updated_at:
            return stock.current_price is None

        now_utc = datetime.utcnow().replace(tzinfo=timezone.utc)
        updated_at = stock.price_updated_at
        if updated_at.tzinfo is None:
            updated_at = updated_at.replace(tzinfo=timezone.utc)

        market_tz = self._get_market_timezone(market)
        now_local = now_utc.astimezone(market_tz)
        updated_local = updated_at.astimezone(market_tz)

        if self._is_market_open(now_utc, market):
            ttl_seconds = self._get_price_cache_ttl_seconds()
            return (now_utc - updated_at).total_seconds() > ttl_seconds

        # Market closed:
        # - Never auto-refresh on weekends.
        # - On weekdays, allow a single refresh after the market close (>= 16:00 ET)
        #   unless we've already refreshed today after close.
        if now_local.weekday() >= 5:
            return False

        market_close_time = now_local.replace(hour=16, minute=0, second=0, microsecond=0)
        if now_local < market_close_time:
            return False

        updated_after_close_today = (
            updated_local.date() == now_local.date() and updated_local >= market_close_time
        )
        return not updated_after_close_today
    
    def _extract_first_trade_date(self, info: Dict) -> Optional[date]:
        """从yfinance返回的信息中提取IPO/首个交易日期"""
        first_trade_date = None

        first_trade_epoch = info.get('firstTradeDateEpochUtc') or info.get('firstTradeDate')
        if first_trade_epoch:
            try:
                first_trade_date = datetime.fromtimestamp(int(first_trade_epoch), tz=timezone.utc).date()
            except (TypeError, ValueError, OSError, OverflowError):
                logger.debug("Unable to parse firstTradeDateEpochUtc=%s", first_trade_epoch)

        if not first_trade_date:
            ipo_value = info.get('ipoDate') or info.get('ipo_date') or info.get('ipo')
            if isinstance(ipo_value, str):
                cleaned_value = ipo_value.strip()
                parse_formats = ("%Y-%m-%d", "%Y/%m/%d", "%Y%m%d")
                for fmt in parse_formats:
                    try:
                        first_trade_date = datetime.strptime(cleaned_value, fmt).date()
                        break
                    except ValueError:
                        continue
                if not first_trade_date:
                    if len(cleaned_value) == 4 and cleaned_value.isdigit():
                        try:
                            first_trade_date = date(int(cleaned_value), 1, 1)
                        except ValueError:
                            pass
            elif isinstance(ipo_value, (int, float)):
                try:
                    first_trade_date = datetime.fromtimestamp(int(ipo_value), tz=timezone.utc).date()
                except (TypeError, ValueError, OSError, OverflowError):
                    logger.debug("Unable to parse IPO date timestamp=%s", ipo_value)

        return first_trade_date
    
    def get_stock_price(self, symbol: str, expected_currency: str = None) -> Optional[Dict]:
        """使用yfinance获取股票当前价格，并验证货币匹配

        Args:
            symbol: 股票代码
            expected_currency: 期望的货币代码 (USD/CAD)，如果提供则验证货币匹配

        Returns:
            Dict: 股票价格数据，如果货币不匹配则返回None
        """
        try:
            # 使用yfinance获取股票信息
            _log_yfinance_call('Ticker.info', symbol, expected_currency=expected_currency)
            ticker = yf.Ticker(symbol)
            info = ticker.info

            # 获取当前价格
            current_price = info.get('regularMarketPrice') or info.get('currentPrice')

            if current_price is None:
                return None

            # 获取货币信息
            yahoo_currency = info.get('currency', 'USD').upper()

            # 获取IPO日期/首个交易日
            first_trade_date = self._extract_first_trade_date(info)

            # 如果指定了期望的货币，进行验证
            if expected_currency:
                expected_currency = expected_currency.upper()
                if yahoo_currency != expected_currency:
                    logger.warning(f"货币不匹配: {symbol} 期望{expected_currency}，但Yahoo Finance返回{yahoo_currency}")
                    return None

            return {
                'symbol': symbol,
                'price': float(current_price),
                'currency': yahoo_currency,
                'exchange': info.get('exchange', ''),
                'name': info.get('longName', ''),
                'updated_at': datetime.utcnow(),
                'first_trade_date': first_trade_date
            }

        except Exception as e:
            logger.error(f"获取{symbol}价格失败: {str(e)}")
            return None
    
    def update_stock_price(self, symbol: str, currency: str, force_refresh: bool = False) -> bool:
        """更新数据库中的股票价格

        Args:
            symbol: 股票代码
            currency: 货币类型
            force_refresh: 是否强制刷新价格，忽略缓存时间限制
        """
        # 验证符号不为空
        if not symbol or not symbol.strip():
            logger.warning(f"无效的股票代码：'{symbol}'，跳过更新")
            return False
            
       

        try:
            # 使用联合主键查询
            stock = StocksCache.query.filter_by(symbol=symbol, currency=currency).first()

            market = self._get_market(symbol, currency)
            needs_update = self._should_refresh_price(stock, market, force_refresh=force_refresh)

            # 如果不需要更新，直接返回成功
            if not needs_update:
                return True

            # 不传入期望货币进行验证，让Yahoo Finance返回实际货币信息
            price_data = self.get_stock_price(symbol)

            created_new = False
            if not stock:
                # 如果stocks_cache中没有该股票，创建新记录，使用指定的currency
                stock = StocksCache(symbol=symbol, currency=currency)
                db.session.add(stock)
                created_new = True

            if price_data:
                yahoo_currency = price_data.get('currency', 'USD').upper()
                if yahoo_currency != currency.upper():
                    logger.warning(
                        f"货币不匹配: {symbol} Yahoo Finance返回{yahoo_currency}，保留缓存中的{currency}"
                    )
                    db.session.rollback()
                    return False

                stock.current_price = Decimal(str(price_data['price']))
                stock.price_updated_at = datetime.utcnow()

                if price_data.get('name') and not stock.name:
                    stock.name = price_data['name']
                if price_data.get('exchange') and not stock.exchange:
                    stock.exchange = price_data['exchange']
                if price_data.get('first_trade_date'):
                    stock.first_trade_date = price_data['first_trade_date']

                db.session.commit()
                return True

            # 请求失败，恢复到之前的状态，避免将价格置零
            db.session.rollback()
            if created_new:
                logger.warning(f"无法获取{symbol}({currency})价格，未能建立缓存记录")
            else:
                logger.warning(f"无法获取{symbol}({currency})价格，保持现有缓存")
            return False

        except Exception as e:
            logger.error(f"更新{symbol}({currency})价格失败: {str(e)}")
            db.session.rollback()
            return False
    
    def update_all_stock_prices(self) -> Dict:
        """更新所有股票价格"""
        # 只选择有效的股票符号（非空且不为空白）
        stocks = StocksCache.query.filter(
            StocksCache.symbol.isnot(None),
            StocksCache.symbol != '',
            StocksCache.symbol.notlike('')
        ).all()
        results = {
            'total': len(stocks),
            'updated': 0,
            'failed': 0,
            'errors': []
        }
        
        for stock in stocks:
            try:
                if self.update_stock_price(stock.symbol, stock.currency):
                    results['updated'] += 1
                else:
                    results['failed'] += 1
                    results['errors'].append(f"{stock.symbol}({stock.currency}): 获取价格失败")
            except Exception as e:
                results['failed'] += 1
                results['errors'].append(f"{stock.symbol}({stock.currency}): {str(e)}")
        
        return results
    
    def update_prices_for_symbols(self, symbol_currency_pairs: List[tuple], force_refresh: bool = False) -> Dict:
        """更新指定股票列表的价格

        Args:
            symbol_currency_pairs: 包含(symbol, currency)元组的列表
            force_refresh: 是否强制刷新价格，忽略缓存时间限制
        """
        # 去重处理，避免重复更新同一只股票
        unique_pairs = list(set(symbol_currency_pairs))
        
        results = {
            'total': len(unique_pairs),
            'updated': 0,
            'failed': 0,
            'errors': []
        }

        for symbol, currency in unique_pairs:
            try:
                if self.update_stock_price(symbol, currency, force_refresh):
                    results['updated'] += 1
                else:
                    results['failed'] += 1
                    results['errors'].append(f"{symbol}({currency}): 获取价格失败")
            except Exception as e:
                results['failed'] += 1
                results['errors'].append(f"{symbol}({currency}): {str(e)}")
        
        return results
    
    def get_cached_stock_price(self, symbol: str, currency: str, *, auto_refresh: bool = False) -> Decimal:
        """
        获取股票当前价格 - 带15分钟缓存过期检查和自动更新
        参数:
            symbol: 股票代码
            currency: 股票货币（必需，用于联合主键查询）
            auto_refresh: 是否在缓存过期时自动调用外部接口刷新
        返回:
            Decimal: 股票当前价格，如果无法获取则返回0
        """
        # 验证符号不为空
        if not symbol or not symbol.strip():
            logger.warning(f"无效的股票代码：'{symbol}'，返回0价格")
            return Decimal('0')

            
        try:
            # 从缓存获取 - 使用联合主键(symbol, currency)
            stock_cache = StocksCache.query.filter_by(symbol=symbol, currency=currency).first()
            
            market = self._get_market(symbol, currency)
            needs_update = self._should_refresh_price(stock_cache, market, force_refresh=False)
            
            # 如果需要更新，从Yahoo Finance获取最新价格
            if needs_update and auto_refresh:
                self.update_stock_price(symbol, currency)
                # 重新查询更新后的数据 - 使用联合主键
                stock_cache = StocksCache.query.filter_by(symbol=symbol, currency=currency).first()
            
            # 返回价格（可能是0如果无法从Yahoo获取）
            if stock_cache and stock_cache.current_price:
                return Decimal(str(stock_cache.current_price))
            
            return Decimal('0')
            
        except Exception as e:
            logger.error(f"获取{symbol}缓存价格失败: {str(e)}")
            return Decimal('0')
    
    def get_stock_history(self, symbol: str, start_date, end_date, currency: str = None) -> Tuple[Dict, Dict]:
        """
        使用 yfinance 获取股票历史价格数据
        参数:
            symbol: 股票代码
            start_date: 开始日期
            end_date: 结束日期
        返回:
            Tuple[Dict, Dict]: (历史价格字典, 响应信息字典)
        """
        info: Dict = {
            'requested_start': start_date,
            'requested_end': end_date
        }

        try:
            effective_end = min(end_date, date.today())
            effective_start = start_date

            if effective_start > effective_end:
                info['error'] = 'invalid_range'
                return {}, info

            # 使用 yfinance 获取历史数据
            _log_yfinance_call('Ticker.history', symbol, start=effective_start, end=effective_end)
            ticker = yf.Ticker(symbol)

            # 获取历史数据，加1天确保包含结束日期
            hist = ticker.history(
                start=effective_start,
                end=effective_end + timedelta(days=1),
                interval='1d',
                actions=False,
                auto_adjust=False,
                back_adjust=False
            )

            if hist.empty:
                # Fallback: use yf.download which is sometimes more reliable
                try:
                    _log_yfinance_call('yf.download', symbol, start=effective_start, end=effective_end)
                    hist = yf.download(
                        symbol,
                        start=effective_start,
                        end=effective_end + timedelta(days=1),
                        interval='1d',
                        progress=False,
                        auto_adjust=False
                    )
                    if not hist.empty and isinstance(hist.columns, pd.MultiIndex):
                        hist = hist.copy()
                        hist.columns = hist.columns.get_level_values(0)
                except Exception:
                    hist = pd.DataFrame()

            if hist.empty:
                message = f"yfinance 未返回 {symbol} 的历史数据 ({effective_start} -> {effective_end})"
                logger.warning(message)
                info['error'] = 'empty_data'
                
                # 记录假期尝试 - 当yfinance返回空数据时，记录整个时间段为无数据
                self._record_holiday_attempts_for_empty_data(symbol, effective_start, effective_end, currency)
                
                return {}, info

            # 转换为原有格式
            result_data = {}
            timestamps = []
            opens = []
            highs = []
            lows = []
            closes = []
            volumes = []

            for idx, row in hist.iterrows():
                try:
                    # 获取日期
                    if hasattr(idx, 'date'):
                        trade_date = idx.date()
                    else:
                        trade_date = idx.to_pydatetime().date()

                    # 转换为时间戳
                    timestamp = int(datetime.combine(trade_date, datetime.min.time()).timestamp())

                    timestamps.append(timestamp)
                    opens.append(float(row['Open']) if not pd.isna(row['Open']) else None)
                    highs.append(float(row['High']) if not pd.isna(row['High']) else None)
                    lows.append(float(row['Low']) if not pd.isna(row['Low']) else None)
                    closes.append(float(row['Close']) if not pd.isna(row['Close']) else None)
                    volumes.append(int(row['Volume']) if not pd.isna(row['Volume']) else 0)

                except Exception as e:
                    logger.debug(f"处理 {symbol} 日期 {idx} 的数据时出错: {str(e)}")
                    continue

            # 构建返回数据，保持与原有接口一致
            result_data = {
                'timestamp': timestamps,
                'open': opens,
                'high': highs,
                'low': lows,
                'close': closes,
                'volume': volumes
            }

            if timestamps:
                # 添加数据范围信息
                data_start = datetime.fromtimestamp(min(timestamps)).date()
                data_end = datetime.fromtimestamp(max(timestamps)).date()
                info['data_start_date'] = data_start
                info['data_end_date'] = data_end
                info['status_code'] = 200
                record_count = len(timestamps)
                success_message = (
                    f"[yfinance][success] Ticker.history symbol={symbol} start={start_date} "
                    f"end={end_date} rows={record_count} range={data_start}->{data_end}"
                )
                logger.info(success_message)
            else:
                warning_message = (
                    f"⚠️ yfinance 返回空数据 {symbol} ({start_date} -> {end_date})"
                )
                logger.warning(warning_message)
                info['error'] = 'empty_data'

            return result_data, info

        except Exception as e:
            logger.error(f"❌ yfinance 获取 {symbol} 历史价格失败 ({start_date} -> {end_date}): {str(e)}")
            info['error'] = str(e)
            return {}, info

    def _record_holiday_attempts_for_empty_data(self, symbol: str, start_date, end_date, currency: str = None):
        """当yfinance返回空数据时，记录假期尝试"""
        try:
            from app.models.market_holiday import StockHolidayAttempt
            
            # 获取市场信息
            market = self._get_market(symbol, currency)
            
            # 遍历请求的日期范围，记录每个交易日为无数据
            current_date = start_date
            while current_date <= end_date:
                # 跳过周末
                if current_date.weekday() < 5:  # 0-4 是周一到周五
                    StockHolidayAttempt.record_attempt(symbol, market, current_date, has_data=False)
                    logger.info(f"🔍 {symbol} 在 {current_date} 无数据，记录假期尝试 ({market}市场)")
                    
                    # 检查是否应该推广为节假日
                    if StockHolidayAttempt.should_promote_to_holiday(current_date, market, threshold=5):
                        from app.models.market_holiday import MarketHoliday
                        MarketHoliday.add_holiday_detection(current_date, market, symbol)
                        logger.info(f"🎉 检测到节假日: {current_date} ({market}市场)")
                
                current_date += timedelta(days=1)
                
        except Exception as e:
            logger.error(f"记录假期尝试失败: {str(e)}")

    def _get_market(self, symbol: str, currency: str = None) -> str:
        """识别股票所属市场"""
        symbol = (symbol or '').upper()
        currency = (currency or '').upper()
        
        # 加拿大市场标识
        tsx_suffixes = ('.TO', '.TSX', '.TSXV', '.V', '.CN', '-T')
        if any(symbol.endswith(suffix) for suffix in tsx_suffixes):
            return 'CA'
        
        # 如果货币是CAD，也认为是加拿大市场
        if currency == 'CAD':
            return 'CA'
        
        # 默认美国市场
        return 'US'

    def _handle_error_response(self, symbol: str, start_date, end_date, data: Optional[Dict], info: Dict) -> Tuple[Dict, Dict]:
        if data and isinstance(data, dict):
            chart = data.get('chart') if isinstance(data.get('chart'), dict) else None
            error_info = chart.get('error') if chart else None
            if error_info:
                info['error_code'] = error_info.get('code')
                info['error_description'] = error_info.get('description')
                self._populate_no_data_from_error(start_date, end_date, info, error_info)
                return {}, info
        logger.error(f"Yahoo历史价格请求失败 {symbol} ({start_date} -> {end_date}) 状态码: {info.get('status_code')}")
        info['error'] = f"HTTP {info.get('status_code')}"
        return {}, info

    def _populate_no_data_from_error(self, start_date, end_date, info: Dict, error_info: Optional[Dict]):
        if not error_info:
            return
        description = error_info.get('description') or ''
        match = re.search(r'startDate\s*=\s*(\d+).+endDate\s*=\s*(\d+)', description)
        if match:
            try:
                start_ts = int(match.group(1))
                end_ts = int(match.group(2))
                start_dt = datetime.fromtimestamp(start_ts, tz=timezone.utc).date()
                end_dt = datetime.fromtimestamp(end_ts, tz=timezone.utc).date()
                info.setdefault('no_data_ranges', []).append((start_dt, end_dt))
            except Exception:
                pass
        elif 'Data doesn' in description:
            info.setdefault('no_data_ranges', []).append((start_date, end_date))
