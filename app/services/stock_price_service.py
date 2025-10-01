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
    details = " ".join(f"{k}={v}" for k, v in kwargs.items() if v is not None)
    message = f"[yfinance] {api_name} symbol={symbol} {details}".strip()
    logger.debug(message)
    print(message)

logger = logging.getLogger(__name__)


class StockPriceService:
    """股票价格服务 - 使用Yahoo Finance"""
    
    def __init__(self):
        pass
    
    def get_stock_price(self, symbol: str, expected_currency: str = None) -> Optional[Dict]:
        """使用yfinance获取股票当前价格，并验证货币匹配

        Args:
            symbol: 股票代码
            expected_currency: 期望的货币代码 (USD/CAD)，如果提供则验证货币匹配

        Returns:
            Dict: 股票价格数据，如果货币不匹配则返回None
        """
        try:
            # 添加延迟避免被ban
            time.sleep(1)
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
                'updated_at': datetime.utcnow()
            }

        except Exception as e:
            logger.error(f"获取{symbol}价格失败: {str(e)}")
            print(f"[yfinance][error] Ticker.info symbol={symbol} error={e}")
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

            # 检查是否需要更新价格（15分钟过期机制，除非强制刷新）
            needs_update = force_refresh
            if not needs_update:
                if not stock:
                    needs_update = True
                elif not stock.price_updated_at:
                    needs_update = True
                else:
                    time_diff = datetime.utcnow() - stock.price_updated_at
                    if time_diff.total_seconds() > 900:  # 15分钟 = 900秒
                        needs_update = True

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
        results = {
            'total': len(symbol_currency_pairs),
            'updated': 0,
            'failed': 0,
            'errors': []
        }

        for symbol, currency in symbol_currency_pairs:
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
    
    def get_cached_stock_price(self, symbol: str, currency: str) -> Decimal:
        """
        获取股票当前价格 - 带15分钟缓存过期检查和自动更新
        参数:
            symbol: 股票代码
            currency: 股票货币（必需，用于联合主键查询）
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
            
            # 检查是否需要更新价格（15分钟过期机制）
            needs_update = False
            if not stock_cache:
                needs_update = True
            elif not stock_cache.price_updated_at:
                needs_update = True
            else:
                time_diff = datetime.utcnow() - stock_cache.price_updated_at
                if time_diff.total_seconds() > 900:  # 15分钟 = 900秒
                    needs_update = True
            
            # 如果需要更新，从Yahoo Finance获取最新价格
            if needs_update:
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
    
    def get_stock_history(self, symbol: str, start_date, end_date) -> Tuple[Dict, Dict]:
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
            # 添加延迟避免被ban
            time.sleep(1)
            # 使用 yfinance 获取历史数据
            _log_yfinance_call('Ticker.history', symbol, start=start_date, end=end_date)
            ticker = yf.Ticker(symbol)

            # 获取历史数据，加1天确保包含结束日期
            hist = ticker.history(
                start=start_date,
                end=end_date + timedelta(days=1),
                interval='1d',
                actions=False,
                auto_adjust=False,
                back_adjust=False
            )

            if hist.empty:
                message = f"yfinance 未返回 {symbol} 的历史数据 ({start_date} -> {end_date})"
                logger.warning(message)
                print(f"[yfinance][empty] Ticker.history symbol={symbol} start={start_date} end={end_date}")
                info['error'] = 'empty_data'
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
                print(success_message)
            else:
                warning_message = (
                    f"⚠️ yfinance 返回空数据 {symbol} ({start_date} -> {end_date})"
                )
                logger.warning(warning_message)
                print(f"[yfinance][empty] Ticker.history symbol={symbol} start={start_date} end={end_date}")
                info['error'] = 'empty_data'

            return result_data, info

        except Exception as e:
            logger.error(f"❌ yfinance 获取 {symbol} 历史价格失败 ({start_date} -> {end_date}): {str(e)}")
            print(f"[yfinance][error] Ticker.history symbol={symbol} start={start_date} end={end_date} error={e}")
            info['error'] = str(e)
            return {}, info

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
