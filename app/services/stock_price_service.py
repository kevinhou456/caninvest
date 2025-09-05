"""
股票价格获取和缓存服务
"""

import requests
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple
from decimal import Decimal
from flask import current_app
from app import db
# from app.models.stock import Stock  # Stock model deleted
from app.models.stocks_cache import StocksCache
from app.models.price_cache import StockPriceCache, PriceUpdateLog


class StockPriceService:
    """股票价格服务"""
    
    def __init__(self):
        self.api_key = current_app.config.get('ALPHA_VANTAGE_API_KEY')
        self.base_url = "https://www.alphavantage.co/query"
        self.timeout = 10  # API请求超时时间
    
    def get_stock_price(self, symbol: str, force_update: bool = False) -> Optional[Dict]:
        """获取股票价格（优先从缓存）"""
        # 检查缓存
        if not force_update:
            cached_price = self._get_cached_price(symbol)
            if cached_price:
                return cached_price
        
        # 检查API请求限制
        if not self._can_make_api_request():
            # 如果达到限制，返回过期的缓存数据
            cached_price = StockPriceCache.query.filter_by(symbol=symbol).first()
            if cached_price:
                return cached_price.to_dict()
            return None
        
        # 从API获取价格
        return self._fetch_price_from_api(symbol)
    
    def get_multiple_prices(self, symbols: List[str], force_update: bool = False) -> Dict[str, Dict]:
        """批量获取股票价格"""
        results = {}
        
        # 先检查缓存
        cached_symbols = []
        api_symbols = []
        
        for symbol in symbols:
            if not force_update:
                cached_price = self._get_cached_price(symbol)
                if cached_price:
                    results[symbol] = cached_price
                    cached_symbols.append(symbol)
                    continue
            
            api_symbols.append(symbol)
        
        # 为未缓存的股票调用API
        if api_symbols and self._can_make_api_request():
            for symbol in api_symbols:
                try:
                    price_data = self._fetch_price_from_api(symbol)
                    if price_data:
                        results[symbol] = price_data
                except Exception as e:
                    print(f"Error fetching price for {symbol}: {e}")
                    # 尝试返回过期缓存
                    cached_price = StockPriceCache.query.filter_by(symbol=symbol).first()
                    if cached_price:
                        results[symbol] = cached_price.to_dict()
        
        return results
    
    def update_stock_prices(self, stock_ids: List[int] = None) -> Dict:
        """更新股票价格（批量） - 临时禁用"""
        # Temporarily disabled due to deleted Stock model
        return {
            'error': 'Stock price updates temporarily disabled during system redesign',
            'updated': 0,
            'skipped': 0,
            'failed': 0,
            'errors': ['Stock model has been deleted']
        }
        
        # Original code commented out - needs to be re-implemented with StocksCache
        # if stock_ids:
        #     stocks = Stock.query.filter(Stock.id.in_(stock_ids), Stock.is_active == True).all()
        # else:
        #     stocks = Stock.query.filter_by(is_active=True).all()
        
        results = {
            'updated': 0,
            'skipped': 0,
            'failed': 0,
            'errors': []
        }
        
        if not self._can_make_api_request():
            results['errors'].append('API request limit reached for today')
            return results
        
        for stock in stocks:
            try:
                # 检查是否需要更新（避免过于频繁的更新）
                cache_entry = StockPriceCache.query.filter_by(symbol=stock.symbol).first()
                if cache_entry and not cache_entry.is_expired():
                    # 如果缓存仍然有效，跳过更新
                    results['skipped'] += 1
                    continue
                
                price_data = self._fetch_price_from_api(stock.symbol)
                if price_data:
                    results['updated'] += 1
                else:
                    results['failed'] += 1
                    
            except Exception as e:
                results['failed'] += 1
                results['errors'].append(f"{stock.symbol}: {str(e)}")
        
        return results
    
    def _get_cached_price(self, symbol: str) -> Optional[Dict]:
        """从缓存获取价格"""
        cache_entry = StockPriceCache.query.filter_by(symbol=symbol).first()
        
        if cache_entry and not cache_entry.is_expired():
            return cache_entry.to_dict()
        
        return None
    
    def _can_make_api_request(self) -> bool:
        """检查是否可以进行API请求"""
        if not self.api_key:
            return False
        
        today = datetime.now().date()
        log_entry = PriceUpdateLog.query.filter_by(date=today).first()
        
        if not log_entry:
            return True
        
        return not log_entry.is_rate_limited()
    
    def _fetch_price_from_api(self, symbol: str) -> Optional[Dict]:
        """从Alpha Vantage API获取价格"""
        try:
            # 记录API请求
            self._log_api_request()
            
            # 处理加拿大股票符号
            api_symbol = symbol
            if symbol.endswith('.TO'):
                api_symbol = symbol.replace('.TO', '.TRT')
            elif symbol.endswith('.V'):
                api_symbol = symbol.replace('.V', '.VAN')
            
            params = {
                'function': 'GLOBAL_QUOTE',
                'symbol': api_symbol,
                'apikey': self.api_key
            }
            
            response = requests.get(self.base_url, params=params, timeout=self.timeout)
            response.raise_for_status()
            
            data = response.json()
            
            # 检查API错误
            if 'Error Message' in data:
                raise ValueError(f"API Error: {data['Error Message']}")
            
            if 'Note' in data:
                # API调用频率限制
                raise ValueError(f"API Note: {data['Note']}")
            
            quote = data.get('Global Quote', {})
            if not quote:
                raise ValueError("No quote data returned")
            
            # 解析价格数据
            current_price = float(quote.get('05. price', 0))
            previous_close = float(quote.get('08. previous close', 0))
            change = float(quote.get('09. change', 0))
            change_percent = quote.get('10. change percent', '0%').replace('%', '')
            change_percent = float(change_percent)
            
            volume = int(quote.get('06. volume', 0))
            
            # 更新或创建缓存
            cache_entry = StockPriceCache.query.filter_by(symbol=symbol).first()
            if cache_entry:
                cache_entry.current_price = Decimal(str(current_price))
                cache_entry.previous_close = Decimal(str(previous_close))
                cache_entry.change_amount = Decimal(str(change))
                cache_entry.change_percent = Decimal(str(change_percent))
                cache_entry.volume = volume
                cache_entry.last_updated = datetime.now()
                cache_entry.api_source = 'Alpha Vantage'
            else:
                cache_entry = StockPriceCache(
                    symbol=symbol,
                    current_price=Decimal(str(current_price)),
                    previous_close=Decimal(str(previous_close)),
                    change_amount=Decimal(str(change)),
                    change_percent=Decimal(str(change_percent)),
                    volume=volume,
                    last_updated=datetime.now(),
                    api_source='Alpha Vantage'
                )
                db.session.add(cache_entry)
            
            db.session.commit()
            
            return cache_entry.to_dict()
            
        except requests.RequestException as e:
            print(f"Network error fetching price for {symbol}: {e}")
            return None
        except (ValueError, KeyError) as e:
            print(f"Data error fetching price for {symbol}: {e}")
            return None
        except Exception as e:
            print(f"Unexpected error fetching price for {symbol}: {e}")
            return None
    
    def _log_api_request(self):
        """记录API请求"""
        today = datetime.now().date()
        log_entry = PriceUpdateLog.query.filter_by(date=today).first()
        
        if log_entry:
            log_entry.request_count += 1
        else:
            log_entry = PriceUpdateLog(
                date=today,
                request_count=1
            )
            db.session.add(log_entry)
        
        db.session.commit()
    
    def get_historical_prices(self, symbol: str, period: str = '1M') -> List[Dict]:
        """获取历史价格数据"""
        if not self._can_make_api_request():
            return []
        
        try:
            self._log_api_request()
            
            api_symbol = symbol
            if symbol.endswith('.TO'):
                api_symbol = symbol.replace('.TO', '.TRT')
            elif symbol.endswith('.V'):
                api_symbol = symbol.replace('.V', '.VAN')
            
            function_map = {
                '1D': 'TIME_SERIES_INTRADAY',
                '1W': 'TIME_SERIES_DAILY',
                '1M': 'TIME_SERIES_DAILY',
                '3M': 'TIME_SERIES_DAILY',
                '1Y': 'TIME_SERIES_DAILY'
            }
            
            function = function_map.get(period, 'TIME_SERIES_DAILY')
            
            params = {
                'function': function,
                'symbol': api_symbol,
                'apikey': self.api_key
            }
            
            if function == 'TIME_SERIES_INTRADAY':
                params['interval'] = '60min'
            
            response = requests.get(self.base_url, params=params, timeout=self.timeout)
            response.raise_for_status()
            
            data = response.json()
            
            if 'Error Message' in data:
                raise ValueError(f"API Error: {data['Error Message']}")
            
            # 解析时间序列数据
            time_series_key = None
            for key in data.keys():
                if 'Time Series' in key:
                    time_series_key = key
                    break
            
            if not time_series_key:
                return []
            
            time_series = data[time_series_key]
            historical_data = []
            
            for date_str, price_data in time_series.items():
                try:
                    date_obj = datetime.strptime(date_str, '%Y-%m-%d')
                except ValueError:
                    date_obj = datetime.strptime(date_str, '%Y-%m-%d %H:%M:%S')
                
                historical_data.append({
                    'date': date_obj.isoformat(),
                    'open': float(price_data['1. open']),
                    'high': float(price_data['2. high']),
                    'low': float(price_data['3. low']),
                    'close': float(price_data['4. close']),
                    'volume': int(price_data['5. volume'])
                })
            
            # 按日期排序
            historical_data.sort(key=lambda x: x['date'])
            
            # 根据周期过滤数据
            if period == '1W':
                cutoff_date = datetime.now() - timedelta(days=7)
            elif period == '1M':
                cutoff_date = datetime.now() - timedelta(days=30)
            elif period == '3M':
                cutoff_date = datetime.now() - timedelta(days=90)
            elif period == '1Y':
                cutoff_date = datetime.now() - timedelta(days=365)
            else:
                cutoff_date = datetime.now() - timedelta(days=1)
            
            filtered_data = [
                item for item in historical_data
                if datetime.fromisoformat(item['date'].replace('Z', '')) >= cutoff_date
            ]
            
            return filtered_data
            
        except Exception as e:
            print(f"Error fetching historical data for {symbol}: {e}")
            return []
    
    def search_stocks(self, query: str) -> List[Dict]:
        """搜索股票"""
        if not self._can_make_api_request() or len(query) < 2:
            return []
        
        try:
            self._log_api_request()
            
            params = {
                'function': 'SYMBOL_SEARCH',
                'keywords': query,
                'apikey': self.api_key
            }
            
            response = requests.get(self.base_url, params=params, timeout=self.timeout)
            response.raise_for_status()
            
            data = response.json()
            
            if 'Error Message' in data:
                raise ValueError(f"API Error: {data['Error Message']}")
            
            matches = data.get('bestMatches', [])
            results = []
            
            for match in matches[:20]:  # 限制返回结果数量
                symbol = match.get('1. symbol', '')
                name = match.get('2. name', '')
                stock_type = match.get('3. type', '')
                region = match.get('4. region', '')
                currency = match.get('8. currency', '')
                
                # 转换加拿大股票符号
                display_symbol = symbol
                if symbol.endswith('.TRT'):
                    display_symbol = symbol.replace('.TRT', '.TO')
                elif symbol.endswith('.VAN'):
                    display_symbol = symbol.replace('.VAN', '.V')
                
                results.append({
                    'symbol': display_symbol,
                    'name': name,
                    'type': stock_type,
                    'region': region,
                    'currency': currency,
                    'match_score': match.get('9. matchScore', '0')
                })
            
            return results
            
        except Exception as e:
            print(f"Error searching stocks with query '{query}': {e}")
            return []
    
    def cleanup_old_cache(self, days: int = 7):
        """清理过期缓存"""
        cutoff_date = datetime.now() - timedelta(days=days)
        
        # 删除过期的价格缓存
        expired_count = StockPriceCache.query.filter(
            StockPriceCache.last_updated < cutoff_date
        ).delete()
        
        # 清理旧的API请求日志
        old_logs = PriceUpdateLog.query.filter(
            PriceUpdateLog.date < cutoff_date.date()
        ).delete()
        
        db.session.commit()
        
        return {
            'expired_cache_deleted': expired_count,
            'old_logs_deleted': old_logs
        }
    
    def get_api_usage_stats(self) -> Dict:
        """获取API使用统计"""
        today = datetime.now().date()
        log_entry = PriceUpdateLog.query.filter_by(date=today).first()
        
        max_requests = current_app.config.get('MAX_DAILY_PRICE_REQUESTS', 500)
        
        return {
            'today_requests': log_entry.request_count if log_entry else 0,
            'max_daily_requests': max_requests,
            'remaining_requests': max_requests - (log_entry.request_count if log_entry else 0),
            'rate_limited': log_entry.is_rate_limited() if log_entry else False,
            'api_key_configured': bool(self.api_key)
        }