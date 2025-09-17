"""
股票价格获取和缓存服务 - 使用Yahoo Finance
"""

import logging
import requests
import json
from datetime import datetime, timedelta
from typing import Dict, List, Optional
from decimal import Decimal
from flask import current_app
from app import db
from app.models.stocks_cache import StocksCache

logger = logging.getLogger(__name__)


class StockPriceService:
    """股票价格服务 - 使用Yahoo Finance"""
    
    def __init__(self):
        self.timeout = 10
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        }
    
    def get_stock_price(self, symbol: str, expected_currency: str = None) -> Optional[Dict]:
        """从Yahoo Finance获取股票当前价格，并验证货币匹配
        
        Args:
            symbol: 股票代码
            expected_currency: 期望的货币代码 (USD/CAD)，如果提供则验证货币匹配
        
        Returns:
            Dict: 股票价格数据，如果货币不匹配则返回None
        """
        try:
            # Yahoo Finance API URL
            url = f"https://query1.finance.yahoo.com/v8/finance/chart/{symbol}"
            
            response = requests.get(url, headers=self.headers, timeout=self.timeout)
            response.raise_for_status()
            
            data = response.json()
            
            if 'chart' not in data or not data['chart']['result']:
                return None
                
            result = data['chart']['result'][0]
            
            # 获取当前价格
            meta = result.get('meta', {})
            current_price = meta.get('regularMarketPrice')
            
            if current_price is None:
                return None
            
            # 获取Yahoo Finance返回的货币信息
            yahoo_currency = meta.get('currency', 'USD').upper()
            
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
                'exchange': meta.get('exchangeName', ''),
                'name': meta.get('longName', ''),
                'updated_at': datetime.utcnow()
            }
            
        except Exception as e:
            logger.error(f"获取{symbol}价格失败: {str(e)}")
            return None
    
    def update_stock_price(self, symbol: str, currency: str) -> bool:
        """更新数据库中的股票价格"""
        # 验证符号不为空
        if not symbol or not symbol.strip():
            logger.warning(f"无效的股票代码：'{symbol}'，跳过更新")
            return False
            
       

        try:
            # 不传入期望货币进行验证，让Yahoo Finance返回实际货币信息
            price_data = self.get_stock_price(symbol)

            
            
            # 使用联合主键查询
            stock = StocksCache.query.filter_by(symbol=symbol, currency=currency).first()
            if not stock:
                # 如果stocks_cache中没有该股票，创建新记录，使用指定的currency
                stock = StocksCache(symbol=symbol, currency=currency)
                db.session.add(stock)

            
            # 更新时间戳，无论是否获取到价格
            stock.price_updated_at = datetime.utcnow()
            
            if price_data:
                # 获取到价格数据，但不检查货币匹配
                # 重要：保持stock.currency不变，使用传入的currency参数
                yahoo_currency = price_data.get('currency', 'USD').upper()
                if yahoo_currency != currency.upper():
                    logger.warning(f"货币不匹配: {symbol} Yahoo Finance返回{yahoo_currency}，但保持数据库中的{currency}")
                    stock.current_price = Decimal('0')
                else:
                    stock.current_price = Decimal(str(price_data['price']))
                    # 重要：不更新stock.currency，保持原有货币设置
                
                    # 如果有新的名称或交易所信息，也更新
                    if price_data.get('name') and not stock.name:
                        stock.name = price_data['name']
                    if price_data.get('exchange') and not stock.exchange:
                        stock.exchange = price_data['exchange']
                
                db.session.commit()
                
                return True
            else:
                # 无法从Yahoo Finance获取价格，设置为0并更新时间戳
                stock.current_price = Decimal('0')
                # currency已经在创建时设置，不需要再次设置
                
                db.session.commit()
                logger.warning(f"无法获取{symbol}({currency})价格，设置为0并更新时间戳")
                return True  # 仍返回True，因为我们成功处理了这种情况
            
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
    
    def update_prices_for_symbols(self, symbol_currency_pairs: List[tuple]) -> Dict:
        """更新指定股票列表的价格
        参数: symbol_currency_pairs - 包含(symbol, currency)元组的列表
        """
        results = {
            'total': len(symbol_currency_pairs),
            'updated': 0,
            'failed': 0,
            'errors': []
        }
        
        for symbol, currency in symbol_currency_pairs:
            try:
                if self.update_stock_price(symbol, currency):
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
    
    def get_stock_history(self, symbol: str, start_date, end_date) -> Dict:
        """
        获取股票历史价格数据
        参数:
            symbol: 股票代码
            start_date: 开始日期
            end_date: 结束日期
        返回:
            Dict: 日期和价格的字典，格式为 {'YYYY-MM-DD': {'close': price}}
        """
        try:
            # 转换日期为Unix时间戳
            import time
            start_timestamp = int(time.mktime(start_date.timetuple()))
            end_timestamp = int(time.mktime(end_date.timetuple()))
            
            # Yahoo Finance API URL for historical data
            url = f"https://query1.finance.yahoo.com/v8/finance/chart/{symbol}"
            params = {
                'period1': start_timestamp,
                'period2': end_timestamp,
                'interval': '1d',
                'includePrePost': 'false'
            }
            
            response = requests.get(url, headers=self.headers, params=params, timeout=self.timeout)
            print(f"[Yahoo API] 请求 {symbol} {start_date}->{end_date} params={params}")
            response.raise_for_status()

            data = response.json()
            
            if 'chart' not in data or not data['chart']['result']:
                logger.warning(f"无法获取{symbol}的历史价格数据 ({start_date} -> {end_date})")
                return {}
            
            result = data['chart']['result'][0]
            
            
            # 检查是否有价格数据
            if 'indicators' not in result or 'quote' not in result['indicators'] or not result['indicators']['quote']:
                logger.warning(f"{symbol}历史数据中无价格信息 ({start_date} -> {end_date})")
                return {}
            
            # 检查timestamp数据是否存在
            if 'timestamp' not in result:
                logger.warning(f"{symbol}历史数据中无时间戳信息 ({start_date} -> {end_date})")
                return {}
                
            timestamps = result['timestamp']
            prices = result['indicators']['quote'][0]['close']
            
            # 构建历史价格字典
            history = {}
            for i, timestamp in enumerate(timestamps):
                if i < len(prices) and prices[i] is not None:
                    date_str = datetime.fromtimestamp(timestamp).strftime('%Y-%m-%d')
                    history[date_str] = {
                        'close': float(prices[i])
                    }
            
            return history
            
        except requests.RequestException as e:
            logger.error(f"获取{symbol}历史价格失败 ({start_date} -> {end_date}): {str(e)}")
            return {}
        except Exception as e:
            logger.error(f"解析{symbol}历史价格数据失败: {str(e)}")
            return {}
