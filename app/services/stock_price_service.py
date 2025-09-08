"""
股票价格获取和缓存服务 - 使用Yahoo Finance
"""

import requests
import json
from datetime import datetime, timedelta
from typing import Dict, List, Optional
from decimal import Decimal
from flask import current_app
from app import db
from app.models.stocks_cache import StocksCache


class StockPriceService:
    """股票价格服务 - 使用Yahoo Finance"""
    
    def __init__(self):
        self.timeout = 10
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        }
    
    def get_stock_price(self, symbol: str) -> Optional[Dict]:
        """从Yahoo Finance获取股票当前价格"""
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
            
            return {
                'symbol': symbol,
                'price': float(current_price),
                'currency': meta.get('currency', 'USD'),
                'exchange': meta.get('exchangeName', ''),
                'name': meta.get('longName', ''),
                'updated_at': datetime.utcnow()
            }
            
        except Exception as e:
            print(f"获取{symbol}价格失败: {str(e)}")
            return None
    
    def update_stock_price(self, symbol: str) -> bool:
        """更新数据库中的股票价格"""
        try:
            price_data = self.get_stock_price(symbol)
            if not price_data:
                return False
            
            stock = StocksCache.query.filter_by(symbol=symbol).first()
            if stock:
                stock.current_price = Decimal(str(price_data['price']))
                stock.price_updated_at = datetime.utcnow()
                
                # 如果有新的名称或交易所信息，也更新
                if price_data.get('name') and not stock.name:
                    stock.name = price_data['name']
                if price_data.get('exchange') and not stock.exchange:
                    stock.exchange = price_data['exchange']
                
                db.session.commit()
                print(f"更新{symbol}价格: {price_data['currency']} ${price_data['price']:.2f}")
                return True
            
        except Exception as e:
            print(f"更新{symbol}价格失败: {str(e)}")
            db.session.rollback()
            
        return False
    
    def update_all_stock_prices(self) -> Dict:
        """更新所有股票价格"""
        stocks = StocksCache.query.all()
        results = {
            'total': len(stocks),
            'updated': 0,
            'failed': 0,
            'errors': []
        }
        
        for stock in stocks:
            print(f"正在更新 {stock.symbol}...")
            try:
                if self.update_stock_price(stock.symbol):
                    results['updated'] += 1
                else:
                    results['failed'] += 1
                    results['errors'].append(f"{stock.symbol}: 获取价格失败")
            except Exception as e:
                results['failed'] += 1
                results['errors'].append(f"{stock.symbol}: {str(e)}")
        
        return results
    
    def update_prices_for_symbols(self, symbols: List[str]) -> Dict:
        """更新指定股票列表的价格"""
        results = {
            'total': len(symbols),
            'updated': 0,
            'failed': 0,
            'errors': []
        }
        
        for symbol in symbols:
            print(f"正在更新 {symbol}...")
            try:
                if self.update_stock_price(symbol):
                    results['updated'] += 1
                else:
                    results['failed'] += 1
                    results['errors'].append(f"{symbol}: 获取价格失败")
            except Exception as e:
                results['failed'] += 1
                results['errors'].append(f"{symbol}: {str(e)}")
        
        return results