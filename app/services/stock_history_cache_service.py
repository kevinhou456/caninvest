"""
股票历史价格缓存服务
高度可扩展且易于维护的设计
"""

import requests
import time
from datetime import datetime, timedelta, date
from typing import Dict, List, Optional, Tuple
from decimal import Decimal
from flask import current_app
from app import db
from app.models.stock_price_history import StockPriceHistory
from app.services.stock_price_service import StockPriceService


class StockHistoryCacheService:
    """
    股票历史价格缓存服务
    
    设计原则:
    1. 单一职责：专门处理历史价格缓存
    2. 开放封闭：易于扩展新的数据源
    3. 依赖倒置：依赖抽象而非具体实现
    4. 无重复代码：统一的数据处理和缓存逻辑
    """
    
    def __init__(self):
        self.stock_service = StockPriceService()
        self.cache_days_threshold = 7  # 缓存过期天数
        
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
            # 1. 评估缓存状态
            cache_gaps = self._analyze_cache_gaps(symbol, start_date, end_date, currency)
            
            # 2. 如果需要刷新或有缺失，获取新数据
            if force_refresh or cache_gaps['needs_update']:
                self._update_cache_data(symbol, cache_gaps, currency)
            
            # 3. 从缓存返回数据
            return self._get_cached_data(symbol, start_date, end_date, currency)
            
        except Exception as e:
            print(f"获取缓存历史数据失败 {symbol}: {str(e)}")
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
            'cache_coverage': 0.0
        }
        
        # 计算应该有的交易日数量（简化计算，实际应考虑节假日）
        total_days = (end_date - start_date).days + 1
        expected_trading_days = total_days * 5 // 7  # 粗略估算交易日
        
        if expected_trading_days > 0:
            analysis['cache_coverage'] = len(cached_dates) / expected_trading_days
        
        # 判断是否需要更新
        if not latest_cached_date:
            # 没有任何缓存数据
            analysis['needs_update'] = True
            analysis['missing_ranges'].append((start_date, end_date))
        elif latest_cached_date < end_date:
            # 检查是否只是缺少最近几天的数据（可能是因为今天还没收盘）
            days_gap = (end_date - latest_cached_date).days
            if days_gap <= 3:  # 如果只差3天以内，认为历史数据已经足够
                print(f"历史数据缓存已足够，最晚日期: {latest_cached_date}, 请求结束: {end_date}, 差距: {days_gap}天")
                analysis['needs_update'] = False
            else:
                # 缓存数据确实不够新
                analysis['needs_update'] = True
                update_start = max(start_date, latest_cached_date + timedelta(days=1))
                analysis['missing_ranges'].append((update_start, end_date))
        elif analysis['cache_coverage'] < 0.6:  # 降低覆盖率要求从80%到60%
            # 缓存覆盖率不足
            analysis['needs_update'] = True
            analysis['missing_ranges'].append((start_date, end_date))
        
        return analysis
    
    def _update_cache_data(self, symbol: str, cache_gaps: Dict, currency: str):
        """
        更新缓存数据
        
        参数:
            symbol: 股票代码
            cache_gaps: 缓存分析结果
            currency: 货币代码
        """
        for start_date, end_date in cache_gaps['missing_ranges']:
            try:
                # 从Yahoo Finance获取数据
                raw_data = self.stock_service.get_stock_history(symbol, start_date, end_date)
                
                if raw_data:
                    # 转换并保存数据
                    processed_data = self._process_raw_data(symbol, raw_data, currency)
                    success = StockPriceHistory.bulk_upsert(processed_data)
                    
                    if success:
                        print(f"成功缓存 {symbol} 从 {start_date} 到 {end_date} 的 {len(processed_data)} 条记录")
                    else:
                        print(f"缓存 {symbol} 数据失败")
                else:
                    print(f"无法获取 {symbol} 的历史数据")
                    
            except Exception as e:
                print(f"更新 {symbol} 缓存数据失败: {str(e)}")
    
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
                print(f"处理日期 {date_str} 的数据失败: {str(e)}")
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
            print(f"从缓存获取数据失败 {symbol}: {str(e)}")
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
            print(f"获取缓存统计失败: {str(e)}")
            return {}
    
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
                print(f"清理了 {deleted_count} 条旧缓存记录")
            
            return {
                'success': True,
                'deleted_count': deleted_count,
                'cutoff_date': cutoff_date.isoformat()
            }
            
        except Exception as e:
            db.session.rollback()
            print(f"清理缓存失败: {str(e)}")
            return {
                'success': False,
                'error': str(e),
                'deleted_count': 0
            }