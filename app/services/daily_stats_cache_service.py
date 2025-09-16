#!/usr/bin/env python3
"""
日统计缓存服务 - 基于现有stock_price_history表的智能缓存优化

设计原则:
1. 复用现有基础设施：最大化利用StockHistoryCacheService和stock_price_history表
2. 分层缓存策略：资产快照缓存 + 历史价格缓存 + 计算结果缓存
3. 智能失效机制：基于交易记录变更的增量失效
4. 高性能批量计算：减少重复的数据库查询和计算
5. 可配置缓存策略：支持不同的缓存时长和策略
"""

from datetime import datetime, date, timedelta
from typing import Dict, List, Optional, Tuple, Set
from decimal import Decimal
import logging
import json
from dataclasses import dataclass, asdict
from enum import Enum

from app import db
from app.models.stock_price_history import StockPriceHistory
from app.services.stock_history_cache_service import StockHistoryCacheService
from app.services.smart_history_manager import SmartHistoryManager
from app.services.asset_valuation_service import AssetValuationService

logger = logging.getLogger(__name__)


@dataclass
class AssetSnapshotCache:
    """资产快照缓存数据结构"""
    account_id: int
    snapshot_date: date
    total_assets: Decimal
    stock_market_value: Decimal
    cash_balance_cad: Decimal
    cash_balance_usd: Decimal
    cash_balance_total_cad: Decimal
    created_at: datetime
    
    # 用于失效检测的哈希值
    data_hash: str = ""
    
    def to_dict(self) -> Dict:
        """转换为字典格式（用于JSON序列化）"""
        return {
            'account_id': self.account_id,
            'snapshot_date': self.snapshot_date.isoformat(),
            'total_assets': str(self.total_assets),
            'stock_market_value': str(self.stock_market_value),
            'cash_balance_cad': str(self.cash_balance_cad),
            'cash_balance_usd': str(self.cash_balance_usd),
            'cash_balance_total_cad': str(self.cash_balance_total_cad),
            'created_at': self.created_at.isoformat(),
            'data_hash': self.data_hash
        }
    
    @classmethod
    def from_dict(cls, data: Dict) -> 'AssetSnapshotCache':
        """从字典创建对象"""
        return cls(
            account_id=data['account_id'],
            snapshot_date=datetime.fromisoformat(data['snapshot_date']).date(),
            total_assets=Decimal(data['total_assets']),
            stock_market_value=Decimal(data['stock_market_value']),
            cash_balance_cad=Decimal(data['cash_balance_cad']),
            cash_balance_usd=Decimal(data['cash_balance_usd']),
            cash_balance_total_cad=Decimal(data['cash_balance_total_cad']),
            created_at=datetime.fromisoformat(data['created_at']),
            data_hash=data['data_hash']
        )


class CacheStrategy(Enum):
    """缓存策略枚举"""
    AGGRESSIVE = "aggressive"    # 积极缓存：缓存所有计算结果
    BALANCED = "balanced"        # 平衡缓存：缓存常用数据
    CONSERVATIVE = "conservative" # 保守缓存：只缓存历史数据


class DailyStatsCacheService:
    """
    日统计缓存服务
    
    基于现有的stock_price_history表，提供多层缓存策略：
    1. L1: 内存缓存（临时会话缓存）
    2. L2: 数据库缓存（复用stock_price_history + 新增asset_snapshot缓存）
    3. L3: 智能计算缓存（基于SmartHistoryManager）
    """
    
    def __init__(self, strategy: CacheStrategy = CacheStrategy.BALANCED):
        self.strategy = strategy
        self.stock_cache_service = StockHistoryCacheService()
        self.history_manager = SmartHistoryManager()
        self.asset_service = AssetValuationService()
        
        # L1 内存缓存（会话级别）
        self._memory_cache = {
            'asset_snapshots': {},  # key: f"{account_id}_{date}", value: AssetSnapshotCache
            'monthly_calendars': {},  # key: f"{account_ids_hash}_{year}_{month}", value: dict
            'price_data': {}  # key: f"{symbol}_{date}", value: Decimal
        }
        
        # 缓存配置
        self.cache_config = {
            'asset_snapshot_ttl_hours': 1,  # 资产快照缓存1小时
            'monthly_calendar_ttl_hours': 4,  # 月历缓存4小时
            'price_data_ttl_hours': 24,  # 价格数据缓存24小时
            'max_memory_cache_size': 1000  # 最大内存缓存条目数
        }
    
    def get_cached_asset_snapshot(self, account_id: int, target_date: date) -> Optional[AssetSnapshotCache]:
        """
        获取缓存的资产快照
        
        优先级：内存缓存 -> 数据库缓存 -> 重新计算
        """
        cache_key = f"{account_id}_{target_date.isoformat()}"
        
        # L1: 检查内存缓存
        if cache_key in self._memory_cache['asset_snapshots']:
            cached_snapshot = self._memory_cache['asset_snapshots'][cache_key]
            if self._is_cache_valid(cached_snapshot.created_at, 'asset_snapshot_ttl_hours'):
                logger.debug(f"L1缓存命中: 账户{account_id}的{target_date}资产快照")
                return cached_snapshot
            else:
                # 缓存过期，清理
                del self._memory_cache['asset_snapshots'][cache_key]
        
        # L2: 检查是否可以从现有数据快速重建
        # 利用stock_price_history表中的历史价格数据
        if self._can_rebuild_from_cache(account_id, target_date):
            snapshot = self._rebuild_snapshot_from_cache(account_id, target_date)
            if snapshot:
                # 存入内存缓存
                self._store_in_memory_cache('asset_snapshots', cache_key, snapshot)
                logger.debug(f"L2缓存重建: 账户{account_id}的{target_date}资产快照")
                return snapshot
        
        # L3: 需要重新计算
        logger.debug(f"缓存未命中，重新计算: 账户{account_id}的{target_date}资产快照")
        return None
    
    def cache_asset_snapshot(self, account_id: int, target_date: date, 
                           snapshot_data: Dict) -> AssetSnapshotCache:
        """
        缓存资产快照数据
        """
        # 计算数据哈希（用于失效检测）
        data_hash = self._calculate_data_hash(account_id, target_date)
        
        # 创建缓存对象
        cached_snapshot = AssetSnapshotCache(
            account_id=account_id,
            snapshot_date=target_date,
            total_assets=snapshot_data['total_assets'],
            stock_market_value=snapshot_data['stock_market_value'],
            cash_balance_cad=snapshot_data['cash_balance_cad'],
            cash_balance_usd=snapshot_data['cash_balance_usd'],
            cash_balance_total_cad=snapshot_data['cash_balance_total_cad'],
            created_at=datetime.utcnow(),
            data_hash=data_hash
        )
        
        # 存入内存缓存
        cache_key = f"{account_id}_{target_date.isoformat()}"
        self._store_in_memory_cache('asset_snapshots', cache_key, cached_snapshot)
        
        # 根据策略决定是否持久化
        if self.strategy in [CacheStrategy.AGGRESSIVE, CacheStrategy.BALANCED]:
            self._persist_snapshot_cache(cached_snapshot)
        
        return cached_snapshot
    
    def get_optimized_price_data(self, symbol: str, date_range: Tuple[date, date], 
                               currency: str = 'USD') -> List[Dict]:
        """
        获取优化的价格数据
        
        充分利用现有的StockHistoryCacheService和SmartHistoryManager
        """
        start_date, end_date = date_range
        
        # 使用SmartHistoryManager获取智能优化的历史数据
        try:
            historical_data = self.history_manager.get_historical_data_for_stock(
                symbol, 
                currency=currency
            )
            
            # 过滤到指定日期范围
            filtered_data = [
                data for data in historical_data
                if start_date <= datetime.strptime(data['date'], '%Y-%m-%d').date() <= end_date
            ]
            
            logger.debug(f"获取{symbol}的{len(filtered_data)}条优化价格数据")
            return filtered_data
            
        except Exception as e:
            logger.error(f"获取{symbol}价格数据失败: {e}")
            return []
    
    def batch_get_price_data(self, symbols: List[str], date_range: Tuple[date, date]) -> Dict[str, List[Dict]]:
        """
        批量获取多个股票的价格数据
        
        利用现有缓存基础设施进行批量优化
        """
        price_data = {}
        
        for symbol in symbols:
            # 获取股票的货币类型
            from app.models.transaction import Transaction
            currency = Transaction.get_currency_by_stock_symbol(symbol) or 'USD'
            
            price_data[symbol] = self.get_optimized_price_data(symbol, date_range, currency)
        
        return price_data
    
    def invalidate_cache_for_account(self, account_id: int, affected_dates: List[date] = None):
        """
        失效指定账户的缓存
        
        当有新交易或数据变更时调用
        """
        logger.info(f"失效账户{account_id}的缓存，影响日期: {affected_dates}")
        
        # 失效内存缓存
        keys_to_remove = []
        for key in self._memory_cache['asset_snapshots']:
            if key.startswith(f"{account_id}_"):
                if affected_dates is None:
                    keys_to_remove.append(key)
                else:
                    # 检查是否在影响日期范围内
                    date_str = key.split('_', 1)[1]
                    snapshot_date = datetime.fromisoformat(date_str).date()
                    if any(abs((snapshot_date - affected_date).days) <= 1 for affected_date in affected_dates):
                        keys_to_remove.append(key)
        
        for key in keys_to_remove:
            del self._memory_cache['asset_snapshots'][key]
        
        # 失效月历缓存
        monthly_keys_to_remove = []
        for key in self._memory_cache['monthly_calendars']:
            if f"_{account_id}_" in key or key.endswith(f"_{account_id}"):
                monthly_keys_to_remove.append(key)
        
        for key in monthly_keys_to_remove:
            del self._memory_cache['monthly_calendars'][key]
        
        logger.debug(f"已失效{len(keys_to_remove)}个资产快照和{len(monthly_keys_to_remove)}个月历缓存")
    
    def get_cache_statistics(self) -> Dict:
        """获取缓存统计信息"""
        # 获取stock_price_history的统计
        price_cache_stats = self.stock_cache_service.get_cache_statistics()
        
        # 内存缓存统计
        memory_stats = {
            'asset_snapshots_count': len(self._memory_cache['asset_snapshots']),
            'monthly_calendars_count': len(self._memory_cache['monthly_calendars']),
            'price_data_count': len(self._memory_cache['price_data']),
            'total_memory_entries': sum(len(cache) for cache in self._memory_cache.values())
        }
        
        return {
            'strategy': self.strategy.value,
            'memory_cache': memory_stats,
            'price_cache': price_cache_stats,
            'config': self.cache_config
        }
    
    def cleanup_expired_cache(self):
        """清理过期的内存缓存"""
        current_time = datetime.utcnow()
        
        for cache_type, cache_dict in self._memory_cache.items():
            ttl_key = f"{cache_type.rstrip('s')}_ttl_hours"
            if ttl_key not in self.cache_config:
                continue
                
            ttl_hours = self.cache_config[ttl_key]
            expired_keys = []
            
            for key, value in cache_dict.items():
                if hasattr(value, 'created_at'):
                    if (current_time - value.created_at).total_seconds() > ttl_hours * 3600:
                        expired_keys.append(key)
            
            for key in expired_keys:
                del cache_dict[key]
            
            if expired_keys:
                logger.debug(f"清理了{len(expired_keys)}个过期的{cache_type}缓存")
    
    def _can_rebuild_from_cache(self, account_id: int, target_date: date) -> bool:
        """
        检查是否可以从现有缓存重建资产快照
        
        检查stock_price_history中是否有足够的价格数据
        """
        # 获取该账户在目标日期的所有持仓股票
        from app.models.transaction import Transaction
        
        # 查询该日期前的所有股票交易
        stock_symbols = db.session.query(Transaction.stock.distinct()).filter(
            Transaction.account_id == account_id,
            Transaction.trade_date <= target_date,
            Transaction.stock.isnot(None),
            Transaction.stock != ''
        ).all()
        
        if not stock_symbols:
            return True  # 没有股票持仓，可以快速计算
        
        # 检查是否所有股票都有该日期的价格缓存
        missing_prices = 0
        for (symbol,) in stock_symbols:
            currency = Transaction.get_currency_by_stock_symbol(symbol) or 'USD'
            
            price_record = StockPriceHistory.query.filter(
                StockPriceHistory.symbol == symbol,
                StockPriceHistory.currency == currency,
                StockPriceHistory.trade_date == target_date
            ).first()
            
            if not price_record:
                missing_prices += 1
        
        # 如果缺失价格数据少于20%，认为可以重建
        total_stocks = len(stock_symbols)
        if total_stocks == 0:
            return True
        
        missing_ratio = missing_prices / total_stocks
        can_rebuild = missing_ratio < 0.2
        
        logger.debug(f"账户{account_id}在{target_date}的价格缓存覆盖率: {(1-missing_ratio)*100:.1f}%, 可重建: {can_rebuild}")
        return can_rebuild
    
    def _rebuild_snapshot_from_cache(self, account_id: int, target_date: date) -> Optional[AssetSnapshotCache]:
        """
        从缓存数据重建资产快照
        
        利用stock_price_history表中的价格数据
        """
        try:
            # 使用AssetValuationService计算，它会自动利用缓存
            snapshot = self.asset_service.get_asset_snapshot(account_id, target_date)
            
            # 转换为缓存格式
            cached_snapshot = AssetSnapshotCache(
                account_id=account_id,
                snapshot_date=target_date,
                total_assets=snapshot.total_assets,
                stock_market_value=snapshot.stock_market_value,
                cash_balance_cad=snapshot.cash_balance_cad,
                cash_balance_usd=snapshot.cash_balance_usd,
                cash_balance_total_cad=snapshot.cash_balance_total_cad,
                created_at=datetime.utcnow(),
                data_hash=self._calculate_data_hash(account_id, target_date)
            )
            
            return cached_snapshot
            
        except Exception as e:
            logger.error(f"从缓存重建快照失败: {e}")
            return None
    
    def _calculate_data_hash(self, account_id: int, target_date: date) -> str:
        """
        计算数据哈希值（用于缓存失效检测）
        
        基于账户的交易记录和股票价格变更时间
        """
        import hashlib
        
        # 获取该账户相关的最后更新时间
        from app.models.transaction import Transaction
        
        last_transaction = Transaction.query.filter(
            Transaction.account_id == account_id,
            Transaction.trade_date <= target_date
        ).order_by(Transaction.created_at.desc()).first()
        
        # 构建哈希输入
        hash_input = f"{account_id}_{target_date}_{last_transaction.created_at if last_transaction else 'none'}"
        
        return hashlib.md5(hash_input.encode()).hexdigest()
    
    def _is_cache_valid(self, created_at: datetime, ttl_key: str) -> bool:
        """检查缓存是否仍然有效"""
        if ttl_key not in self.cache_config:
            return False
        
        ttl_hours = self.cache_config[ttl_key]
        elapsed_hours = (datetime.utcnow() - created_at).total_seconds() / 3600
        
        return elapsed_hours < ttl_hours
    
    def _store_in_memory_cache(self, cache_type: str, key: str, value):
        """存储到内存缓存，带大小限制"""
        cache_dict = self._memory_cache[cache_type]
        
        # 检查缓存大小限制
        if len(cache_dict) >= self.cache_config['max_memory_cache_size']:
            # LRU清理：删除最老的条目
            oldest_key = min(cache_dict.keys(), 
                           key=lambda k: getattr(cache_dict[k], 'created_at', datetime.min))
            del cache_dict[oldest_key]
        
        cache_dict[key] = value
    
    def _persist_snapshot_cache(self, snapshot: AssetSnapshotCache):
        """
        持久化快照缓存（可选实现）
        
        如果需要更持久的缓存，可以扩展这个方法
        将数据存储到专门的缓存表中
        """
        # 这里可以实现将快照数据存储到数据库的逻辑
        # 目前我们主要依赖现有的stock_price_history表
        pass


# 全局缓存服务实例
daily_stats_cache_service = DailyStatsCacheService()