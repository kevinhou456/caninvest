#!/usr/bin/env python3
"""
数据变更监听服务
监听模型数据变更，确保缓存失效和数据一致性
"""

import logging
from typing import Set, Dict, List
from datetime import date, datetime
from sqlalchemy import event
from flask import current_app

from app.models.transaction import Transaction
from app.models.account import Account

logger = logging.getLogger(__name__)


class SmartCacheManager:
    """智能缓存管理器"""
    
    def __init__(self):
        self.cache = {}  # 简单内存缓存，生产环境应使用Redis
        
    def get(self, key: str):
        """获取缓存"""
        if key in self.cache:
            cached_data, timestamp = self.cache[key]
            # 简单的过期检查（5分钟）
            if (datetime.now() - timestamp).seconds < 300:
                return cached_data
            else:
                del self.cache[key]
        return None
        
    def set(self, key: str, value, ttl: int = 300):
        """设置缓存"""
        self.cache[key] = (value, datetime.now())
        
    def delete(self, key: str):
        """删除单个缓存"""
        if key in self.cache:
            del self.cache[key]
            
    def delete_pattern(self, pattern: str):
        """删除匹配模式的缓存"""
        keys_to_delete = []
        for key in self.cache.keys():
            if pattern.replace('*', '') in key:
                keys_to_delete.append(key)
        
        for key in keys_to_delete:
            del self.cache[key]
            
        logger.debug(f"删除了{len(keys_to_delete)}个匹配'{pattern}'的缓存")
        
    def clear_account_cache(self, account_id: int, from_date: date = None):
        """清除账户相关缓存"""
        if from_date:
            # 清除指定日期之后的缓存
            self.delete_pattern(f"account:{account_id}:date:{from_date}:*")
            self.delete_pattern(f"account:{account_id}:snapshot:*")
        else:
            # 清除所有账户缓存
            self.delete_pattern(f"account:{account_id}:*")
            
        logger.info(f"清除了账户{account_id}的缓存，起始日期: {from_date}")


class DataChangeListener:
    """数据变更监听器"""
    
    def __init__(self):
        self.cache_manager = SmartCacheManager()
        self._setup_listeners()
        
    def _setup_listeners(self):
        """设置SQLAlchemy事件监听器"""
        
        # Transaction表监听
        event.listen(Transaction, 'after_insert', self._on_transaction_changed)
        event.listen(Transaction, 'after_update', self._on_transaction_changed)
        event.listen(Transaction, 'after_delete', self._on_transaction_changed)
        
        # Account表监听（现金变更）
        event.listen(Account, 'after_update', self._on_account_changed)
        
        logger.info("数据变更监听器已设置完成")
        
    def _on_transaction_changed(self, mapper, connection, target):
        """交易记录变更处理"""
        try:
            account_id = target.account_id
            transaction_date = target.trade_date
            
            logger.info(f"检测到交易记录变更: 账户{account_id}, 日期{transaction_date}")
            
            # 清除该账户从交易日期开始的所有缓存
            self.cache_manager.clear_account_cache(account_id, transaction_date)
            
            # 如果涉及股票交易，还需要清除持仓相关缓存
            if target.stock:
                self._clear_stock_related_cache(target.stock, transaction_date)
                
        except Exception as e:
            logger.error(f"处理交易变更时出错: {e}", exc_info=True)
            
    def _on_account_changed(self, mapper, connection, target):
        """账户信息变更处理"""
        try:
            account_id = target.id
            
            # 检查是否是现金余额变更
            if self._is_cash_balance_changed(target):
                logger.info(f"检测到账户{account_id}现金余额变更")
                
                # 清除所有资产快照缓存
                self.cache_manager.clear_account_cache(account_id)
                
        except Exception as e:
            logger.error(f"处理账户变更时出错: {e}", exc_info=True)
            
            
    def _is_cash_balance_changed(self, account: Account) -> bool:
        """检查账户现金余额是否发生变更"""
        # SQLAlchemy的inspect可以检查字段变更
        from sqlalchemy import inspect
        
        state = inspect(account)
        history = state.attrs
        
        # 检查cash_cad和cash_usd字段
        cash_cad_changed = 'cash_cad' in history and history['cash_cad'].history.has_changes()
        cash_usd_changed = 'cash_usd' in history and history['cash_usd'].history.has_changes()
        
        return cash_cad_changed or cash_usd_changed
        
    def _get_accounts_holding_stock(self, symbol: str) -> List[int]:
        """获取持有指定股票的所有账户ID"""
        try:
            # 查询所有涉及该股票的交易记录
            from sqlalchemy import func
            
            account_ids = Transaction.query.filter(
                Transaction.stock == symbol
            ).with_entities(Transaction.account_id).distinct().all()
            
            return [account_id[0] for account_id in account_ids]
            
        except Exception as e:
            logger.error(f"获取持股账户时出错: {e}")
            return []
            
    def _clear_stock_related_cache(self, symbol: str, from_date: date):
        """清除股票相关缓存"""
        self.cache_manager.delete_pattern(f"stock:{symbol}:*")
        self.cache_manager.delete_pattern(f"holdings:{symbol}:*")
        logger.debug(f"清除了股票{symbol}的相关缓存")
        
    def manual_invalidate_account(self, account_id: int, from_date: date = None):
        """手动失效账户缓存（用于批量数据修改后）"""
        logger.info(f"手动失效账户{account_id}缓存，起始日期: {from_date}")
        self.cache_manager.clear_account_cache(account_id, from_date)
        
    def manual_invalidate_all_cache(self):
        """手动清除所有缓存（用于系统维护）"""
        logger.warning("手动清除所有缓存")
        self.cache_manager.cache.clear()
        
    def get_cache_stats(self) -> Dict:
        """获取缓存统计信息"""
        return {
            'total_keys': len(self.cache_manager.cache),
            'cache_keys': list(self.cache_manager.cache.keys())
        }


# 全局单例实例
_data_change_listener = None


def get_data_change_listener() -> DataChangeListener:
    """获取数据变更监听器单例"""
    global _data_change_listener
    
    if _data_change_listener is None:
        _data_change_listener = DataChangeListener()
        
    return _data_change_listener


def init_data_change_listener(app):
    """初始化数据变更监听器（在Flask应用启动时调用）"""
    with app.app_context():
        listener = get_data_change_listener()
        logger.info("数据变更监听器已初始化")
        return listener