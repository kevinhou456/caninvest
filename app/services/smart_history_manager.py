"""
智能历史数据管理器
根据交易历史动态确定需要获取的历史价格数据范围
"""

from datetime import datetime, date, timedelta
from typing import List, Dict, Optional, Tuple
from app.models.transaction import Transaction
from app.services.stock_history_cache_service import StockHistoryCacheService


class SmartHistoryManager:
    """
    智能历史数据管理器
    
    设计原则:
    1. 智能确定日期范围：根据实际交易历史，而不是固定365天
    2. 高效缓存利用：只获取必要的数据，避免重复请求
    3. 可扩展性：易于添加新的优化策略
    4. 易于维护：清晰的职责分离，无重复代码
    """
    
    def __init__(self):
        self.cache_service = StockHistoryCacheService()
        self.default_buffer_days = 30  # 在交易日期前后添加的缓冲天数
        
    def get_optimal_date_range(self, stock_symbol: str, transactions: List[Transaction] = None,
                              family_id: int = None, member_id: int = None, 
                              account_id: int = None) -> Tuple[date, date]:
        """
        根据交易历史智能确定最优的历史数据日期范围
        
        Args:
            stock_symbol: 股票代码
            transactions: 交易记录（可选，如果提供则直接使用）
            family_id: 家庭ID（用于查询交易）
            member_id: 成员ID（用于过滤交易）
            account_id: 账户ID（用于过滤交易）
            
        Returns:
            Tuple[date, date]: (开始日期, 结束日期)
        """
        # 如果没有提供交易记录，则查询
        if transactions is None:
            transactions = self._get_transactions(stock_symbol, family_id, member_id, account_id)
        
        # 如果没有交易记录，返回默认范围（最近1年）
        if not transactions:
            end_date = date.today()
            start_date = end_date - timedelta(days=365)
            return start_date, end_date
        
        # 找到最早和最晚的交易日期
        trade_dates = [t.trade_date for t in transactions if t.trade_date]
        if not trade_dates:
            # 如果没有有效的交易日期，返回默认范围
            end_date = date.today()
            start_date = end_date - timedelta(days=365)
            return start_date, end_date
            
        earliest_trade = min(trade_dates)
        latest_trade = max(trade_dates)
        
        # 计算智能日期范围
        start_date = earliest_trade - timedelta(days=self.default_buffer_days)
        
        # 结束日期：最晚交易日期之后的缓冲天数，或今天（取较晚的）
        end_date = max(
            latest_trade + timedelta(days=self.default_buffer_days),
            date.today()
        )
        
        return start_date, end_date
    
    def get_historical_data_for_stock(self, stock_symbol: str, transactions: List[Transaction] = None,
                                     family_id: int = None, member_id: int = None, 
                                     account_id: int = None, currency: str = None) -> List[Dict]:
        """
        获取股票的智能历史数据
        
        Args:
            stock_symbol: 股票代码
            transactions: 交易记录（可选）
            family_id: 家庭ID
            member_id: 成员ID
            account_id: 账户ID
            currency: 货币类型（可选，会自动推断）
            
        Returns:
            List[Dict]: 历史价格数据列表
        """
        stock_symbol = stock_symbol.upper()
        
        # 获取或查询交易记录
        if transactions is None:
            transactions = self._get_transactions(stock_symbol, family_id, member_id, account_id)
        
        # 智能确定日期范围
        start_date, end_date = self.get_optimal_date_range(stock_symbol, transactions)
        
        # 智能推断货币类型
        if not currency and transactions:
            currency = self._infer_currency(stock_symbol, transactions)
        elif not currency:
            # 使用Transaction模型的方法从交易记录中获取币种
            currency = Transaction.get_currency_by_stock_symbol(stock_symbol)
            if not currency:
                currency = 'USD'  # 默认值
        
        # 获取缓存的历史数据
        try:
            # 首先检查是否需要强制刷新（缺少早期数据）
            force_refresh = self._should_force_refresh(stock_symbol, start_date, end_date, currency)
            print(f"智能历史管理器: {stock_symbol} force_refresh={force_refresh} (请求范围: {start_date} 到 {end_date})")
            
            historical_data = self.cache_service.get_cached_history(
                stock_symbol, start_date, end_date, currency, force_refresh=force_refresh
            )
            
            if historical_data:
                print(f"智能历史数据管理器: 成功获取 {stock_symbol} 从 {start_date} 到 {end_date} 的 {len(historical_data)} 条记录")
                print(f"  - 基于 {len(transactions)} 笔交易记录")
                print(f"  - 日期范围优化: 最早交易 {min([t.trade_date for t in transactions]) if transactions else 'N/A'}")
            else:
                print(f"智能历史数据管理器: 未找到 {stock_symbol} 的历史数据")
                
            return historical_data or []
            
        except Exception as e:
            print(f"智能历史数据获取失败 {stock_symbol}: {e}")
            return []
    
    def _get_transactions(self, stock_symbol: str, family_id: int = None, 
                         member_id: int = None, account_id: int = None) -> List[Transaction]:
        """
        查询指定股票的交易记录
        
        Args:
            stock_symbol: 股票代码
            family_id: 家庭ID
            member_id: 成员ID
            account_id: 账户ID
            
        Returns:
            List[Transaction]: 交易记录列表
        """
        query = Transaction.query.filter_by(stock=stock_symbol.upper())
        
        # 应用过滤器
        if family_id:
            query = query.join(Transaction.account).filter_by(family_id=family_id)
        
        if member_id:
            from app.models.account import AccountMember
            query = query.join(Transaction.account).join(AccountMember)\
                         .filter(AccountMember.member_id == member_id)
        
        if account_id:
            query = query.filter(Transaction.account_id == account_id)
        
        return query.order_by(Transaction.trade_date.asc()).all()
    
    def _infer_currency(self, stock_symbol: str, transactions: List[Transaction]) -> str:
        """
        从交易记录中智能推断货币类型
        
        Args:
            stock_symbol: 股票代码
            transactions: 交易记录
            
        Returns:
            str: 货币代码
        """
        # 优先从交易记录获取货币
        for transaction in transactions:
            if transaction.currency:
                return transaction.currency
        
        # 使用Transaction模型的方法从交易记录中获取币种
        currency = Transaction.get_currency_by_stock_symbol(stock_symbol)
        return currency if currency else 'USD'  # 默认值
    
    def _should_force_refresh(self, stock_symbol: str, start_date: date, end_date: date, currency: str) -> bool:
        """
        检查是否应该强制刷新缓存
        
        Args:
            stock_symbol: 股票代码
            start_date: 请求的开始日期
            end_date: 请求的结束日期
            currency: 货币类型
            
        Returns:
            bool: 是否需要强制刷新
        """
        from app.models.stock_price_history import StockPriceHistory
        
        # 获取缓存中的最早日期
        earliest_cached = StockPriceHistory.query.filter_by(
            symbol=stock_symbol, currency=currency
        ).order_by(StockPriceHistory.trade_date.asc()).first()
        
        if not earliest_cached:
            # 没有缓存数据，需要强制刷新
            print(f"智能历史管理器: {stock_symbol} 无缓存数据，强制刷新")
            return True
        
        # 如果请求的开始日期比缓存最早日期还早，需要强制刷新
        if start_date < earliest_cached.trade_date:
            print(f"智能历史管理器: {stock_symbol} 需要更早数据 (请求:{start_date} vs 缓存最早:{earliest_cached.trade_date})，强制刷新")
            return True
            
        # 检查数据缺口
        total_days_requested = (end_date - start_date).days + 1
        cached_count = StockPriceHistory.query.filter(
            StockPriceHistory.symbol == stock_symbol,
            StockPriceHistory.currency == currency,
            StockPriceHistory.trade_date >= start_date,
            StockPriceHistory.trade_date <= end_date
        ).count()
        
        # 如果缓存覆盖率低于60%，强制刷新
        coverage = cached_count / max(total_days_requested * 5 / 7, 1)  # 估算交易日
        if coverage < 0.6:
            print(f"智能历史管理器: {stock_symbol} 缓存覆盖率过低 ({coverage:.1%})，强制刷新")
            return True
        
        return False
    
    def get_date_range_summary(self, stock_symbol: str, transactions: List[Transaction] = None,
                              **filters) -> Dict:
        """
        获取日期范围摘要信息（用于调试和监控）
        
        Args:
            stock_symbol: 股票代码
            transactions: 交易记录
            **filters: 其他过滤参数
            
        Returns:
            Dict: 包含日期范围分析的摘要
        """
        if transactions is None:
            transactions = self._get_transactions(stock_symbol, **filters)
        
        start_date, end_date = self.get_optimal_date_range(stock_symbol, transactions)
        
        trade_dates = [t.trade_date for t in transactions if t.trade_date]
        
        return {
            'stock_symbol': stock_symbol,
            'transaction_count': len(transactions),
            'date_range': {
                'start_date': start_date.isoformat(),
                'end_date': end_date.isoformat(),
                'total_days': (end_date - start_date).days
            },
            'trade_dates': {
                'earliest': min(trade_dates).isoformat() if trade_dates else None,
                'latest': max(trade_dates).isoformat() if trade_dates else None,
                'span_days': (max(trade_dates) - min(trade_dates)).days if len(trade_dates) > 1 else 0
            },
            'optimization': {
                'buffer_days': self.default_buffer_days,
                'saved_days': max(0, 365 - (end_date - start_date).days) if len(trade_dates) > 0 else 0
            }
        }