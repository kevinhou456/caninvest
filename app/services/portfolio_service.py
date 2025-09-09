"""
统一投资组合服务 - 基于时间段的FIFO持仓计算

设计原则：
1. 时间段统计：支持任意时间段的增量计算
2. FIFO原则：严格按照先进先出原则计算持仓成本
3. 增量计算：时间段统计 = 期末状态 - 期初状态
4. 可扩展性：易于添加新的统计维度和时间段
5. 一致性：所有统计使用相同的计算逻辑
"""

from datetime import datetime, date, timedelta
from typing import Dict, List, Optional, Tuple, Union
from decimal import Decimal, ROUND_HALF_UP
from collections import defaultdict
from dataclasses import dataclass
from enum import Enum
import logging

from app import db
from app.models.account import Account
from app.models.transaction import Transaction
from app.models.stocks_cache import StocksCache
from app.models.price_cache import StockPriceCache

logger = logging.getLogger(__name__)


class TimePeriod(Enum):
    """时间段枚举"""
    ALL_TIME = "all_time"
    YTD = "ytd" 
    LAST_YEAR = "last_year"
    LAST_MONTH = "last_month"
    LAST_QUARTER = "last_quarter"
    LAST_30_DAYS = "last_30_days"
    LAST_90_DAYS = "last_90_days"
    LAST_365_DAYS = "last_365_days"
    CUSTOM = "custom"


@dataclass
class FIFOLot:
    """FIFO批次"""
    quantity: Decimal
    cost_per_share: Decimal
    purchase_date: date
    total_cost: Decimal
    

@dataclass
class PositionSnapshot:
    """持仓快照 - 某个时间点的持仓状态"""
    symbol: str
    account_id: int
    as_of_date: date
    
    # 基础信息
    account_name: str = ""
    currency: str = "USD"
    company_name: str = ""
    sector: str = "Unknown"
    
    # 持仓数据
    current_shares: Decimal = Decimal('0')
    total_cost: Decimal = Decimal('0')
    average_cost: Decimal = Decimal('0')
    
    # 累计交易统计
    total_bought_shares: Decimal = Decimal('0')
    total_sold_shares: Decimal = Decimal('0')
    total_bought_value: Decimal = Decimal('0')
    total_sold_value: Decimal = Decimal('0')
    
    # 收益统计
    realized_gain: Decimal = Decimal('0')
    total_dividends: Decimal = Decimal('0')
    total_interest: Decimal = Decimal('0')
    
    # 市场数据
    current_price: Decimal = Decimal('0')
    current_value: Decimal = Decimal('0')
    unrealized_gain: Decimal = Decimal('0')
    unrealized_gain_percent: Decimal = Decimal('0')
    
    # FIFO批次（内部使用）
    _fifo_lots: List[FIFOLot] = None
    
    def __post_init__(self):
        if self._fifo_lots is None:
            self._fifo_lots = []
    
    def to_dict(self) -> Dict:
        # 计算已实现收益率（用于清仓股票）
        realized_gain_percent = Decimal('0')
        if self.total_bought_value > 0:
            realized_gain_percent = (self.realized_gain / self.total_bought_value) * 100
        
        return {
            'symbol': self.symbol,
            'account_id': self.account_id,
            'account_name': self.account_name,
            'currency': self.currency,
            'company_name': self.company_name,
            'sector': self.sector,
            'shares': float(self.current_shares),  # 保持兼容性
            'current_shares': float(self.current_shares),
            'total_cost': float(self.total_cost),
            'average_cost': float(self.average_cost),
            'total_bought_shares': float(self.total_bought_shares),
            'total_sold_shares': float(self.total_sold_shares),
            'total_bought_value': float(self.total_bought_value),
            'total_sold_value': float(self.total_sold_value),
            'realized_gain': float(self.realized_gain),
            'realized_gain_percent': float(realized_gain_percent),  # 添加已实现收益率
            'dividends': float(self.total_dividends),  # 保持兼容性
            'total_dividends': float(self.total_dividends),
            'interest': float(self.total_interest),  # 保持兼容性
            'total_interest': float(self.total_interest),
            'current_price': float(self.current_price),
            'current_value': float(self.current_value),
            'unrealized_gain': float(self.unrealized_gain),
            'unrealized_gain_percent': float(self.unrealized_gain_percent),
            'as_of_date': self.as_of_date.isoformat()
        }


@dataclass
class PeriodStats:
    """时间段统计"""
    start_date: Optional[date]
    end_date: date
    period_type: TimePeriod
    
    # 持仓变动
    net_shares_change: Decimal = Decimal('0')
    net_cost_change: Decimal = Decimal('0')
    
    # 交易统计
    period_bought_shares: Decimal = Decimal('0')
    period_sold_shares: Decimal = Decimal('0')
    period_bought_value: Decimal = Decimal('0')
    period_sold_value: Decimal = Decimal('0')
    
    # 收益统计
    period_realized_gain: Decimal = Decimal('0')
    period_dividends: Decimal = Decimal('0')
    period_interest: Decimal = Decimal('0')
    
    # 市场变动
    period_price_change: Decimal = Decimal('0')
    period_value_change: Decimal = Decimal('0')
    period_unrealized_gain_change: Decimal = Decimal('0')


class PortfolioService:
    """统一投资组合服务"""
    
    def __init__(self):
        self._snapshot_cache = {}
        self._price_cache = {}
    
    def get_time_period_dates(self, period: TimePeriod, 
                            start_date: Optional[date] = None,
                            end_date: Optional[date] = None) -> Tuple[Optional[date], date]:
        """获取时间段的起始和结束日期"""
        today = date.today()
        end_date = end_date or today
        
        if period == TimePeriod.ALL_TIME:
            return None, end_date
        elif period == TimePeriod.YTD:
            return date(end_date.year, 1, 1), end_date
        elif period == TimePeriod.LAST_YEAR:
            last_year = end_date.year - 1
            return date(last_year, 1, 1), date(last_year, 12, 31)
        elif period == TimePeriod.LAST_MONTH:
            first_day_last_month = (end_date.replace(day=1) - timedelta(days=1)).replace(day=1)
            last_day_last_month = end_date.replace(day=1) - timedelta(days=1)
            return first_day_last_month, last_day_last_month
        elif period == TimePeriod.LAST_QUARTER:
            # 计算上一个季度
            quarter = (end_date.month - 1) // 3 + 1
            if quarter == 1:
                # 上一年第4季度
                return date(end_date.year - 1, 10, 1), date(end_date.year - 1, 12, 31)
            else:
                start_month = (quarter - 2) * 3 + 1
                end_month = start_month + 2
                return date(end_date.year, start_month, 1), date(end_date.year, end_month, 31)
        elif period == TimePeriod.LAST_30_DAYS:
            return end_date - timedelta(days=30), end_date
        elif period == TimePeriod.LAST_90_DAYS:
            return end_date - timedelta(days=90), end_date
        elif period == TimePeriod.LAST_365_DAYS:
            return end_date - timedelta(days=365), end_date
        elif period == TimePeriod.CUSTOM:
            return start_date, end_date
        else:
            return None, end_date

    def get_position_snapshot(self, 
                            symbol: str, 
                            account_id: int, 
                            as_of_date: date) -> PositionSnapshot:
        """获取特定时间点的持仓快照"""
        cache_key = f"{symbol}_{account_id}_{as_of_date}"
        
        if cache_key in self._snapshot_cache:
            return self._snapshot_cache[cache_key]
        
        # 获取账户信息
        account = Account.query.get(account_id)
        if not account:
            return PositionSnapshot(symbol, account_id, as_of_date)
        
        # 创建快照对象
        # 初始化时使用默认货币，实际货币将从交易记录中获取
        snapshot = PositionSnapshot(
            symbol=symbol,
            account_id=account_id,
            as_of_date=as_of_date,
            account_name=account.name,
            currency='USD'  # 默认值，将在处理交易时被实际货币覆盖
        )
        
        # 获取截止日期前的所有交易记录
        transactions = Transaction.query.filter(
            Transaction.account_id == account_id,
            Transaction.stock == symbol,
            Transaction.trade_date <= as_of_date
        ).order_by(Transaction.trade_date.asc()).all()
        
        # 处理交易记录，构建FIFO持仓
        for tx in transactions:
            self._process_transaction_fifo(snapshot, tx)
        
        # 更新市场数据和股票信息
        self._update_market_data(snapshot)
        self._update_stock_info(snapshot)
        
        self._snapshot_cache[cache_key] = snapshot
        return snapshot

    def _process_transaction_fifo(self, snapshot: PositionSnapshot, tx: Transaction):
        """使用严格的FIFO原则处理交易"""
        # 从交易记录中获取真实的货币信息
        if tx.currency:
            snapshot.currency = tx.currency
            
        if tx.type == 'BUY':
            # 买入：创建新的FIFO批次
            lot = FIFOLot(
                quantity=Decimal(str(tx.quantity)),
                cost_per_share=Decimal(str(tx.price)) + (Decimal(str(tx.fee)) / Decimal(str(tx.quantity)) if tx.quantity > 0 else Decimal('0')),
                purchase_date=tx.trade_date,
                total_cost=Decimal(str(tx.net_amount))
            )
            snapshot._fifo_lots.append(lot)
            
            # 更新持仓统计
            snapshot.current_shares += lot.quantity
            snapshot.total_cost += lot.total_cost
            snapshot.total_bought_shares += lot.quantity
            snapshot.total_bought_value += lot.total_cost
            
            # 更新平均成本
            if snapshot.current_shares > 0:
                snapshot.average_cost = snapshot.total_cost / snapshot.current_shares
        
        elif tx.type == 'SELL':
            # 卖出：使用FIFO原则出售
            remaining_to_sell = Decimal(str(tx.quantity))
            sell_proceeds = Decimal(str(tx.net_amount))
            cost_basis = Decimal('0')
            
            # 从最早的批次开始卖出
            while remaining_to_sell > 0 and snapshot._fifo_lots:
                lot = snapshot._fifo_lots[0]
                
                if lot.quantity <= remaining_to_sell:
                    # 完全卖出这个批次
                    sold_from_lot = lot.quantity
                    cost_from_lot = lot.total_cost
                    
                    remaining_to_sell -= sold_from_lot
                    cost_basis += cost_from_lot
                    snapshot.total_cost -= cost_from_lot
                    
                    # 移除这个批次
                    snapshot._fifo_lots.pop(0)
                else:
                    # 部分卖出这个批次
                    sold_from_lot = remaining_to_sell
                    cost_from_lot = sold_from_lot * lot.cost_per_share
                    
                    cost_basis += cost_from_lot
                    snapshot.total_cost -= cost_from_lot
                    
                    # 更新批次
                    lot.quantity -= sold_from_lot
                    lot.total_cost -= cost_from_lot
                    
                    remaining_to_sell = Decimal('0')
            
            # 更新持仓统计
            snapshot.current_shares -= Decimal(str(tx.quantity))
            snapshot.total_sold_shares += Decimal(str(tx.quantity))
            snapshot.total_sold_value += sell_proceeds
            
            # 计算已实现收益
            realized_gain = sell_proceeds - cost_basis
            snapshot.realized_gain += realized_gain
            
            # 更新平均成本
            if snapshot.current_shares > 0:
                snapshot.average_cost = snapshot.total_cost / snapshot.current_shares
            else:
                snapshot.average_cost = Decimal('0')
        
        elif tx.type == 'DIVIDEND':
            # 分红
            snapshot.total_dividends += Decimal(str(tx.amount or 0))
        
        elif tx.type == 'INTEREST':
            # 利息
            snapshot.total_interest += Decimal(str(tx.amount or 0))

    def get_portfolio_summary(self, 
                            account_ids: List[int],
                            period: TimePeriod = TimePeriod.ALL_TIME,
                            start_date: Optional[date] = None,
                            end_date: Optional[date] = None) -> Dict:
        """获取投资组合汇总 - 支持任意时间段"""
        period_start, period_end = self.get_time_period_dates(period, start_date, end_date)
        
        current_holdings = []
        cleared_holdings = []
        
        for account_id in account_ids:
            # 获取该账户的所有股票
            symbols = db.session.query(Transaction.stock).filter(
                Transaction.account_id == account_id,
                Transaction.stock.isnot(None)
            ).distinct().all()
            
            for (symbol,) in symbols:
                if symbol is None:
                    continue
                    
                # 获取期末快照
                position = self.get_position_snapshot(symbol, account_id, period_end)
                position_dict = position.to_dict()
                
                if position.current_shares > 0:
                    current_holdings.append(position_dict)
                elif position.total_sold_shares > 0:
                    cleared_holdings.append(position_dict)
        
        # 汇总统计
        total_current_value = sum(Decimal(str(h['current_value'])) for h in current_holdings)
        total_cost = sum(Decimal(str(h['total_cost'])) for h in current_holdings)
        total_unrealized_gain = sum(Decimal(str(h['unrealized_gain'])) for h in current_holdings)
        
        return {
            'period_info': {
                'period_type': period.value,
                'start_date': period_start.isoformat() if period_start else None,
                'end_date': period_end.isoformat(),
            },
            'current_holdings': current_holdings,
            'cleared_holdings': cleared_holdings,
            'summary': {
                'total_current_value': float(total_current_value),
                'total_cost': float(total_cost),
                'total_unrealized_gain': float(total_unrealized_gain),
                'total_return_percent': float((total_unrealized_gain / total_cost * 100)) if total_cost > 0 else 0
            }
        }
        

    def _update_market_data(self, snapshot: PositionSnapshot):
        """更新市场数据"""
        current_price = self._get_current_price(snapshot.symbol, snapshot.currency)
        if current_price:
            snapshot.current_price = current_price
            snapshot.current_value = snapshot.current_shares * current_price
            snapshot.unrealized_gain = snapshot.current_value - snapshot.total_cost
            
            if snapshot.total_cost > 0:
                snapshot.unrealized_gain_percent = (snapshot.unrealized_gain / snapshot.total_cost) * 100
    
    def _update_stock_info(self, snapshot: PositionSnapshot):
        """更新股票信息"""
        stock_info = StocksCache.query.filter_by(symbol=snapshot.symbol, currency=snapshot.currency).first()
        if stock_info:
            snapshot.company_name = stock_info.name or snapshot.symbol
            snapshot.sector = self._get_sector(stock_info)
            # 货币信息只从交易记录中获取，不从stocks_cache中获取
    
    def _get_current_price(self, symbol: str, currency: str) -> Optional[Decimal]:
        """获取当前价格 - 使用统一的缓存机制"""
        cache_key = f"{symbol}_{currency}"
        if cache_key in self._price_cache:
            return self._price_cache[cache_key]
        
        try:
            from app.services.stock_price_service import StockPriceService
            price_service = StockPriceService()
            price = price_service.get_cached_stock_price(symbol, currency)
            self._price_cache[cache_key] = price
            return price
        except Exception as e:
            logger.error(f"Failed to get stock price for {symbol} ({currency}): {e}")
            return Decimal('0')
    
    def _get_sector(self, stock_info) -> str:
        """获取股票行业"""
        if hasattr(stock_info, 'category') and stock_info.category:
            return stock_info.category.name
        return 'Unknown'
    
    def clear_cache(self):
        """清除缓存"""
        self._snapshot_cache.clear()
        self._price_cache.clear()
# 全局服务实例
portfolio_service = PortfolioService()
