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
        pass
    
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
        """获取当前价格 - 直接调用外部API服务（保持API缓存）"""
        try:
            from app.services.stock_price_service import StockPriceService
            price_service = StockPriceService()
            price = price_service.get_cached_stock_price(symbol, currency)
            return price
        except Exception as e:
            logger.error(f"Failed to get stock price for {symbol} ({currency}): {e}")
            return Decimal('0')
    
    def _get_sector(self, stock_info) -> str:
        """获取股票行业"""
        if hasattr(stock_info, 'category') and stock_info.category:
            return stock_info.category.name
        return 'Unknown'
    
    def get_annual_analysis(self, account_ids: List[int], 
                           years: Optional[List[int]] = None) -> Dict:
        """获取年度分析数据
        
        Args:
            account_ids: 账户ID列表
            years: 年份列表，如果为None则自动计算有交易记录的年份
            
        Returns:
            包含年度统计数据的字典
        """
        if not years:
            # 获取有交易记录的年份
            transaction_years_query = db.session.query(db.extract('year', Transaction.trade_date)).filter(
                Transaction.account_id.in_(account_ids)
            ).distinct()
            transaction_years = [int(year[0]) for year in transaction_years_query.all()]
            
            # 获取所有需要分析的年份（从最早交易年份到当前年份）
            if transaction_years:
                min_year = min(transaction_years)
                current_year = datetime.now().year
                years = list(range(min_year, current_year + 1))
        
        annual_data = []
        for year in sorted(years, reverse=True):  # 倒序排列
            year_start = date(year, 1, 1)
            year_end = date(year, 12, 31)
            
            # 使用统一的portfolio_summary获取年度数据
            year_portfolio = self.get_portfolio_summary(
                account_ids, TimePeriod.CUSTOM, year_start, year_end
            )
            
            # 计算年度交易统计
            year_transactions = Transaction.query.filter(
                Transaction.account_id.in_(account_ids),
                Transaction.trade_date >= year_start,
                Transaction.trade_date <= year_end
            ).all()
            
            # 统计交易数据
            transaction_count = len(year_transactions)
            buy_amount = sum((tx.quantity * tx.price + tx.fee) 
                           for tx in year_transactions if tx.type == 'BUY' and tx.quantity and tx.price)
            sell_amount = sum((tx.quantity * tx.price - tx.fee) 
                            for tx in year_transactions if tx.type == 'SELL' and tx.quantity and tx.price)
            
            # 按货币统计交易数据
            buy_cad = sum((tx.quantity * tx.price + tx.fee) 
                         for tx in year_transactions if tx.type == 'BUY' and tx.quantity and tx.price and tx.currency == 'CAD')
            buy_usd = sum((tx.quantity * tx.price + tx.fee) 
                         for tx in year_transactions if tx.type == 'BUY' and tx.quantity and tx.price and tx.currency == 'USD')
            sell_cad = sum((tx.quantity * tx.price - tx.fee) 
                          for tx in year_transactions if tx.type == 'SELL' and tx.quantity and tx.price and tx.currency == 'CAD')
            sell_usd = sum((tx.quantity * tx.price - tx.fee) 
                          for tx in year_transactions if tx.type == 'SELL' and tx.quantity and tx.price and tx.currency == 'USD')
            
            # 统计分红和利息（从amount字段）
            dividends = sum(tx.amount for tx in year_transactions 
                          if tx.type == 'DIVIDEND' and tx.amount)
            interest = sum(tx.amount for tx in year_transactions 
                         if tx.type == 'INTEREST' and tx.amount)
            
            # 按货币统计分红和利息
            dividends_cad = sum(tx.amount for tx in year_transactions 
                              if tx.type == 'DIVIDEND' and tx.amount and tx.currency == 'CAD')
            dividends_usd = sum(tx.amount for tx in year_transactions 
                              if tx.type == 'DIVIDEND' and tx.amount and tx.currency == 'USD')
            interest_cad = sum(tx.amount for tx in year_transactions 
                             if tx.type == 'INTEREST' and tx.amount and tx.currency == 'CAD')
            interest_usd = sum(tx.amount for tx in year_transactions 
                             if tx.type == 'INTEREST' and tx.amount and tx.currency == 'USD')
            
            # 计算年度已实现收益（从清算的持仓中统计）
            annual_realized_gain = 0
            annual_realized_gain_cad = 0
            annual_realized_gain_usd = 0
            for holding in year_portfolio.get('cleared_holdings', []):
                realized_gain = holding.get('realized_gain', 0)
                annual_realized_gain += realized_gain
                # 按货币分组已实现收益
                if holding.get('currency') == 'CAD':
                    annual_realized_gain_cad += realized_gain
                elif holding.get('currency') == 'USD':
                    annual_realized_gain_usd += realized_gain
            
            # 计算按货币分组的总资产和浮动收益
            total_assets_cad = 0
            total_assets_usd = 0
            unrealized_gain_cad = 0
            unrealized_gain_usd = 0
            
            for holding in year_portfolio.get('current_holdings', []):
                if holding.get('currency') == 'CAD':
                    total_assets_cad += holding.get('current_value', 0)
                    unrealized_gain_cad += holding.get('unrealized_gain', 0)
                elif holding.get('currency') == 'USD':
                    total_assets_usd += holding.get('current_value', 0)
                    unrealized_gain_usd += holding.get('unrealized_gain', 0)
            
            annual_data.append({
                'year': year,
                'total_assets': year_portfolio['summary']['total_current_value'],
                'annual_realized_gain': annual_realized_gain,
                'annual_unrealized_gain': year_portfolio['summary']['total_unrealized_gain'],
                'annual_dividends': dividends,
                'annual_interest': interest,
                'transaction_count': transaction_count,
                'buy_amount': buy_amount,
                'sell_amount': sell_amount,
                'currency_breakdown': {
                    'total_assets_cad': total_assets_cad,
                    'total_assets_usd': total_assets_usd,
                    'realized_gain_cad': annual_realized_gain_cad,
                    'realized_gain_usd': annual_realized_gain_usd,
                    'unrealized_gain_cad': unrealized_gain_cad,
                    'unrealized_gain_usd': unrealized_gain_usd,
                    'buy_cad': buy_cad,
                    'buy_usd': buy_usd,
                    'sell_cad': sell_cad,
                    'sell_usd': sell_usd,
                    'dividends_cad': dividends_cad,
                    'dividends_usd': dividends_usd,
                    'interest_cad': interest_cad,
                    'interest_usd': interest_usd
                }
            })
        
        # 计算图表数据
        chart_data = self._prepare_annual_chart_data(annual_data)
        
        return {
            'annual_data': annual_data,
            'chart_data': chart_data,
            'summary': {
                'years_covered': len(annual_data),
                'total_years_gain': sum(item['annual_realized_gain'] + item['annual_unrealized_gain'] 
                                      for item in annual_data),
                'total_dividends': sum(item['annual_dividends'] for item in annual_data),
                'total_interest': sum(item['annual_interest'] for item in annual_data),
                'average_annual_return': self._calculate_average_annual_return(annual_data)
            }
        }
    
    def get_quarterly_analysis(self, account_ids: List[int], 
                              years: Optional[List[int]] = None) -> Dict:
        """获取季度分析数据
        
        Args:
            account_ids: 账户ID列表
            years: 年份列表，如果为None则自动计算有交易记录的年份
            
        Returns:
            包含季度统计数据的字典
        """
        if not years:
            # 获取有交易记录的年份
            transaction_years_query = db.session.query(db.extract('year', Transaction.trade_date)).filter(
                Transaction.account_id.in_(account_ids)
            ).distinct()
            transaction_years = [int(year[0]) for year in transaction_years_query.all()]
            
            # 获取所有需要分析的年份（从最早交易年份到当前年份）
            if transaction_years:
                min_year = min(transaction_years)
                current_year = datetime.now().year
                years = list(range(min_year, current_year + 1))
        
        quarterly_data = []
        for year in sorted(years, reverse=True):
            for quarter in [4, 3, 2, 1]:  # 倒序排列
                quarter_start = date(year, (quarter - 1) * 3 + 1, 1)
                if quarter == 4:
                    quarter_end = date(year, 12, 31)
                else:
                    next_quarter_start = date(year, quarter * 3 + 1, 1)
                    quarter_end = next_quarter_start - timedelta(days=1)
                
                # 使用统一的portfolio_summary获取季度数据
                quarter_portfolio = self.get_portfolio_summary(
                    account_ids, TimePeriod.CUSTOM, quarter_start, quarter_end
                )
                
                # 计算季度交易统计
                quarter_transactions = Transaction.query.filter(
                    Transaction.account_id.in_(account_ids),
                    Transaction.trade_date >= quarter_start,
                    Transaction.trade_date <= quarter_end
                ).all()
                
                # 统计交易数据
                transaction_count = len(quarter_transactions)
                buy_amount = sum((tx.quantity * tx.price + tx.fee) 
                               for tx in quarter_transactions if tx.type == 'BUY' and tx.quantity and tx.price)
                sell_amount = sum((tx.quantity * tx.price - tx.fee) 
                                for tx in quarter_transactions if tx.type == 'SELL' and tx.quantity and tx.price)
                
                # 按货币统计交易数据
                buy_cad = sum((tx.quantity * tx.price + tx.fee) 
                             for tx in quarter_transactions if tx.type == 'BUY' and tx.quantity and tx.price and tx.currency == 'CAD')
                buy_usd = sum((tx.quantity * tx.price + tx.fee) 
                             for tx in quarter_transactions if tx.type == 'BUY' and tx.quantity and tx.price and tx.currency == 'USD')
                sell_cad = sum((tx.quantity * tx.price - tx.fee) 
                              for tx in quarter_transactions if tx.type == 'SELL' and tx.quantity and tx.price and tx.currency == 'CAD')
                sell_usd = sum((tx.quantity * tx.price - tx.fee) 
                              for tx in quarter_transactions if tx.type == 'SELL' and tx.quantity and tx.price and tx.currency == 'USD')
                
                # 统计分红和利息
                dividends = sum(tx.amount for tx in quarter_transactions 
                              if tx.type == 'DIVIDEND' and tx.amount)
                interest = sum(tx.amount for tx in quarter_transactions 
                             if tx.type == 'INTEREST' and tx.amount)
                
                # 按货币统计分红和利息
                dividends_cad = sum(tx.amount for tx in quarter_transactions 
                                  if tx.type == 'DIVIDEND' and tx.amount and tx.currency == 'CAD')
                dividends_usd = sum(tx.amount for tx in quarter_transactions 
                                  if tx.type == 'DIVIDEND' and tx.amount and tx.currency == 'USD')
                interest_cad = sum(tx.amount for tx in quarter_transactions 
                                 if tx.type == 'INTEREST' and tx.amount and tx.currency == 'CAD')
                interest_usd = sum(tx.amount for tx in quarter_transactions 
                                 if tx.type == 'INTEREST' and tx.amount and tx.currency == 'USD')
                
                # 计算季度已实现收益
                quarterly_realized_gain = 0
                quarterly_realized_gain_cad = 0
                quarterly_realized_gain_usd = 0
                for holding in quarter_portfolio.get('cleared_holdings', []):
                    realized_gain = holding.get('realized_gain', 0)
                    quarterly_realized_gain += realized_gain
                    if holding.get('currency') == 'CAD':
                        quarterly_realized_gain_cad += realized_gain
                    elif holding.get('currency') == 'USD':
                        quarterly_realized_gain_usd += realized_gain
                
                # 计算按货币分组的总资产和浮动收益
                total_assets_cad = 0
                total_assets_usd = 0
                unrealized_gain_cad = 0
                unrealized_gain_usd = 0
                
                for holding in quarter_portfolio.get('current_holdings', []):
                    if holding.get('currency') == 'CAD':
                        total_assets_cad += holding.get('current_value', 0)
                        unrealized_gain_cad += holding.get('unrealized_gain', 0)
                    elif holding.get('currency') == 'USD':
                        total_assets_usd += holding.get('current_value', 0)
                        unrealized_gain_usd += holding.get('unrealized_gain', 0)
                
                quarterly_data.append({
                    'year': year,
                    'quarter': quarter,
                    'total_assets': quarter_portfolio['summary']['total_current_value'],
                    'quarterly_realized_gain': quarterly_realized_gain,
                    'quarterly_unrealized_gain': quarter_portfolio['summary']['total_unrealized_gain'],
                    'quarterly_dividends': dividends,
                    'quarterly_interest': interest,
                    'transaction_count': transaction_count,
                    'buy_amount': buy_amount,
                    'sell_amount': sell_amount,
                    'currency_breakdown': {
                        'total_assets_cad': total_assets_cad,
                        'total_assets_usd': total_assets_usd,
                        'realized_gain_cad': quarterly_realized_gain_cad,
                        'realized_gain_usd': quarterly_realized_gain_usd,
                        'unrealized_gain_cad': unrealized_gain_cad,
                        'unrealized_gain_usd': unrealized_gain_usd,
                        'buy_cad': buy_cad,
                        'buy_usd': buy_usd,
                        'sell_cad': sell_cad,
                        'sell_usd': sell_usd,
                        'dividends_cad': dividends_cad,
                        'dividends_usd': dividends_usd,
                        'interest_cad': interest_cad,
                        'interest_usd': interest_usd
                    }
                })
        
        return {
            'quarterly_data': quarterly_data,
            'summary': {
                'quarters_covered': len(quarterly_data),
                'total_quarters_gain': sum(item['quarterly_realized_gain'] + item['quarterly_unrealized_gain'] 
                                         for item in quarterly_data),
                'total_dividends': sum(item['quarterly_dividends'] for item in quarterly_data),
                'total_interest': sum(item['quarterly_interest'] for item in quarterly_data),
                'average_quarterly_return': self._calculate_average_return(quarterly_data, 'quarterly_realized_gain', 'quarterly_unrealized_gain')
            }
        }
    
    def get_monthly_analysis(self, account_ids: List[int], 
                            months: Optional[int] = 12) -> Dict:
        """获取月度分析数据
        
        Args:
            account_ids: 账户ID列表
            months: 要分析的月数，默认12个月
            
        Returns:
            包含月度统计数据的字典
        """
        current_date = datetime.now().date()
        monthly_data = []
        
        for i in range(months):
            # 计算月份
            month_date = current_date.replace(day=1) - timedelta(days=i * 30)
            month_start = month_date.replace(day=1)
            if month_start.month == 12:
                month_end = month_start.replace(year=month_start.year + 1, month=1, day=1) - timedelta(days=1)
            else:
                month_end = month_start.replace(month=month_start.month + 1, day=1) - timedelta(days=1)
            
            # 使用统一的portfolio_summary获取月度数据
            month_portfolio = self.get_portfolio_summary(
                account_ids, TimePeriod.CUSTOM, month_start, month_end
            )
            
            # 计算月度交易统计
            month_transactions = Transaction.query.filter(
                Transaction.account_id.in_(account_ids),
                Transaction.trade_date >= month_start,
                Transaction.trade_date <= month_end
            ).all()
            
            # 统计交易数据
            transaction_count = len(month_transactions)
            buy_amount = sum((tx.quantity * tx.price + tx.fee) 
                           for tx in month_transactions if tx.type == 'BUY' and tx.quantity and tx.price)
            sell_amount = sum((tx.quantity * tx.price - tx.fee) 
                            for tx in month_transactions if tx.type == 'SELL' and tx.quantity and tx.price)
            
            # 按货币统计交易数据
            buy_cad = sum((tx.quantity * tx.price + tx.fee) 
                         for tx in month_transactions if tx.type == 'BUY' and tx.quantity and tx.price and tx.currency == 'CAD')
            buy_usd = sum((tx.quantity * tx.price + tx.fee) 
                         for tx in month_transactions if tx.type == 'BUY' and tx.quantity and tx.price and tx.currency == 'USD')
            sell_cad = sum((tx.quantity * tx.price - tx.fee) 
                          for tx in month_transactions if tx.type == 'SELL' and tx.quantity and tx.price and tx.currency == 'CAD')
            sell_usd = sum((tx.quantity * tx.price - tx.fee) 
                          for tx in month_transactions if tx.type == 'SELL' and tx.quantity and tx.price and tx.currency == 'USD')
            
            # 统计分红和利息
            dividends = sum(tx.amount for tx in month_transactions 
                          if tx.type == 'DIVIDEND' and tx.amount)
            interest = sum(tx.amount for tx in month_transactions 
                         if tx.type == 'INTEREST' and tx.amount)
            
            # 按货币统计分红和利息
            dividends_cad = sum(tx.amount for tx in month_transactions 
                              if tx.type == 'DIVIDEND' and tx.amount and tx.currency == 'CAD')
            dividends_usd = sum(tx.amount for tx in month_transactions 
                              if tx.type == 'DIVIDEND' and tx.amount and tx.currency == 'USD')
            interest_cad = sum(tx.amount for tx in month_transactions 
                             if tx.type == 'INTEREST' and tx.amount and tx.currency == 'CAD')
            interest_usd = sum(tx.amount for tx in month_transactions 
                             if tx.type == 'INTEREST' and tx.amount and tx.currency == 'USD')
            
            # 计算月度已实现收益
            monthly_realized_gain = 0
            monthly_realized_gain_cad = 0
            monthly_realized_gain_usd = 0
            for holding in month_portfolio.get('cleared_holdings', []):
                realized_gain = holding.get('realized_gain', 0)
                monthly_realized_gain += realized_gain
                if holding.get('currency') == 'CAD':
                    monthly_realized_gain_cad += realized_gain
                elif holding.get('currency') == 'USD':
                    monthly_realized_gain_usd += realized_gain
            
            # 计算按货币分组的总资产和浮动收益
            total_assets_cad = 0
            total_assets_usd = 0
            unrealized_gain_cad = 0
            unrealized_gain_usd = 0
            
            for holding in month_portfolio.get('current_holdings', []):
                if holding.get('currency') == 'CAD':
                    total_assets_cad += holding.get('current_value', 0)
                    unrealized_gain_cad += holding.get('unrealized_gain', 0)
                elif holding.get('currency') == 'USD':
                    total_assets_usd += holding.get('current_value', 0)
                    unrealized_gain_usd += holding.get('unrealized_gain', 0)
            
            monthly_data.append({
                'year': month_start.year,
                'month': month_start.month,
                'month_name': month_start.strftime('%Y-%m'),
                'total_assets': month_portfolio['summary']['total_current_value'],
                'monthly_realized_gain': monthly_realized_gain,
                'monthly_unrealized_gain': month_portfolio['summary']['total_unrealized_gain'],
                'monthly_dividends': dividends,
                'monthly_interest': interest,
                'transaction_count': transaction_count,
                'buy_amount': buy_amount,
                'sell_amount': sell_amount,
                'currency_breakdown': {
                    'total_assets_cad': total_assets_cad,
                    'total_assets_usd': total_assets_usd,
                    'realized_gain_cad': monthly_realized_gain_cad,
                    'realized_gain_usd': monthly_realized_gain_usd,
                    'unrealized_gain_cad': unrealized_gain_cad,
                    'unrealized_gain_usd': unrealized_gain_usd,
                    'buy_cad': buy_cad,
                    'buy_usd': buy_usd,
                    'sell_cad': sell_cad,
                    'sell_usd': sell_usd,
                    'dividends_cad': dividends_cad,
                    'dividends_usd': dividends_usd,
                    'interest_cad': interest_cad,
                    'interest_usd': interest_usd
                }
            })
        
        return {
            'monthly_data': monthly_data,
            'summary': {
                'months_covered': len(monthly_data),
                'total_months_gain': sum(item['monthly_realized_gain'] + item['monthly_unrealized_gain'] 
                                       for item in monthly_data),
                'total_dividends': sum(item['monthly_dividends'] for item in monthly_data),
                'total_interest': sum(item['monthly_interest'] for item in monthly_data),
                'average_monthly_return': self._calculate_average_return(monthly_data, 'monthly_realized_gain', 'monthly_unrealized_gain')
            }
        }
    
    def get_daily_analysis(self, account_ids: List[int], 
                          days: Optional[int] = 30) -> Dict:
        """获取日度分析数据
        
        Args:
            account_ids: 账户ID列表
            days: 要分析的天数，默认30天
            
        Returns:
            包含日度统计数据的字典
        """
        current_date = datetime.now().date()
        daily_data = []
        
        for i in range(days):
            analysis_date = current_date - timedelta(days=i)
            
            # 使用统一的portfolio_summary获取单日数据
            day_portfolio = self.get_portfolio_summary(
                account_ids, TimePeriod.CUSTOM, analysis_date, analysis_date
            )
            
            # 计算当日交易统计
            day_transactions = Transaction.query.filter(
                Transaction.account_id.in_(account_ids),
                Transaction.trade_date == analysis_date
            ).all()
            
            # 统计交易数据
            transaction_count = len(day_transactions)
            buy_amount = sum((tx.quantity * tx.price + tx.fee) 
                           for tx in day_transactions if tx.type == 'BUY' and tx.quantity and tx.price)
            sell_amount = sum((tx.quantity * tx.price - tx.fee) 
                            for tx in day_transactions if tx.type == 'SELL' and tx.quantity and tx.price)
            
            # 按货币统计交易数据
            buy_cad = sum((tx.quantity * tx.price + tx.fee) 
                         for tx in day_transactions if tx.type == 'BUY' and tx.quantity and tx.price and tx.currency == 'CAD')
            buy_usd = sum((tx.quantity * tx.price + tx.fee) 
                         for tx in day_transactions if tx.type == 'BUY' and tx.quantity and tx.price and tx.currency == 'USD')
            sell_cad = sum((tx.quantity * tx.price - tx.fee) 
                          for tx in day_transactions if tx.type == 'SELL' and tx.quantity and tx.price and tx.currency == 'CAD')
            sell_usd = sum((tx.quantity * tx.price - tx.fee) 
                          for tx in day_transactions if tx.type == 'SELL' and tx.quantity and tx.price and tx.currency == 'USD')
            
            # 统计分红和利息
            dividends = sum(tx.amount for tx in day_transactions 
                          if tx.type == 'DIVIDEND' and tx.amount)
            interest = sum(tx.amount for tx in day_transactions 
                         if tx.type == 'INTEREST' and tx.amount)
            
            # 按货币统计分红和利息
            dividends_cad = sum(tx.amount for tx in day_transactions 
                              if tx.type == 'DIVIDEND' and tx.amount and tx.currency == 'CAD')
            dividends_usd = sum(tx.amount for tx in day_transactions 
                              if tx.type == 'DIVIDEND' and tx.amount and tx.currency == 'USD')
            interest_cad = sum(tx.amount for tx in day_transactions 
                             if tx.type == 'INTEREST' and tx.amount and tx.currency == 'CAD')
            interest_usd = sum(tx.amount for tx in day_transactions 
                             if tx.type == 'INTEREST' and tx.amount and tx.currency == 'USD')
            
            # 计算当日已实现收益
            daily_realized_gain = 0
            daily_realized_gain_cad = 0
            daily_realized_gain_usd = 0
            for holding in day_portfolio.get('cleared_holdings', []):
                realized_gain = holding.get('realized_gain', 0)
                daily_realized_gain += realized_gain
                if holding.get('currency') == 'CAD':
                    daily_realized_gain_cad += realized_gain
                elif holding.get('currency') == 'USD':
                    daily_realized_gain_usd += realized_gain
            
            # 计算按货币分组的总资产和浮动收益
            total_assets_cad = 0
            total_assets_usd = 0
            unrealized_gain_cad = 0
            unrealized_gain_usd = 0
            
            for holding in day_portfolio.get('current_holdings', []):
                if holding.get('currency') == 'CAD':
                    total_assets_cad += holding.get('current_value', 0)
                    unrealized_gain_cad += holding.get('unrealized_gain', 0)
                elif holding.get('currency') == 'USD':
                    total_assets_usd += holding.get('current_value', 0)
                    unrealized_gain_usd += holding.get('unrealized_gain', 0)
            
            daily_data.append({
                'date': analysis_date.strftime('%Y-%m-%d'),
                'total_assets': day_portfolio['summary']['total_current_value'],
                'daily_realized_gain': daily_realized_gain,
                'daily_unrealized_gain': day_portfolio['summary']['total_unrealized_gain'],
                'daily_dividends': dividends,
                'daily_interest': interest,
                'transaction_count': transaction_count,
                'buy_amount': buy_amount,
                'sell_amount': sell_amount,
                'currency_breakdown': {
                    'total_assets_cad': total_assets_cad,
                    'total_assets_usd': total_assets_usd,
                    'realized_gain_cad': daily_realized_gain_cad,
                    'realized_gain_usd': daily_realized_gain_usd,
                    'unrealized_gain_cad': unrealized_gain_cad,
                    'unrealized_gain_usd': unrealized_gain_usd,
                    'buy_cad': buy_cad,
                    'buy_usd': buy_usd,
                    'sell_cad': sell_cad,
                    'sell_usd': sell_usd,
                    'dividends_cad': dividends_cad,
                    'dividends_usd': dividends_usd,
                    'interest_cad': interest_cad,
                    'interest_usd': interest_usd
                }
            })
        
        return {
            'daily_data': daily_data,
            'summary': {
                'days_covered': len(daily_data),
                'total_days_gain': sum(item['daily_realized_gain'] + item['daily_unrealized_gain'] 
                                     for item in daily_data),
                'total_dividends': sum(item['daily_dividends'] for item in daily_data),
                'total_interest': sum(item['daily_interest'] for item in daily_data),
                'average_daily_return': self._calculate_average_return(daily_data, 'daily_realized_gain', 'daily_unrealized_gain')
            }
        }
    
    def get_recent_30_days_analysis(self, account_ids: List[int]) -> Dict:
        """获取最近30天分析数据 - 调用日度分析"""
        return self.get_daily_analysis(account_ids, 30)
    
    
    def _prepare_annual_chart_data(self, annual_data: List[Dict]) -> Dict:
        """准备年度图表数据"""
        return {
            'years': [item['year'] for item in annual_data],
            'total_assets': [item['total_assets'] for item in annual_data],
            'annual_gains': [item['annual_realized_gain'] + item['annual_unrealized_gain'] 
                           for item in annual_data],
            'realized_gains': [item['annual_realized_gain'] for item in annual_data],
            'unrealized_gains': [item['annual_unrealized_gain'] for item in annual_data],
            'dividends': [item['annual_dividends'] for item in annual_data],
            'interest': [item['annual_interest'] for item in annual_data]
        }
    
    def _calculate_average_annual_return(self, annual_data: List[Dict]) -> float:
        """计算平均年收益率"""
        if not annual_data:
            return 0.0
        
        total_return = 0.0
        valid_years = 0
        
        for item in annual_data:
            if item['total_assets'] > 0:
                annual_return = (item['annual_realized_gain'] + item['annual_unrealized_gain']) / item['total_assets'] * 100
                total_return += annual_return
                valid_years += 1
        
        return total_return / valid_years if valid_years > 0 else 0.0
    
    def _calculate_average_return(self, data: List[Dict], realized_field: str, unrealized_field: str) -> float:
        """计算平均收益率（通用方法）"""
        if not data:
            return 0.0
        
        total_return = 0.0
        valid_periods = 0
        
        for item in data:
            if item['total_assets'] > 0:
                period_return = (item[realized_field] + item[unrealized_field]) / item['total_assets'] * 100
                total_return += period_return
                valid_periods += 1
        
        return total_return / valid_periods if valid_periods > 0 else 0.0

    def get_holdings_distribution(self, account_ids: List[int]) -> Dict:
        """获取持仓分布数据 - 为四个饼状图提供数据"""
        try:
            # 获取当前时间点的投资组合汇总
            current_date = datetime.now().date()
            summary = self.get_portfolio_summary(account_ids, TimePeriod.CUSTOM, end_date=current_date)
            
            # 使用正确的数据结构：current_holdings是直接的持仓列表
            current_holdings = summary.get('current_holdings', [])
            
            # 1. 按股票分布
            by_stocks = []
            stock_aggregation = {}
            
            for holding in current_holdings:
                stock_symbol = holding['symbol']
                current_value = float(holding['current_value'])
                
                if current_value > 0:
                    if stock_symbol in stock_aggregation:
                        stock_aggregation[stock_symbol]['value'] += current_value
                    else:
                        stock_cache = StocksCache.query.filter_by(symbol=stock_symbol).first()
                        stock_aggregation[stock_symbol] = {
                            'symbol': stock_symbol,
                            'name': stock_cache.name if stock_cache else stock_symbol,
                            'value': current_value
                        }
            
            by_stocks = list(stock_aggregation.values())
            
            # 2. 按类别分布
            by_category = defaultdict(lambda: {'value': 0, 'stocks': set()})
            
            for holding in current_holdings:
                stock_symbol = holding['symbol']
                current_value = float(holding['current_value'])
                
                if current_value > 0:
                    stock_cache = StocksCache.query.filter_by(symbol=stock_symbol).first()
                    category_name = stock_cache.category.name if (stock_cache and stock_cache.category) else 'Uncategorized'
                    
                    by_category[category_name]['value'] += current_value
                    by_category[category_name]['stocks'].add(stock_symbol)
            
            # 转换为列表格式
            by_category_list = [
                {
                    'category': category,
                    'value': data['value'],
                    'stocks_count': len(data['stocks'])
                }
                for category, data in by_category.items()
            ]
            
            # 3. 按货币分布 - 通过汇率计算
            total_value = float(summary['summary']['total_current_value'])
            
            # 计算各币种占比
            cad_value = 0
            usd_value = 0
            
            for holding in current_holdings:
                current_value = float(holding['current_value'])
                if current_value > 0:
                    # 获取股票的货币
                    stock_cache = StocksCache.query.filter_by(symbol=holding['symbol']).first()
                    currency = stock_cache.currency if stock_cache else 'CAD'
                    
                    if currency == 'CAD':
                        cad_value += current_value
                    else:  # USD
                        usd_value += current_value
            
            by_currency = []
            if cad_value > 0:
                by_currency.append({
                    'currency': 'CAD',
                    'value': cad_value
                })
            if usd_value > 0:
                by_currency.append({
                    'currency': 'USD', 
                    'value': usd_value
                })
            
            # 4. 按账户分布
            by_account = defaultdict(lambda: {'value': 0, 'holdings_count': 0})
            account_names = {}
            
            for holding in current_holdings:
                account_id = holding['account_id']
                current_value = float(holding['current_value'])
                
                if current_value > 0:
                    by_account[account_id]['value'] += current_value
                    by_account[account_id]['holdings_count'] += 1
                    
                    # 获取账户名称
                    if account_id not in account_names:
                        from app.models import Account
                        account = Account.query.get(account_id)
                        account_names[account_id] = account.name if account else f'Account {account_id}'
            
            by_account_list = [
                {
                    'account_id': account_id,
                    'account_name': account_names[account_id],
                    'value': data['value'],
                    'holdings_count': data['holdings_count']
                }
                for account_id, data in by_account.items()
                if data['value'] > 0
            ]
            
            return {
                'summary': {
                    'total_value_cad': total_value,  # 所有值已经转换为CAD
                    'unique_stocks': len(by_stocks),
                    'categories_count': len(by_category_list),
                    'accounts_count': len(by_account_list)
                },
                'by_stocks': by_stocks,
                'by_category': by_category_list,
                'by_currency': by_currency,
                'by_account': by_account_list
            }
            
        except Exception as e:
            logger.error(f"Error getting holdings distribution: {e}")
            raise

    def clear_cache(self):
        """清除缓存（无操作 - 不再使用本地缓存）"""
        pass

# 全局服务实例
portfolio_service = PortfolioService()
