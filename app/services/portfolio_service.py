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
from calendar import monthrange
from typing import Dict, List, Optional, Tuple, Union
from decimal import Decimal, ROUND_HALF_UP, InvalidOperation
from collections import defaultdict
from dataclasses import dataclass
from enum import Enum
import logging

from app import db
from app.models.account import Account, AccountMember
from app.models.transaction import Transaction
from app.models.stocks_cache import StocksCache
from app.models.price_cache import StockPriceCache
from app.services.stock_history_cache_service import StockHistoryCacheService
from app.services.currency_service import currency_service
from app.services.asset_valuation_service import AssetValuationService

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
        self.history_cache_service = StockHistoryCacheService()
        self._benchmark_cache: Dict[Tuple[str, date, date], List[float]] = {}
    
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
        # 初始化时不能使用默认货币，如果交易区间没有交易记录会搞错股票的币种
        currency = Transaction.get_currency_by_stock_symbol(symbol)

        snapshot = PositionSnapshot(
            symbol=symbol,
            account_id=account_id,
            as_of_date=as_of_date,
            account_name=account.name,
            currency=currency  # 默认值，将在处理交易时被实际货币覆盖
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
                Transaction.stock.isnot(None),
                Transaction.stock != ''
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
            from app.models import StocksCache
            
            price_service = StockPriceService()

          
            
            #所有股票价格都是用stock price service获取的，所以货币就是传入的货币
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
                           years: Optional[List[int]] = None,
                           member_id: Optional[int] = None) -> Dict:
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
        exchange_rate_value = currency_service.get_current_rate('USD', 'CAD')
        if not exchange_rate_value:
            exchange_rate_value = 1
        exchange_rate_decimal = Decimal(str(exchange_rate_value))

        asset_service = AssetValuationService()

        ownership_map: Dict[int, Decimal] = {}
        if member_id:
            memberships = AccountMember.query.filter_by(member_id=member_id).all()
            for membership in memberships:
                try:
                    ownership_map[membership.account_id] = Decimal(str(membership.ownership_percentage or 0)) / Decimal('100')
                except (InvalidOperation, TypeError):
                    ownership_map[membership.account_id] = Decimal('0')

        def get_proportion(account_id: int) -> Decimal:
            return ownership_map.get(account_id, Decimal('1')) if ownership_map else Decimal('1')

        annual_data = []
        if not years:
            years = []  # Set empty list if no years found
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
            buy_amount = sell_amount = Decimal('0')
            buy_cad = buy_usd = Decimal('0')
            sell_cad = sell_usd = Decimal('0')
            dividends = interest = Decimal('0')
            dividends_cad = dividends_usd = Decimal('0')
            interest_cad = interest_usd = Decimal('0')

            for tx in year_transactions:
                tx_type = (tx.type or '').upper()
                tx_currency = (tx.currency or '').upper()
                proportion = get_proportion(tx.account_id)
                if proportion <= 0:
                    continue

                quantity = Decimal(str(tx.quantity or 0))
                price = Decimal(str(tx.price or 0))
                fee = Decimal(str(tx.fee or 0))
                amount = Decimal(str(tx.amount or 0))

                if tx_type == 'BUY' and quantity and price:
                    gross = (quantity * price + fee) * proportion
                    buy_amount += gross
                    if tx_currency == 'CAD':
                        buy_cad += gross
                    elif tx_currency == 'USD':
                        buy_usd += gross
                elif tx_type == 'SELL' and quantity and price:
                    net = (quantity * price - fee) * proportion
                    sell_amount += net
                    if tx_currency == 'CAD':
                        sell_cad += net
                    elif tx_currency == 'USD':
                        sell_usd += net
                elif tx_type == 'DIVIDEND' and amount:
                    value = amount * proportion
                    dividends += value
                    if tx_currency == 'CAD':
                        dividends_cad += value
                    elif tx_currency == 'USD':
                        dividends_usd += value
                elif tx_type == 'INTEREST' and amount:
                    value = amount * proportion
                    interest += value
                    if tx_currency == 'CAD':
                        interest_cad += value
                    elif tx_currency == 'USD':
                        interest_usd += value
            
            # 计算年度已实现收益（从清算的持仓中统计）
            annual_realized_gain = Decimal('0')
            annual_realized_gain_cad = Decimal('0')
            annual_realized_gain_usd = Decimal('0')
            for holding in year_portfolio.get('cleared_holdings', []):
                proportion = get_proportion(holding.get('account_id'))
                if proportion <= 0:
                    continue
                realized_gain = Decimal(str(holding.get('realized_gain', 0))) * proportion
                annual_realized_gain += realized_gain
                # 按货币分组已实现收益
                if holding.get('currency') == 'CAD':
                    annual_realized_gain_cad += realized_gain
                elif holding.get('currency') == 'USD':
                    annual_realized_gain_usd += realized_gain
            
            # 计算按货币分组的总资产和浮动收益
            total_assets_cad = Decimal('0')
            total_assets_usd = Decimal('0')
            
            current_holdings = year_portfolio.get('current_holdings', [])
            total_assets_value = Decimal('0')
            for holding in current_holdings:
                proportion = get_proportion(holding.get('account_id'))
                if proportion <= 0:
                    continue
                value_dec = Decimal(str(holding.get('current_value', 0))) * proportion
                total_assets_value += value_dec
                currency = (holding.get('currency') or '').upper()
                if currency == 'CAD':
                    total_assets_cad += value_dec
                elif currency == 'USD':
                    total_assets_usd += value_dec

            annual_unrealized_gain_dec, currency_gain_map = self._calculate_quarterly_unrealized_gain(
                current_holdings,
                year_start,
                year_end,
                ownership_map
            ) if current_holdings else (Decimal('0'), {'CAD': Decimal('0'), 'USD': Decimal('0')})

            annual_unrealized_gain = float(annual_unrealized_gain_dec)
            unrealized_gain_cad = float(currency_gain_map.get('CAD', Decimal('0')))
            unrealized_gain_usd = float(currency_gain_map.get('USD', Decimal('0')))

            year_cash_cad = Decimal('0')
            year_cash_usd = Decimal('0')
            for account_id in account_ids:
                proportion = get_proportion(account_id)
                if proportion <= 0:
                    continue
                snapshot = asset_service.get_asset_snapshot(account_id, year_end)
                year_cash_cad += snapshot.cash_balance_cad * proportion
                year_cash_usd += snapshot.cash_balance_usd * proportion

            total_assets_dec = total_assets_value + year_cash_cad + year_cash_usd * exchange_rate_decimal

            annual_data.append({
                'year': year,
                'total_assets': float(total_assets_dec),
                'annual_realized_gain': float(annual_realized_gain),
                'annual_unrealized_gain': annual_unrealized_gain,
                'annual_dividends': float(dividends),
                'annual_interest': float(interest),
                'annual_income': float(dividends + interest),
                'transaction_count': transaction_count,
                'buy_amount': float(buy_amount),
                'sell_amount': float(sell_amount),
                'currency_breakdown': {
                    'total_assets_cad': float(total_assets_cad + year_cash_cad),
                    'total_assets_usd': float(total_assets_usd + year_cash_usd),
                    'realized_gain_cad': float(annual_realized_gain_cad),
                    'realized_gain_usd': float(annual_realized_gain_usd),
                    'unrealized_gain_cad': unrealized_gain_cad,
                    'unrealized_gain_usd': unrealized_gain_usd,
                    'buy_cad': float(buy_cad),
                    'buy_usd': float(buy_usd),
                    'sell_cad': float(sell_cad),
                    'sell_usd': float(sell_usd),
                    'dividends_cad': float(dividends_cad),
                    'dividends_usd': float(dividends_usd),
                    'interest_cad': float(interest_cad),
                    'interest_usd': float(interest_usd),
                    'cash_cad': float(year_cash_cad),
                    'cash_usd': float(year_cash_usd)
                },
                'cash_balance': {
                    'cad': float(year_cash_cad),
                    'usd': float(year_cash_usd)
                }
            })

        # 计算图表数据
        chart_data = self._prepare_annual_chart_data(annual_data)

        total_realized_sum = sum(item.get('annual_realized_gain', 0) or 0 for item in annual_data)
        total_unrealized_sum = sum(item.get('annual_unrealized_gain', 0) or 0 for item in annual_data)
        total_income_sum = sum((item.get('annual_dividends', 0) or 0) + (item.get('annual_interest', 0) or 0)
                               for item in annual_data)

        if annual_data:
            latest_entry = max(annual_data, key=lambda x: x.get('year', 0))
            latest_cash = latest_entry.get('cash_balance', {})
            latest_cash_cad = latest_cash.get('cad', 0.0) or 0.0
            latest_cash_usd = latest_cash.get('usd', 0.0) or 0.0
            cash_balance_summary = {
                'cad': latest_cash_cad,
                'usd': latest_cash_usd,
                'total_cad': latest_cash_cad + latest_cash_usd * float(exchange_rate_decimal)
            }
            current_assets_value = latest_entry.get('total_assets', 0.0) or 0.0
        else:
            cash_balance_summary = {'cad': 0.0, 'usd': 0.0, 'total_cad': 0.0}
            current_assets_value = 0.0

        return {
            'annual_data': annual_data,
            'chart_data': chart_data,
            'summary': {
                'years_covered': len(annual_data),
                'total_years_gain': sum(item['annual_realized_gain'] + item['annual_unrealized_gain'] 
                                      for item in annual_data),
                'total_dividends': sum(item['annual_dividends'] for item in annual_data),
                'total_interest': sum(item['annual_interest'] for item in annual_data),
                'average_annual_return': self._calculate_average_annual_return(annual_data),
                'total_realized_gain': total_realized_sum,
                'total_unrealized_gain': total_unrealized_sum,
                'total_income': total_income_sum,
                'current_assets': current_assets_value,
                'cash_balance': cash_balance_summary
            },
            'cash_balance': cash_balance_summary
        }
    
    def get_quarterly_analysis(self, account_ids: List[int], 
                              years: Optional[List[int]] = None,
                              member_id: Optional[int] = None) -> Dict:
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

        if not years:
            years = [datetime.now().year]
        else:
            years = sorted(set(years))

        ownership_map: Dict[int, Decimal] = {}
        if member_id:
            memberships = AccountMember.query.filter_by(member_id=member_id).all()
            for membership in memberships:
                try:
                    ownership_map[membership.account_id] = Decimal(str(membership.ownership_percentage or 0)) / Decimal('100')
                except (InvalidOperation, TypeError):
                    ownership_map[membership.account_id] = Decimal('0')

        quarterly_data = []
        today = date.today()
        for year in sorted(years, reverse=True):
            for quarter in [4, 3, 2, 1]:  # 倒序排列
                quarter_start = date(year, (quarter - 1) * 3 + 1, 1)
                if quarter == 4:
                    quarter_end = date(year, 12, 31)
                else:
                    next_quarter_start = date(year, quarter * 3 + 1, 1)
                    quarter_end = next_quarter_start - timedelta(days=1)

                if quarter_start > today:
                    continue

                effective_end = min(quarter_end, today)

                # 使用统一的portfolio_summary获取季度数据
                quarter_portfolio = self.get_portfolio_summary(
                    account_ids, TimePeriod.CUSTOM, quarter_start, effective_end
                )

                def get_proportion(account_id: int) -> Decimal:
                    return ownership_map.get(account_id, Decimal('1')) if ownership_map else Decimal('1')

                # 计算季度交易统计
                quarter_transactions = Transaction.query.filter(
                    Transaction.account_id.in_(account_ids),
                    Transaction.trade_date >= quarter_start,
                    Transaction.trade_date <= effective_end
                ).all()

                # 统计交易与收益数据（兼容大小写）
                transaction_count = len(quarter_transactions)
                buy_amount = sell_amount = 0.0
                buy_cad = buy_usd = 0.0
                sell_cad = sell_usd = 0.0
                dividends = interest = 0.0
                dividends_cad = dividends_usd = 0.0
                interest_cad = interest_usd = 0.0

                for tx in quarter_transactions:
                    tx_type = (tx.type or '').upper()
                    tx_currency = (tx.currency or '').upper()
                    proportion = float(get_proportion(tx.account_id))

                    quantity = float(tx.quantity or 0)
                    price = float(tx.price or 0)
                    fee = float(tx.fee or 0)
                    amount = float(tx.amount or 0)

                    if tx_type == 'BUY' and quantity and price:
                        gross = (quantity * price + fee) * proportion
                        buy_amount += gross
                        if tx_currency == 'CAD':
                            buy_cad += gross
                        elif tx_currency == 'USD':
                            buy_usd += gross
                    elif tx_type == 'SELL' and quantity and price:
                        net = (quantity * price - fee) * proportion
                        sell_amount += net
                        if tx_currency == 'CAD':
                            sell_cad += net
                        elif tx_currency == 'USD':
                            sell_usd += net
                    elif tx_type == 'DIVIDEND' and amount:
                        dividend_value = amount * proportion
                        dividends += dividend_value
                        if tx_currency == 'CAD':
                            dividends_cad += dividend_value
                        elif tx_currency == 'USD':
                            dividends_usd += dividend_value
                    elif tx_type == 'INTEREST' and amount:
                        interest_value = amount * proportion
                        interest += interest_value
                        if tx_currency == 'CAD':
                            interest_cad += interest_value
                        elif tx_currency == 'USD':
                            interest_usd += interest_value

                # 计算季度已实现收益
                quarterly_realized_gain = 0
                quarterly_realized_gain_cad = 0
                quarterly_realized_gain_usd = 0
                for holding in quarter_portfolio.get('cleared_holdings', []):
                    realized_gain = holding.get('realized_gain', 0) * float(get_proportion(holding.get('account_id')))
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
                
                current_holdings = quarter_portfolio.get('current_holdings', [])
                total_assets = Decimal('0')
                for holding in current_holdings:
                    proportion_dec = get_proportion(holding.get('account_id'))
                    value_dec = Decimal(str(holding.get('current_value', 0))) * proportion_dec
                    total_assets += value_dec
                    if holding.get('currency') == 'CAD':
                        total_assets_cad += float(value_dec)
                    elif holding.get('currency') == 'USD':
                        total_assets_usd += float(value_dec)

                quarterly_unrealized_gain_dec, currency_gain_map = self._calculate_quarterly_unrealized_gain(
                    current_holdings,
                    quarter_start,
                    effective_end,
                    ownership_map
                )

                quarterly_unrealized_gain = float(quarterly_unrealized_gain_dec)
                unrealized_gain_cad = float(currency_gain_map.get('CAD', Decimal('0')))
                unrealized_gain_usd = float(currency_gain_map.get('USD', Decimal('0')))

                quarterly_data.append({
                    'year': year,
                    'quarter': quarter,
                    'total_assets': float(total_assets),
                    'quarterly_realized_gain': quarterly_realized_gain,
                    'quarterly_unrealized_gain': quarterly_unrealized_gain,
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

    def _calculate_quarterly_unrealized_gain(self,
                                            holdings: List[Dict],
                                            quarter_start: date,
                                            quarter_end: date,
                                            ownership_map: Dict[int, Decimal]) -> Tuple[Decimal, Dict[str, Decimal]]:
        total_gain = Decimal('0')
        per_currency_gain: Dict[str, Decimal] = defaultdict(lambda: Decimal('0'))

        prev_cutoff = quarter_start - timedelta(days=1)

        for holding in holdings:
            account_id = holding.get('account_id')
            symbol = holding.get('symbol')
            currency = (holding.get('currency') or 'USD').upper()
            current_shares = Decimal(str(holding.get('current_shares', 0)))

            if not symbol or not account_id or current_shares <= 0:
                continue

            gain = self._calculate_holding_quarterly_unrealized_gain(
                account_id,
                symbol,
                currency,
                quarter_start,
                quarter_end,
                prev_cutoff,
                holding,
                ownership_map.get(account_id, Decimal('1')) if ownership_map else Decimal('1')
            )

            total_gain += gain
            per_currency_gain[currency] += gain

        return total_gain, per_currency_gain

    def _calculate_holding_quarterly_unrealized_gain(self,
                                                    account_id: int,
                                                    symbol: str,
                                                    currency: str,
                                                    quarter_start: date,
                                                    quarter_end: date,
                                                    prev_cutoff: date,
                                                    holding: Dict,
                                                    proportion: Decimal) -> Decimal:
        from app.models.transaction import Transaction

        transactions = Transaction.query.filter(
            Transaction.account_id == account_id,
            Transaction.stock == symbol,
            Transaction.trade_date <= quarter_end
        ).order_by(Transaction.trade_date.asc(), Transaction.id.asc()).all()

        if not transactions:
            return Decimal('0')

        price_end = self._get_last_trading_price(symbol, currency, quarter_end)
        if price_end is None:
            current_price = holding.get('current_price')
            price_end = Decimal(str(current_price)) if current_price else Decimal('0')
        if price_end is None or price_end == 0:
            return Decimal('0')

        prev_price = self._get_last_trading_price(symbol, currency, prev_cutoff)

        lots: List[Dict[str, Decimal]] = []

        def add_lot(quantity: Decimal, base_price: Optional[Decimal], cost_per_share: Decimal):
            if quantity <= 0:
                return
            lots.append({
                'quantity': quantity,
                'base_price': base_price,
                'cost_per_share': cost_per_share
            })

        def remove_quantity(quantity: Decimal):
            remaining = quantity
            while remaining > 0 and lots:
                lot = lots[0]
                lot_quantity = lot['quantity']
                if lot_quantity > remaining:
                    lot['quantity'] = lot_quantity - remaining
                    remaining = Decimal('0')
                else:
                    remaining -= lot_quantity
                    lots.pop(0)

        def compute_cost_per_share(tx) -> Decimal:
            qty = Decimal(str(tx.quantity or 0))
            if qty == 0:
                return Decimal('0')
            price = Decimal(str(tx.price or 0))
            fee = Decimal(str(tx.fee or 0))
            return price + (fee / qty)

        pre_transactions = [tx for tx in transactions if tx.trade_date < quarter_start]
        in_quarter_transactions = [tx for tx in transactions if quarter_start <= tx.trade_date <= quarter_end]

        for tx in pre_transactions:
            qty = Decimal(str(tx.quantity or 0))
            if qty <= 0:
                continue
            tx_type = (tx.type or '').upper()
            if tx_type == 'BUY':
                cost_per_share = compute_cost_per_share(tx)
                add_lot(qty, None, cost_per_share)
            elif tx_type == 'SELL':
                remove_quantity(qty)

        for lot in lots:
            base_price = prev_price if prev_price is not None else lot['cost_per_share']
            lot['base_price'] = base_price

        for tx in in_quarter_transactions:
            qty = Decimal(str(tx.quantity or 0))
            if qty <= 0:
                continue
            tx_type = (tx.type or '').upper()
            if tx_type == 'BUY':
                cost_per_share = compute_cost_per_share(tx)
                add_lot(qty, cost_per_share, cost_per_share)
            elif tx_type == 'SELL':
                remove_quantity(qty)

        gain = Decimal('0')
        for lot in lots:
            base_price = lot.get('base_price')
            if base_price is None:
                base_price = lot.get('cost_per_share', Decimal('0'))
            gain += lot['quantity'] * (price_end - base_price)

        return gain * proportion

    def _get_last_trading_price(self, symbol: str, currency: str, target_date: date) -> Optional[Decimal]:
        if not symbol or not target_date:
            return None
        effective_end = min(target_date, date.today())
        window_start = effective_end - timedelta(days=14)
        history = self.history_cache_service.get_cached_history(symbol, window_start, effective_end, currency)
        if not history:
            return None
        history_sorted = sorted(
            (record for record in history if record.get('date') and record.get('close') is not None),
            key=lambda r: r['date'],
            reverse=True
        )
        for record in history_sorted:
            try:
                record_date = datetime.fromisoformat(record['date']).date()
            except ValueError:
                continue
            if record_date <= effective_end:
                return Decimal(str(record['close']))
        return None

    def _shift_month(self, base_month_start: date, offset: int) -> Optional[date]:
        if base_month_start is None:
            return None
        year = base_month_start.year
        month = base_month_start.month - offset
        while month <= 0:
            month += 12
            year -= 1
        if year < 1:
            return None
        return date(year, month, 1)
    
    def get_monthly_analysis(self, account_ids: List[int], 
                            months: Optional[int] = 12,
                            member_id: Optional[int] = None) -> Dict:
        """获取月度分析数据
        
        Args:
            account_ids: 账户ID列表
            months: 要分析的月数，默认12个月
            
        Returns:
            包含月度统计数据的字典
        """
        if not months:
            months = 12

        today = date.today()
        base_month_start = today.replace(day=1)
        monthly_data = []

        ownership_map: Dict[int, Decimal] = {}
        if member_id:
            memberships = AccountMember.query.filter_by(member_id=member_id).all()
            for membership in memberships:
                try:
                    ownership_map[membership.account_id] = Decimal(str(membership.ownership_percentage or 0)) / Decimal('100')
                except (InvalidOperation, TypeError):
                    ownership_map[membership.account_id] = Decimal('0')

        for offset in range(months):
            target_start = self._shift_month(base_month_start, offset)
            if not target_start:
                continue

            _, days_in_month = monthrange(target_start.year, target_start.month)
            month_end = date(target_start.year, target_start.month, days_in_month)

            if target_start > today:
                continue

            effective_end = min(month_end, today)

            # 使用统一的portfolio_summary获取月度数据
            month_portfolio = self.get_portfolio_summary(
                account_ids, TimePeriod.CUSTOM, target_start, effective_end
            )
            
            # 计算月度交易统计
            month_transactions = Transaction.query.filter(
                Transaction.account_id.in_(account_ids),
                Transaction.trade_date >= target_start,
                Transaction.trade_date <= effective_end
            ).all()
            
            # 统计交易数据
            transaction_count = len(month_transactions)
            buy_amount = sell_amount = 0.0
            buy_cad = buy_usd = 0.0
            sell_cad = sell_usd = 0.0
            dividends = interest = 0.0
            dividends_cad = dividends_usd = 0.0
            interest_cad = interest_usd = 0.0

            def get_proportion(account_id: int) -> Decimal:
                return ownership_map.get(account_id, Decimal('1')) if ownership_map else Decimal('1')

            for tx in month_transactions:
                tx_type = (tx.type or '').upper()
                tx_currency = (tx.currency or '').upper()
                proportion = float(get_proportion(tx.account_id))

                quantity = float(tx.quantity or 0)
                price = float(tx.price or 0)
                fee = float(tx.fee or 0)
                amount = float(tx.amount or 0)

                if tx_type == 'BUY' and quantity and price:
                    gross = (quantity * price + fee) * proportion
                    buy_amount += gross
                    if tx_currency == 'CAD':
                        buy_cad += gross
                    elif tx_currency == 'USD':
                        buy_usd += gross
                elif tx_type == 'SELL' and quantity and price:
                    net = (quantity * price - fee) * proportion
                    sell_amount += net
                    if tx_currency == 'CAD':
                        sell_cad += net
                    elif tx_currency == 'USD':
                        sell_usd += net
                elif tx_type == 'DIVIDEND' and amount:
                    dividend_value = amount * proportion
                    dividends += dividend_value
                    if tx_currency == 'CAD':
                        dividends_cad += dividend_value
                    elif tx_currency == 'USD':
                        dividends_usd += dividend_value
                elif tx_type == 'INTEREST' and amount:
                    interest_value = amount * proportion
                    interest += interest_value
                    if tx_currency == 'CAD':
                        interest_cad += interest_value
                    elif tx_currency == 'USD':
                        interest_usd += interest_value
            
            # 计算月度已实现收益
            monthly_realized_gain = 0
            monthly_realized_gain_cad = 0
            monthly_realized_gain_usd = 0
            for holding in month_portfolio.get('cleared_holdings', []):
                realized_gain = holding.get('realized_gain', 0) * float(get_proportion(holding.get('account_id')))
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
            
            current_holdings = month_portfolio.get('current_holdings', [])
            total_assets = Decimal('0')
            for holding in current_holdings:
                proportion_dec = get_proportion(holding.get('account_id'))
                value_dec = Decimal(str(holding.get('current_value', 0))) * proportion_dec
                total_assets += value_dec
                if (holding.get('currency') or '').upper() == 'CAD':
                    total_assets_cad += float(value_dec)
                    unrealized_gain_cad += float(Decimal(str(holding.get('unrealized_gain', 0))) * proportion_dec)
                elif (holding.get('currency') or '').upper() == 'USD':
                    total_assets_usd += float(value_dec)
                    unrealized_gain_usd += float(Decimal(str(holding.get('unrealized_gain', 0))) * proportion_dec)

            monthly_unrealized_gain_dec, currency_gain_map = self._calculate_quarterly_unrealized_gain(
                current_holdings,
                target_start,
                effective_end,
                ownership_map
            )
            monthly_unrealized_gain = float(monthly_unrealized_gain_dec)
            unrealized_gain_cad = float(currency_gain_map.get('CAD', Decimal('0')))
            unrealized_gain_usd = float(currency_gain_map.get('USD', Decimal('0')))
            
            monthly_data.append({
                'year': target_start.year,
                'month': target_start.month,
                'month_name': target_start.strftime('%Y-%m'),
                'total_assets': float(total_assets),
                'monthly_realized_gain': monthly_realized_gain,
                'monthly_unrealized_gain': monthly_unrealized_gain,
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
    
    def get_performance_comparison(self, account_ids: List[int],
                                   period: str = '1m',
                                   member_id: Optional[int] = None,
                                   return_type: str = 'mwr') -> Dict:
        """获取收益对比数据，支持多种时间范围"""
        if not account_ids:
            return {
                'performance_series': [],
                'summary': {
                    'start_date': date.today().isoformat(),
                    'end_date': date.today().isoformat(),
                    'range': period,
                    'portfolio_return_percent': 0.0,
                    'portfolio_total_return': 0.0,
                    'portfolio_final_value': 0.0,
                    'portfolio_base_value': 0.0,
                    'sp500_return_percent': 0.0,
                    'nasdaq_return_percent': 0.0
                }
            }

        period = (period or '1m').lower()

        def resolve_start_date(label: str, today: date) -> date:
            if label == '1m':
                return today - timedelta(days=29)
            if label == '3m':
                return today - timedelta(days=89)
            if label == '6m':
                return today - timedelta(days=179)
            if label == 'ytd':
                return date(today.year, 1, 1)
            if label == '1y':
                return today - timedelta(days=364)
            if label == '2y':
                return today - timedelta(days=729)
            if label == '5y':
                return today - timedelta(days=1824)
            if label == 'all':
                min_trade = db.session.query(db.func.min(Transaction.trade_date))\
                    .filter(Transaction.account_id.in_(account_ids)).scalar()
                if min_trade:
                    return min_trade
                return today - timedelta(days=29)
            # default fallback
            return today - timedelta(days=29)

        today = date.today()
        start_date = resolve_start_date(period, today)
        if start_date > today:
            start_date = today

        ownership_map: Dict[int, Decimal] = {}
        if member_id:
            memberships = AccountMember.query.filter_by(member_id=member_id).all()
            for membership in memberships:
                try:
                    ownership_map[membership.account_id] = Decimal(str(membership.ownership_percentage or 0)) / Decimal('100')
                except (InvalidOperation, TypeError):
                    ownership_map[membership.account_id] = Decimal('0')

        def get_proportion(account_id: int) -> Decimal:
            return ownership_map.get(account_id, Decimal('1')) if ownership_map else Decimal('1')

        days_count = (today - start_date).days + 1
        if days_count <= 0:
            days_count = 1
        date_range = [start_date + timedelta(days=i) for i in range(days_count)]

        use_twr = (return_type or 'mwr').lower() == 'twr'

        asset_service = AssetValuationService()

        portfolio_values: List[Tuple[date, Decimal]] = []
        daily_flows: List[Tuple[date, Decimal]] = []  # date, net flow

        for current_date in date_range:
            total_value = Decimal('0')
            for account_id in account_ids:
                proportion = get_proportion(account_id)
                if proportion <= 0:
                    continue
                snapshot = asset_service.get_asset_snapshot(account_id, current_date)
                total_value += Decimal(str(snapshot.total_assets)) * proportion
            portfolio_values.append((current_date, total_value))

            day_flows = Decimal('0')
            for tx in Transaction.query.filter(
                Transaction.account_id.in_(account_ids),
                Transaction.trade_date == current_date
            ).all():
                tx_type = (tx.type or '').upper()
                if tx_type in ('DEPOSIT', 'WITHDRAWAL'):
                    proportion = get_proportion(tx.account_id)
                    if proportion <= 0:
                        continue
                    amount = Decimal(str(tx.amount or 0)) * proportion
                    if tx_type == 'WITHDRAWAL':
                        amount *= Decimal('-1')
                    day_flows += amount
            daily_flows.append((current_date, day_flows))

        portfolio_returns: List[float] = []
        actual_base_value = next((value for _, value in portfolio_values if value > 0), Decimal('0'))

        if actual_base_value <= 0:
            portfolio_returns = [0.0 for _ in portfolio_values]
            base_value = Decimal('1')
        else:
            base_value = actual_base_value
            if use_twr:
                previous_value = None
                for i, (current_date, value) in enumerate(portfolio_values):
                    flow = daily_flows[i][1]
                    if previous_value is None or previous_value <= 0:
                        portfolio_returns.append(0.0)
                    else:
                        adjusted_prev = previous_value - flow
                        if adjusted_prev <= 0:
                            portfolio_returns.append(0.0)
                        else:
                            period_return = (value - flow - adjusted_prev) / adjusted_prev
                            cumulative = (Decimal('1') + Decimal(str(portfolio_returns[-1] / 100))) if portfolio_returns else Decimal('1')
                            cumulative *= (Decimal('1') + period_return)
                            portfolio_returns.append(float((cumulative - Decimal('1')) * Decimal('100')))
                    previous_value = value
            else:
                for _, value in portfolio_values:
                    portfolio_returns.append(float(((value / base_value) - Decimal('1')) * Decimal('100')))

        def build_index_returns(symbol: str) -> List[float]:
            cache_key = (symbol.upper(), start_date, today)
            if cache_key in self._benchmark_cache:
                cached_series = self._benchmark_cache[cache_key]
                if len(cached_series) == len(date_range):
                    return cached_series

            history = self.history_cache_service.get_cached_history(
                symbol,
                start_date - timedelta(days=14),
                today,
                'USD'
            )
            price_map: Dict[date, Decimal] = {}
            for record in history:
                record_date_str = record.get('date')
                if not record_date_str:
                    continue
                try:
                    record_date = datetime.fromisoformat(record_date_str).date()
                except ValueError:
                    continue
                close_value = record.get('close')
                if close_value in (None, ''):
                    continue
                try:
                    price_map[record_date] = Decimal(str(close_value))
                except (InvalidOperation, TypeError):
                    continue

            returns: List[float] = []
            last_close: Optional[Decimal] = None
            base_close: Optional[Decimal] = None
            for current_date in date_range:
                if current_date in price_map:
                    last_close = price_map[current_date]
                    if base_close is None and last_close > 0:
                        base_close = last_close
                if base_close is None or base_close <= 0 or last_close is None:
                    returns.append(0.0)
                else:
                    returns.append(float(((last_close / base_close) - Decimal('1')) * Decimal('100')))
            self._benchmark_cache[cache_key] = returns
            if len(self._benchmark_cache) > 32:
                try:
                    oldest_key = next(iter(self._benchmark_cache))
                    if oldest_key != cache_key:
                        self._benchmark_cache.pop(oldest_key, None)
                except StopIteration:
                    pass
            return returns

        sp500_returns = build_index_returns('^GSPC')
        nasdaq_returns = build_index_returns('^IXIC')

        performance_series = []
        for idx, current_date in enumerate(date_range):
            performance_series.append({
                'date': current_date.isoformat(),
                'portfolio': portfolio_returns[idx] if idx < len(portfolio_returns) else 0.0,
                'sp500': sp500_returns[idx] if idx < len(sp500_returns) else 0.0,
                'nasdaq': nasdaq_returns[idx] if idx < len(nasdaq_returns) else 0.0
            })

        final_portfolio_return = portfolio_returns[-1] if portfolio_returns else 0.0
        final_sp500_return = sp500_returns[-1] if sp500_returns else 0.0
        final_nasdaq_return = nasdaq_returns[-1] if nasdaq_returns else 0.0
        final_value = float(portfolio_values[-1][1]) if portfolio_values else 0.0
        base_value_float = float(actual_base_value) if actual_base_value > 0 else 0.0
        total_return_value = float(portfolio_values[-1][1] - actual_base_value) if (portfolio_values and actual_base_value > 0) else 0.0

        return {
            'performance_series': performance_series,
            'summary': {
                'start_date': start_date.isoformat(),
                'end_date': today.isoformat(),
                'range': period,
                'return_type': 'twr' if use_twr else 'mwr',
                'portfolio_return_percent': final_portfolio_return,
                'portfolio_total_return': total_return_value,
                'portfolio_final_value': final_value,
                'portfolio_base_value': base_value_float,
                'sp500_return_percent': final_sp500_return,
                'nasdaq_return_percent': final_nasdaq_return
            }
        }

    def get_recent_30_days_analysis(self, account_ids: List[int],
                                    member_id: Optional[int] = None) -> Dict:
        """保持向后兼容，默认返回最近1个月对比数据"""
        return self.get_performance_comparison(account_ids, '1m', member_id=member_id)
    
    
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
            account_info = {}

            for holding in current_holdings:
                account_id = holding['account_id']
                current_value = float(holding['current_value'])
                
                if current_value > 0:
                    by_account[account_id]['value'] += current_value
                    by_account[account_id]['holdings_count'] += 1
                    
                    # 获取账户信息
                    if account_id not in account_info:
                        from app.models import Account
                        account = Account.query.get(account_id)
                        members = []
                        if account and account.account_members:
                            for account_member in account.account_members:
                                member_name = account_member.member.name if account_member.member else 'Unknown'
                                ownership = float(account_member.ownership_percentage or 0)
                                members.append({
                                    'member_id': account_member.member_id,
                                    'name': member_name,
                                    'ownership_percentage': ownership
                                })
                        account_info[account_id] = {
                            'name': account.name if account else f'Account {account_id}',
                            'is_joint': account.is_joint if account else False,
                            'members': members
                        }

            by_account_list = [
                {
                    'account_id': account_id,
                    'account_name': account_info.get(account_id, {}).get('name', f'Account {account_id}'),
                    'value': data['value'],
                    'holdings_count': data['holdings_count'],
                    'is_joint': account_info.get(account_id, {}).get('is_joint', False),
                    'members': [
                        {
                            'member_id': member.get('member_id'),
                            'name': member.get('name'),
                            'ownership_percentage': member.get('ownership_percentage', 0),
                            'value': data['value'] * (member.get('ownership_percentage', 0) / 100.0)
                        }
                        for member in account_info.get(account_id, {}).get('members', [])
                    ]
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
