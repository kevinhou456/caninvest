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
from app.models.account import Account, AccountMember, AccountType
from app.models.member import Member
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

    previous_close: Decimal = Decimal('0')
    previous_value: Decimal = Decimal('0')
    daily_change_value: Decimal = Decimal('0')
    daily_change_percent: Decimal = Decimal('0')
    
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
            'previous_close': float(self.previous_close),
            'previous_value': float(self.previous_value),
            'daily_change_value': float(self.daily_change_value),
            'daily_change_percent': float(self.daily_change_percent),
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
        
        # 计算已实现收益 - 包括当前持仓和清仓持仓
        total_realized_gain = sum(Decimal(str(h['realized_gain'])) for h in current_holdings)
        total_realized_gain += sum(Decimal(str(h['realized_gain'])) for h in cleared_holdings)
        
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
                'total_realized_gain': float(total_realized_gain),
                'total_unrealized_gain': float(total_unrealized_gain),
                'total_return_percent': float(((total_realized_gain + total_unrealized_gain) / total_cost * 100)) if total_cost > 0 else 0
            }
        }
        

    def _update_market_data(self, snapshot: PositionSnapshot):
        """更新市场数据"""
        # 如果as_of_date是今天，使用当前价格；否则使用历史价格
        today = date.today()
        if snapshot.as_of_date >= today:
            current_price = self._get_current_price(snapshot.symbol, snapshot.currency)
        else:
            # 使用历史价格 - 基于as_of_date的最后交易日价格
            current_price = self._get_last_trading_price(snapshot.symbol, snapshot.currency, snapshot.as_of_date)

        if current_price:
            snapshot.current_price = current_price
            snapshot.current_value = snapshot.current_shares * current_price
            snapshot.unrealized_gain = snapshot.current_value - snapshot.total_cost
            
            if snapshot.total_cost > 0:
                snapshot.unrealized_gain_percent = (snapshot.unrealized_gain / snapshot.total_cost) * 100

            prev_date = snapshot.as_of_date - timedelta(days=1)
            previous_price = self._get_last_trading_price(snapshot.symbol, snapshot.currency, prev_date)
            if previous_price is None:
                previous_price = self._get_last_trading_price(snapshot.symbol, snapshot.currency, snapshot.as_of_date)

            if previous_price is not None:
                snapshot.previous_close = previous_price
                snapshot.previous_value = snapshot.current_shares * previous_price
                snapshot.daily_change_value = snapshot.current_shares * (snapshot.current_price - previous_price)
                if previous_price > 0:
                    snapshot.daily_change_percent = ((snapshot.current_price - previous_price) / previous_price) * 100
                else:
                    snapshot.daily_change_percent = Decimal('0')
            else:
                snapshot.previous_close = Decimal('0')
                snapshot.previous_value = Decimal('0')
                snapshot.daily_change_value = Decimal('0')
                snapshot.daily_change_percent = Decimal('0')
        
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
                           member_id: Optional[int] = None,
                           selected_account_id: Optional[int] = None) -> Dict:
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

        def calculate_realized_totals(portfolio_summary: Optional[Dict]) -> Dict[str, Decimal]:
            totals = {
                'total': Decimal('0'),
                'cad': Decimal('0'),
                'usd': Decimal('0')
            }
            if not portfolio_summary:
                return totals

            for collection in ('current_holdings', 'cleared_holdings'):
                for holding in (portfolio_summary.get(collection, []) or []):
                    try:
                        proportion_dec = get_proportion(holding.get('account_id'))
                    except Exception:
                        proportion_dec = Decimal('1')
                    if proportion_dec <= 0:
                        continue
                    realized_value = Decimal(str(holding.get('realized_gain', 0) or 0)) * proportion_dec
                    totals['total'] += realized_value
                    currency = (holding.get('currency') or 'USD').upper()
                    if currency == 'CAD':
                        totals['cad'] += realized_value
                    elif currency == 'USD':
                        totals['usd'] += realized_value
            return totals

        def compute_totals_for_date(as_of_date: date) -> Dict[str, float]:
            portfolio = self.get_portfolio_summary(account_ids, TimePeriod.CUSTOM, end_date=as_of_date)
            realized_totals = calculate_realized_totals(portfolio)
            current_holdings = portfolio.get('current_holdings', []) or []

            total_assets_stock_dec = Decimal('0')
            total_assets_cad = 0.0
            total_assets_usd = 0.0
            unrealized_gain_cad = 0.0
            unrealized_gain_usd = 0.0
            total_unrealized_dec = Decimal('0')

            for holding in current_holdings:
                proportion_dec = get_proportion(holding.get('account_id'))
                if proportion_dec <= 0:
                    continue
                value_dec = Decimal(str(holding.get('current_value', 0) or 0)) * proportion_dec
                unrealized_dec = Decimal(str(holding.get('unrealized_gain', 0) or 0)) * proportion_dec
                total_assets_stock_dec += value_dec
                total_unrealized_dec += unrealized_dec
                currency = (holding.get('currency') or 'USD').upper()
                if currency == 'CAD':
                    total_assets_cad += float(value_dec)
                    unrealized_gain_cad += float(unrealized_dec)
                elif currency == 'USD':
                    total_assets_usd += float(value_dec)
                    unrealized_gain_usd += float(unrealized_dec)

            cash_total_cad_dec = Decimal('0')
            cash_total_usd_dec = Decimal('0')
            for account_id in account_ids:
                proportion_dec = get_proportion(account_id)
                if proportion_dec <= 0:
                    continue
                try:
                    snapshot = asset_service.get_asset_snapshot(account_id, as_of_date)
                except Exception:
                    continue
                cash_total_cad_dec += Decimal(str(snapshot.cash_balance_cad or 0)) * proportion_dec
                cash_total_usd_dec += Decimal(str(snapshot.cash_balance_usd or 0)) * proportion_dec

            total_assets_dec = total_assets_stock_dec + cash_total_cad_dec + (cash_total_usd_dec * usd_to_cad_decimal)
            if cash_total_cad_dec < 0:
                cash_total_cad_dec = Decimal('0')
            if cash_total_usd_dec < 0:
                cash_total_usd_dec = Decimal('0')

            total_assets_float = float(total_assets_dec)
            total_assets_cad += float(cash_total_cad_dec)
            total_assets_usd += float(cash_total_usd_dec)

            # Apply current exchange rate to realized and unrealized gains
            total_realized_with_rate = float(realized_totals['cad']) + float(realized_totals['usd']) * float(usd_to_cad_decimal)
            total_unrealized_with_rate = unrealized_gain_cad + unrealized_gain_usd * float(usd_to_cad_decimal)

            total_unrealized_float = float(total_unrealized_dec)
            total_realized_float = float(realized_totals['total'])
            overall_return_percent = 0.0
            if total_assets_float > 0:
                overall_return_percent = ((total_realized_with_rate + total_unrealized_with_rate) / total_assets_float) * 100

            return {
                'total_assets': total_assets_float,
                'total_assets_cad': total_assets_cad,
                'total_assets_usd': total_assets_usd,
                'realized_gain': total_realized_with_rate,
                'realized_gain_cad': float(realized_totals['cad']),
                'realized_gain_usd': float(realized_totals['usd']),
                'unrealized_gain': total_unrealized_with_rate,
                'unrealized_gain_cad': unrealized_gain_cad,
                'unrealized_gain_usd': unrealized_gain_usd,
                'cash_cad': float(cash_total_cad_dec),
                'cash_usd': float(cash_total_usd_dec),
                'return_percent': overall_return_percent
            }

        asset_service = AssetValuationService()
        usd_to_cad_rate = currency_service.get_current_rate('USD', 'CAD') or 1
        try:
            usd_to_cad_decimal = Decimal(str(usd_to_cad_rate))
        except (InvalidOperation, TypeError):
            usd_to_cad_decimal = Decimal('1')

        def get_proportion(account_id: int) -> Decimal:
            return ownership_map.get(account_id, Decimal('1')) if ownership_map else Decimal('1')

        annual_data = []
        if not years:
            years = []  # Set empty list if no years found

        # 批量获取年度平均汇率
        annual_exchange_rates = currency_service.get_annual_rates_for_years(years, 'USD', 'CAD') if years else {}

        # 获取成员相关的账户信息，用于账户类型分组
        member_accounts_info = {}
        if member_id:
            member_accounts = db.session.query(Account, AccountType, AccountMember).join(
                AccountMember, Account.id == AccountMember.account_id
            ).join(
                AccountType, Account.account_type_id == AccountType.id
            ).filter(
                AccountMember.member_id == member_id,
                Account.id.in_(account_ids)
            ).all()

            for account, account_type, membership in member_accounts:
                member_accounts_info[account.id] = {
                    'account_type_id': account_type.id,
                    'account_type_name': account_type.name,
                    'ownership_percentage': membership.ownership_percentage or Decimal('100')
                }

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
            deposit_amount = withdrawal_amount = Decimal('0')
            deposit_cad = deposit_usd = Decimal('0')
            withdrawal_cad = withdrawal_usd = Decimal('0')
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
                elif tx_type == 'DEPOSIT' and amount:
                    value = amount * proportion
                    deposit_amount += value
                    if tx_currency == 'CAD':
                        deposit_cad += value
                    elif tx_currency == 'USD':
                        deposit_usd += value
                elif tx_type == 'WITHDRAWAL' and amount:
                    value = amount * proportion
                    withdrawal_amount += value
                    if tx_currency == 'CAD':
                        withdrawal_cad += value
                    elif tx_currency == 'USD':
                        withdrawal_usd += value
            
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

            # 使用统一的 get_portfolio_summary 计算年度增量未实现收益
            year_end_portfolio = self.get_portfolio_summary(
                account_ids, TimePeriod.CUSTOM, year_end, year_end
            )

            # 使用前一年的年末作为基准，而不是当年年初
            previous_year_end = date(year - 1, 12, 31)
            previous_year_portfolio = self.get_portfolio_summary(
                account_ids, TimePeriod.CUSTOM, previous_year_end, previous_year_end
            )

            # 计算年度增量未实现收益（今年年末 - 去年年末）
            year_end_unrealized = year_end_portfolio.get('summary', {}).get('total_unrealized_gain', 0)
            previous_year_unrealized = previous_year_portfolio.get('summary', {}).get('total_unrealized_gain', 0)
            annual_unrealized_gain = year_end_unrealized - previous_year_unrealized

            # 计算按货币分类的未实现收益
            year_end_unrealized_cad_dec = Decimal('0')
            year_end_unrealized_usd_dec = Decimal('0')
            previous_year_unrealized_cad_dec = Decimal('0')
            previous_year_unrealized_usd_dec = Decimal('0')

            for holding in year_end_portfolio.get('current_holdings', []):
                proportion = get_proportion(holding.get('account_id'))
                if proportion <= 0:
                    continue
                unrealized = Decimal(str(holding.get('unrealized_gain', 0) or 0)) * proportion
                currency = (holding.get('currency') or '').upper()
                if currency == 'CAD':
                    year_end_unrealized_cad_dec += unrealized
                elif currency == 'USD':
                    year_end_unrealized_usd_dec += unrealized

            for holding in previous_year_portfolio.get('current_holdings', []):
                proportion = get_proportion(holding.get('account_id'))
                if proportion <= 0:
                    continue
                unrealized = Decimal(str(holding.get('unrealized_gain', 0) or 0)) * proportion
                currency = (holding.get('currency') or '').upper()
                if currency == 'CAD':
                    previous_year_unrealized_cad_dec += unrealized
                elif currency == 'USD':
                    previous_year_unrealized_usd_dec += unrealized

            unrealized_gain_cad = float(year_end_unrealized_cad_dec - previous_year_unrealized_cad_dec)
            unrealized_gain_usd = float(year_end_unrealized_usd_dec - previous_year_unrealized_usd_dec)

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

            # 获取年度平均汇率
            annual_usd_cad_rate = annual_exchange_rates.get(year)
            annual_usd_cad_rate_float = float(annual_usd_cad_rate) if annual_usd_cad_rate else None

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
                'deposit_amount': float(deposit_amount),
                'withdrawal_amount': float(withdrawal_amount),
                'annual_usd_cad_rate': annual_usd_cad_rate_float,
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
                    'deposit_cad': float(deposit_cad),
                    'deposit_usd': float(deposit_usd),
                    'withdrawal_cad': float(withdrawal_cad),
                    'withdrawal_usd': float(withdrawal_usd),
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

            # 如果选择了特定成员，添加按账户类型分组的详细数据
            # 但如果选择的是单一账户且该账户完全由该成员拥有，则不需要显示分组
            should_show_account_type_breakdown = False
            if member_id and member_accounts_info:
                print(f"Debug: member_id={member_id}, selected_account_id={selected_account_id}, account_ids={account_ids}")
                print(f"Debug: member_accounts_info={member_accounts_info}")
                if selected_account_id and len(account_ids) == 1:
                    # 检查是否是单一成员完全拥有的账户
                    if selected_account_id in member_accounts_info:
                        ownership_pct = member_accounts_info[selected_account_id]['ownership_percentage']
                        print(f"Debug: ownership_pct={ownership_pct}")
                        # 如果该成员拥有该账户100%，则不需要显示分组
                        should_show_account_type_breakdown = ownership_pct < Decimal('100')
                        print(f"Debug: should_show_account_type_breakdown={should_show_account_type_breakdown}")
                    else:
                        should_show_account_type_breakdown = False
                        print(f"Debug: selected_account_id not in member_accounts_info")
                else:
                    # 多个账户或未指定具体账户，显示分组
                    should_show_account_type_breakdown = True
                    print(f"Debug: multiple accounts or no specific account selected")

            if should_show_account_type_breakdown:
                account_type_groups = {}

                # 按账户类型分组计算 - 使用原始portfolio数据重新获取，避免二次缩放
                original_portfolio = self.get_portfolio_summary(account_ids, TimePeriod.CUSTOM, end_date=year_end)

                # 按账户类型分组计算
                for collection in ('current_holdings', 'cleared_holdings'):
                    for holding in original_portfolio.get(collection, []):
                        account_id = holding.get('account_id')
                        if account_id not in member_accounts_info:
                            continue

                        account_info = member_accounts_info[account_id]
                        account_type_name = account_info['account_type_name']
                        ownership_pct = ownership_map.get(account_id, Decimal('1')) if ownership_map else Decimal('1')

                        if account_type_name not in account_type_groups:
                            account_type_groups[account_type_name] = {
                                'total_assets': Decimal('0'),
                                'annual_realized_gain': Decimal('0'),
                                'annual_unrealized_gain': Decimal('0'),
                                'annual_dividends': Decimal('0'),
                                'annual_interest': Decimal('0'),
                                'buy_amount': Decimal('0'),
                                'sell_amount': Decimal('0'),
                                'deposit_amount': Decimal('0'),
                                'withdrawal_amount': Decimal('0'),
                                'transaction_count': 0,
                                'currency_breakdown': {
                                    'total_assets_cad': 0.0,
                                    'total_assets_usd': 0.0,
                                    'realized_gain_cad': 0.0,
                                    'realized_gain_usd': 0.0,
                                    'unrealized_gain_cad': 0.0,
                                    'unrealized_gain_usd': 0.0,
                                    'buy_cad': 0.0,
                                    'buy_usd': 0.0,
                                    'sell_cad': 0.0,
                                    'sell_usd': 0.0,
                                    'deposit_cad': 0.0,
                                    'deposit_usd': 0.0,
                                    'withdrawal_cad': 0.0,
                                    'withdrawal_usd': 0.0,
                                    'dividends_cad': 0.0,
                                    'dividends_usd': 0.0,
                                    'interest_cad': 0.0,
                                    'interest_usd': 0.0,
                                }
                            }

                        group = account_type_groups[account_type_name]

                        # 计算按比例的数值
                        current_value = Decimal(str(holding.get('current_value', 0) or 0)) * ownership_pct
                        realized_gain = Decimal(str(holding.get('realized_gain', 0) or 0)) * ownership_pct
                        unrealized_gain = Decimal(str(holding.get('unrealized_gain', 0) or 0)) * ownership_pct

                        # 只有current_holdings计算总资产和浮动盈亏
                        if collection == 'current_holdings':
                            group['total_assets'] += current_value
                            group['annual_unrealized_gain'] += unrealized_gain

                        # 实现收益对两种collection都计算
                        group['annual_realized_gain'] += realized_gain

                        # 货币分解
                        currency = (holding.get('currency') or 'USD').upper()
                        if currency == 'CAD':
                            if collection == 'current_holdings':
                                group['currency_breakdown']['total_assets_cad'] += float(current_value)
                                group['currency_breakdown']['unrealized_gain_cad'] += float(unrealized_gain)
                            group['currency_breakdown']['realized_gain_cad'] += float(realized_gain)
                        elif currency == 'USD':
                            if collection == 'current_holdings':
                                group['currency_breakdown']['total_assets_usd'] += float(current_value)
                                group['currency_breakdown']['unrealized_gain_usd'] += float(unrealized_gain)
                            group['currency_breakdown']['realized_gain_usd'] += float(realized_gain)

                # 处理交易数据按账户类型分组 - 交易按比例分配给成员
                for transaction in year_transactions:
                    account_id = transaction.account_id
                    if account_id not in member_accounts_info:
                        continue

                    account_type_name = member_accounts_info[account_id]['account_type_name']

                    if account_type_name not in account_type_groups:
                        continue

                    group = account_type_groups[account_type_name]

                    amount = transaction.amount or Decimal('0')
                    tx_currency = (transaction.currency or 'USD').upper()

                    # 交易按该成员在该账户的持股比例分配
                    ownership_pct = member_accounts_info[account_id]['ownership_percentage'] / Decimal('100')
                    value = amount * ownership_pct

                    group['transaction_count'] += 1

                    if transaction.type == 'BUY':
                        group['buy_amount'] += value
                        if tx_currency == 'CAD':
                            group['currency_breakdown']['buy_cad'] += float(value)
                        elif tx_currency == 'USD':
                            group['currency_breakdown']['buy_usd'] += float(value)
                    elif transaction.type == 'SELL':
                        group['sell_amount'] += value
                        if tx_currency == 'CAD':
                            group['currency_breakdown']['sell_cad'] += float(value)
                        elif tx_currency == 'USD':
                            group['currency_breakdown']['sell_usd'] += float(value)
                    elif transaction.type == 'DIVIDEND':
                        group['annual_dividends'] += value
                        if tx_currency == 'CAD':
                            group['currency_breakdown']['dividends_cad'] += float(value)
                        elif tx_currency == 'USD':
                            group['currency_breakdown']['dividends_usd'] += float(value)
                    elif transaction.type == 'INTEREST':
                        group['annual_interest'] += value
                        if tx_currency == 'CAD':
                            group['currency_breakdown']['interest_cad'] += float(value)
                        elif tx_currency == 'USD':
                            group['currency_breakdown']['interest_usd'] += float(value)
                    elif transaction.type == 'DEPOSIT':
                        group['deposit_amount'] += value
                        if tx_currency == 'CAD':
                            group['currency_breakdown']['deposit_cad'] += float(value)
                        elif tx_currency == 'USD':
                            group['currency_breakdown']['deposit_usd'] += float(value)
                    elif transaction.type == 'WITHDRAWAL':
                        group['withdrawal_amount'] += value
                        if tx_currency == 'CAD':
                            group['currency_breakdown']['withdrawal_cad'] += float(value)
                        elif tx_currency == 'USD':
                            group['currency_breakdown']['withdrawal_usd'] += float(value)

                # 为每个账户类型创建详细行
                for account_type_name, group_data in account_type_groups.items():
                    # 添加现金余额
                    type_cash_cad = Decimal('0')
                    type_cash_usd = Decimal('0')

                    for account_id in account_ids:
                        if account_id in member_accounts_info and member_accounts_info[account_id]['account_type_name'] == account_type_name:
                            proportion = get_proportion(account_id)
                            snapshot = asset_service.get_asset_snapshot(account_id, year_end)
                            type_cash_cad += snapshot.cash_balance_cad * proportion
                            type_cash_usd += snapshot.cash_balance_usd * proportion

                    total_assets_with_cash = group_data['total_assets'] + type_cash_cad + type_cash_usd * exchange_rate_decimal

                    annual_data.append({
                        'year': year,
                        'account_type': account_type_name,  # 标识这是账户类型详细行
                        'is_detail_row': True,  # 标识这是详细行
                        'total_assets': float(total_assets_with_cash),
                        'annual_realized_gain': float(group_data['annual_realized_gain']),
                        'annual_unrealized_gain': float(group_data['annual_unrealized_gain']),
                        'annual_dividends': float(group_data['annual_dividends']),
                        'annual_interest': float(group_data['annual_interest']),
                        'annual_income': float(group_data['annual_dividends'] + group_data['annual_interest']),
                        'transaction_count': group_data['transaction_count'],
                        'buy_amount': float(group_data['buy_amount']),
                        'sell_amount': float(group_data['sell_amount']),
                        'deposit_amount': float(group_data['deposit_amount']),
                        'withdrawal_amount': float(group_data['withdrawal_amount']),
                        'annual_usd_cad_rate': annual_usd_cad_rate_float,
                        'currency_breakdown': {
                            'total_assets_cad': group_data['currency_breakdown']['total_assets_cad'] + float(type_cash_cad),
                            'total_assets_usd': group_data['currency_breakdown']['total_assets_usd'] + float(type_cash_usd),
                            **group_data['currency_breakdown'],
                            'cash_cad': float(type_cash_cad),
                            'cash_usd': float(type_cash_usd)
                        },
                        'cash_balance': {
                            'cad': float(type_cash_cad),
                            'usd': float(type_cash_usd)
                        }
                    })

            # 决定是否显示成员分组的详细数据
            # 如果选择了特定成员，不显示成员行（因为所有数据已经是该成员的）
            # 如果没有选择特定成员，根据账户情况决定是否显示所有成员
            should_show_member_breakdown = False
            if member_id is not None:
                # 选择了特定成员，不显示成员行，因为所有数据已经是该成员的数据
                should_show_member_breakdown = False
                print(f"Debug: Specific member {member_id} selected, skipping member breakdown since data is already member-specific")
            elif member_id is None:
                if selected_account_id and len(account_ids) == 1:
                    # 检查是否是单一成员完全拥有的账户
                    account_members = AccountMember.query.filter_by(account_id=selected_account_id).all()
                    if len(account_members) == 1 and account_members[0].ownership_percentage >= Decimal('100'):
                        should_show_member_breakdown = False
                        print(f"Debug: Single member owns 100% of account {selected_account_id}, skipping member breakdown")
                    else:
                        should_show_member_breakdown = True
                        print(f"Debug: Multiple members or partial ownership, showing member breakdown")
                else:
                    # 多个账户或未指定具体账户，显示成员分组
                    should_show_member_breakdown = True
                    print(f"Debug: Multiple accounts or no specific account, showing member breakdown")

            if should_show_member_breakdown:
                # 获取家庭所有成员
                if account_ids:
                    # 从第一个账户获取family_id
                    first_account = Account.query.get(account_ids[0])
                    family_id = first_account.family_id if first_account else 1
                else:
                    family_id = 1
                # 如果指定了member_id，只处理该成员；否则处理所有成员
                if member_id is not None:
                    family_members = [Member.query.get(member_id)] if Member.query.get(member_id) else []
                else:
                    family_members = Member.query.filter_by(family_id=family_id).all()

                # 获取当前年份的联合账户数据（最新添加的数据项）
                current_year_data = annual_data[-1] if annual_data else None
                if not current_year_data:
                    continue  # 如果没有联合账户数据，跳过成员计算

                for member in family_members:
                    # 获取该成员在所有选定账户中的持股信息
                    member_account_memberships = AccountMember.query.filter_by(member_id=member.id).all()
                    member_account_map = {}

                    for membership in member_account_memberships:
                        if membership.account_id in account_ids:
                            ownership_pct = Decimal(str(membership.ownership_percentage or 0)) / Decimal('100')
                            member_account_map[membership.account_id] = ownership_pct

                    if not member_account_map:
                        continue  # 该成员在选定账户中没有份额

                    # 计算该成员的实际数据，按账户分别计算然后汇总
                    member_total_assets = Decimal('0')
                    member_realized_gain = Decimal('0')
                    member_unrealized_gain = Decimal('0')
                    member_dividends = Decimal('0')
                    member_interest = Decimal('0')
                    member_buy_amount = Decimal('0')
                    member_sell_amount = Decimal('0')
                    member_deposit_amount = Decimal('0')
                    member_withdrawal_amount = Decimal('0')
                    member_transaction_count = 0

                    # 货币分解统计
                    member_currency_breakdown = {
                        'total_assets_cad': 0.0,
                        'total_assets_usd': 0.0,
                        'realized_gain_cad': 0.0,
                        'realized_gain_usd': 0.0,
                        'unrealized_gain_cad': 0.0,
                        'unrealized_gain_usd': 0.0,
                        'buy_cad': 0.0,
                        'buy_usd': 0.0,
                        'sell_cad': 0.0,
                        'sell_usd': 0.0,
                        'deposit_cad': 0.0,
                        'deposit_usd': 0.0,
                        'withdrawal_cad': 0.0,
                        'withdrawal_usd': 0.0,
                        'dividends_cad': 0.0,
                        'dividends_usd': 0.0,
                        'interest_cad': 0.0,
                        'interest_usd': 0.0,
                        'cash_cad': 0.0,
                        'cash_usd': 0.0
                    }

                    member_cash_cad = Decimal('0')
                    member_cash_usd = Decimal('0')

                    # 按账户分别计算该成员的数据
                    for account_id, ownership_pct in member_account_map.items():
                        if ownership_pct <= 0:
                            continue

                        # 获取该账户在该年度的portfolio数据
                        account_portfolio = self.get_portfolio_summary([account_id], TimePeriod.CUSTOM, end_date=year_end)

                        # 累计资产数据
                        for collection in ('current_holdings', 'cleared_holdings'):
                            for holding in account_portfolio.get(collection, []):
                                if holding.get('account_id') != account_id:
                                    continue

                                # 按该成员在该账户的持股比例计算
                                current_value = Decimal(str(holding.get('current_value', 0) or 0)) * ownership_pct
                                realized_gain = Decimal(str(holding.get('realized_gain', 0) or 0)) * ownership_pct
                                unrealized_gain = Decimal(str(holding.get('unrealized_gain', 0) or 0)) * ownership_pct

                                # 只有current_holdings计算总资产和浮动盈亏
                                if collection == 'current_holdings':
                                    member_total_assets += current_value
                                    member_unrealized_gain += unrealized_gain

                                # 实现收益对两种collection都计算
                                member_realized_gain += realized_gain

                                # 货币分解
                                currency = (holding.get('currency') or 'USD').upper()
                                if currency == 'CAD':
                                    if collection == 'current_holdings':
                                        member_currency_breakdown['total_assets_cad'] += float(current_value)
                                        member_currency_breakdown['unrealized_gain_cad'] += float(unrealized_gain)
                                    member_currency_breakdown['realized_gain_cad'] += float(realized_gain)
                                elif currency == 'USD':
                                    if collection == 'current_holdings':
                                        member_currency_breakdown['total_assets_usd'] += float(current_value)
                                        member_currency_breakdown['unrealized_gain_usd'] += float(unrealized_gain)
                                    member_currency_breakdown['realized_gain_usd'] += float(realized_gain)

                        # 处理该账户的交易数据
                        account_transactions = [t for t in year_transactions if t.account_id == account_id]
                        for transaction in account_transactions:
                            amount = Decimal(str(transaction.amount or 0))
                            tx_currency = (transaction.currency or 'USD').upper()
                            value = amount * ownership_pct

                            member_transaction_count += 1

                            if transaction.type == 'BUY':
                                member_buy_amount += value
                                if tx_currency == 'CAD':
                                    member_currency_breakdown['buy_cad'] += float(value)
                                elif tx_currency == 'USD':
                                    member_currency_breakdown['buy_usd'] += float(value)
                            elif transaction.type == 'SELL':
                                member_sell_amount += value
                                if tx_currency == 'CAD':
                                    member_currency_breakdown['sell_cad'] += float(value)
                                elif tx_currency == 'USD':
                                    member_currency_breakdown['sell_usd'] += float(value)
                            elif transaction.type == 'DEPOSIT':
                                member_deposit_amount += value
                                if tx_currency == 'CAD':
                                    member_currency_breakdown['deposit_cad'] += float(value)
                                elif tx_currency == 'USD':
                                    member_currency_breakdown['deposit_usd'] += float(value)
                            elif transaction.type == 'WITHDRAWAL':
                                member_withdrawal_amount += value
                                if tx_currency == 'CAD':
                                    member_currency_breakdown['withdrawal_cad'] += float(value)
                                elif tx_currency == 'USD':
                                    member_currency_breakdown['withdrawal_usd'] += float(value)
                            elif transaction.type == 'DIVIDEND':
                                member_dividends += value
                                if tx_currency == 'CAD':
                                    member_currency_breakdown['dividends_cad'] += float(value)
                                elif tx_currency == 'USD':
                                    member_currency_breakdown['dividends_usd'] += float(value)
                            elif transaction.type == 'INTEREST':
                                member_interest += value
                                if tx_currency == 'CAD':
                                    member_currency_breakdown['interest_cad'] += float(value)
                                elif tx_currency == 'USD':
                                    member_currency_breakdown['interest_usd'] += float(value)

                        # 计算现金余额
                        snapshot = asset_service.get_asset_snapshot(account_id, year_end)
                        member_cash_cad += snapshot.cash_balance_cad * ownership_pct
                        member_cash_usd += snapshot.cash_balance_usd * ownership_pct

                    # 添加现金到货币分解
                    member_currency_breakdown['cash_cad'] = float(member_cash_cad)
                    member_currency_breakdown['cash_usd'] = float(member_cash_usd)

                    # 计算总资产（包含现金）
                    total_assets_with_cash = member_total_assets + member_cash_cad + member_cash_usd * exchange_rate_decimal

                    member_cash_balance = {
                        'cad': float(member_cash_cad),
                        'usd': float(member_cash_usd)
                    }

                    # 为该成员创建详细行
                    annual_data.append({
                        'year': year,
                        'member_name': member.name,  # 标识这是成员详细行
                        'member_id': member.id,
                        'is_member_row': True,  # 标识这是成员行
                        'total_assets': float(total_assets_with_cash),
                        'annual_realized_gain': float(member_realized_gain),
                        'annual_unrealized_gain': float(member_unrealized_gain),
                        'annual_dividends': float(member_dividends),
                        'annual_interest': float(member_interest),
                        'annual_income': float(member_dividends + member_interest),
                        'transaction_count': member_transaction_count,
                        'buy_amount': float(member_buy_amount),
                        'sell_amount': float(member_sell_amount),
                        'deposit_amount': float(member_deposit_amount),
                        'withdrawal_amount': float(member_withdrawal_amount),
                        'annual_usd_cad_rate': annual_usd_cad_rate_float,
                        'currency_breakdown': member_currency_breakdown,
                        'cash_balance': member_cash_balance
                    })

                    # 如果是全部成员模式（没有选择特定成员），为每个成员按账户类型进行细分
                    if member_id is None:
                        # 获取该成员的账户信息，包括账户类型
                        member_specific_accounts = db.session.query(Account, AccountType, AccountMember).join(
                            AccountMember, Account.id == AccountMember.account_id
                        ).join(
                            AccountType, Account.account_type_id == AccountType.id
                        ).filter(
                            AccountMember.member_id == member.id,
                            Account.id.in_(account_ids)
                        ).all()

                        # 按账户类型分组该成员的数据
                        member_account_type_groups = {}

                        for account, account_type, membership in member_specific_accounts:
                            account_type_name = account_type.name
                            ownership_pct = Decimal(str(membership.ownership_percentage or 0)) / Decimal('100')

                            if account_type_name not in member_account_type_groups:
                                member_account_type_groups[account_type_name] = {
                                    'total_assets': Decimal('0'),
                                    'annual_realized_gain': Decimal('0'),
                                    'annual_unrealized_gain': Decimal('0'),
                                    'annual_dividends': Decimal('0'),
                                    'annual_interest': Decimal('0'),
                                    'buy_amount': Decimal('0'),
                                    'sell_amount': Decimal('0'),
                                    'deposit_amount': Decimal('0'),
                                    'withdrawal_amount': Decimal('0'),
                                    'transaction_count': 0,
                                    'currency_breakdown': {
                                        'total_assets_cad': 0.0,
                                        'total_assets_usd': 0.0,
                                        'realized_gain_cad': 0.0,
                                        'realized_gain_usd': 0.0,
                                        'unrealized_gain_cad': 0.0,
                                        'unrealized_gain_usd': 0.0,
                                        'buy_cad': 0.0,
                                        'buy_usd': 0.0,
                                        'sell_cad': 0.0,
                                        'sell_usd': 0.0,
                                        'deposit_cad': 0.0,
                                        'deposit_usd': 0.0,
                                        'withdrawal_cad': 0.0,
                                        'withdrawal_usd': 0.0,
                                        'dividends_cad': 0.0,
                                        'dividends_usd': 0.0,
                                        'interest_cad': 0.0,
                                        'interest_usd': 0.0,
                                    }
                                }

                            # 获取该账户的持仓数据
                            original_portfolio = self.get_portfolio_summary([account.id], TimePeriod.CUSTOM, end_date=year_end)

                            # 累计计算该账户类型的数据
                            group = member_account_type_groups[account_type_name]

                            for collection in ('current_holdings', 'cleared_holdings'):
                                for holding in original_portfolio.get(collection, []):
                                    if holding.get('account_id') != account.id:
                                        continue

                                    # 按该成员在该账户的持股比例计算
                                    current_value = Decimal(str(holding.get('current_value', 0) or 0)) * ownership_pct
                                    realized_gain = Decimal(str(holding.get('realized_gain', 0) or 0)) * ownership_pct
                                    unrealized_gain = Decimal(str(holding.get('unrealized_gain', 0) or 0)) * ownership_pct

                                    # 只有current_holdings计算总资产和浮动盈亏
                                    if collection == 'current_holdings':
                                        group['total_assets'] += current_value
                                        group['annual_unrealized_gain'] += unrealized_gain

                                    # 实现收益对两种collection都计算
                                    group['annual_realized_gain'] += realized_gain

                                    # 货币分解
                                    currency = (holding.get('currency') or 'USD').upper()
                                    if currency == 'CAD':
                                        if collection == 'current_holdings':
                                            group['currency_breakdown']['total_assets_cad'] += float(current_value)
                                            group['currency_breakdown']['unrealized_gain_cad'] += float(unrealized_gain)
                                        group['currency_breakdown']['realized_gain_cad'] += float(realized_gain)
                                    elif currency == 'USD':
                                        if collection == 'current_holdings':
                                            group['currency_breakdown']['total_assets_usd'] += float(current_value)
                                            group['currency_breakdown']['unrealized_gain_usd'] += float(unrealized_gain)
                                        group['currency_breakdown']['realized_gain_usd'] += float(realized_gain)

                            # 处理该账户的交易数据
                            account_transactions = [t for t in year_transactions if t.account_id == account.id]
                            for transaction in account_transactions:
                                amount = Decimal(str(transaction.amount or 0))
                                tx_currency = (transaction.currency or 'USD').upper()
                                value = amount * ownership_pct

                                group['transaction_count'] += 1

                                if transaction.type == 'BUY':
                                    group['buy_amount'] += value
                                    if tx_currency == 'CAD':
                                        group['currency_breakdown']['buy_cad'] += float(value)
                                    elif tx_currency == 'USD':
                                        group['currency_breakdown']['buy_usd'] += float(value)
                                elif transaction.type == 'SELL':
                                    group['sell_amount'] += value
                                    if tx_currency == 'CAD':
                                        group['currency_breakdown']['sell_cad'] += float(value)
                                    elif tx_currency == 'USD':
                                        group['currency_breakdown']['sell_usd'] += float(value)
                                elif transaction.type == 'DIVIDEND':
                                    group['annual_dividends'] += value
                                    if tx_currency == 'CAD':
                                        group['currency_breakdown']['dividends_cad'] += float(value)
                                    elif tx_currency == 'USD':
                                        group['currency_breakdown']['dividends_usd'] += float(value)
                                elif transaction.type == 'INTEREST':
                                    group['annual_interest'] += value
                                    if tx_currency == 'CAD':
                                        group['currency_breakdown']['interest_cad'] += float(value)
                                    elif tx_currency == 'USD':
                                        group['currency_breakdown']['interest_usd'] += float(value)
                                elif transaction.type == 'DEPOSIT':
                                    group['deposit_amount'] += value
                                    if tx_currency == 'CAD':
                                        group['currency_breakdown']['deposit_cad'] += float(value)
                                    elif tx_currency == 'USD':
                                        group['currency_breakdown']['deposit_usd'] += float(value)
                                elif transaction.type == 'WITHDRAWAL':
                                    group['withdrawal_amount'] += value
                                    if tx_currency == 'CAD':
                                        group['currency_breakdown']['withdrawal_cad'] += float(value)
                                    elif tx_currency == 'USD':
                                        group['currency_breakdown']['withdrawal_usd'] += float(value)

                        # 为每个账户类型创建详细行
                        for account_type_name, group_data in member_account_type_groups.items():
                            # 计算该账户类型的现金余额
                            type_cash_cad = Decimal('0')
                            type_cash_usd = Decimal('0')

                            for account, account_type, membership in member_specific_accounts:
                                if account_type.name == account_type_name:
                                    ownership_pct = Decimal(str(membership.ownership_percentage or 0)) / Decimal('100')
                                    snapshot = asset_service.get_asset_snapshot(account.id, year_end)
                                    type_cash_cad += snapshot.cash_balance_cad * ownership_pct
                                    type_cash_usd += snapshot.cash_balance_usd * ownership_pct

                            total_assets_with_cash = group_data['total_assets'] + type_cash_cad + type_cash_usd * exchange_rate_decimal

                            annual_data.append({
                                'year': year,
                                'member_name': member.name,
                                'member_id': member.id,
                                'account_type': account_type_name,
                                'is_member_account_type_row': True,  # 标识这是成员+账户类型详细行
                                'total_assets': float(total_assets_with_cash),
                                'annual_realized_gain': float(group_data['annual_realized_gain']),
                                'annual_unrealized_gain': float(group_data['annual_unrealized_gain']),
                                'annual_dividends': float(group_data['annual_dividends']),
                                'annual_interest': float(group_data['annual_interest']),
                                'annual_income': float(group_data['annual_dividends'] + group_data['annual_interest']),
                                'transaction_count': group_data['transaction_count'],
                                'buy_amount': float(group_data['buy_amount']),
                                'sell_amount': float(group_data['sell_amount']),
                                'deposit_amount': float(group_data['deposit_amount']),
                                'withdrawal_amount': float(group_data['withdrawal_amount']),
                                'annual_usd_cad_rate': annual_usd_cad_rate_float,
                                'currency_breakdown': {
                                    'total_assets_cad': group_data['currency_breakdown']['total_assets_cad'] + float(type_cash_cad),
                                    'total_assets_usd': group_data['currency_breakdown']['total_assets_usd'] + float(type_cash_usd),
                                    **group_data['currency_breakdown'],
                                    'cash_cad': float(type_cash_cad),
                                    'cash_usd': float(type_cash_usd)
                                },
                                'cash_balance': {
                                    'cad': float(type_cash_cad),
                                    'usd': float(type_cash_usd)
                                }
                            })

        # 计算图表数据
        chart_data = self._prepare_annual_chart_data(annual_data)

        # 只计算联合账户数据，不包括成员数据和成员账户类型数据
        total_realized_sum = sum(item.get('annual_realized_gain', 0) or 0
                               for item in annual_data
                               if not item.get('is_member_row', False) and not item.get('is_member_account_type_row', False))

        # 获取当前持仓的总未实现收益，而不是年度增量的总和
        current_portfolio = self.get_portfolio_summary(account_ids)
        current_total_unrealized = current_portfolio.get('summary', {}).get('total_unrealized_gain', 0)

        # 如果是成员数据，需要按比例分配总未实现收益
        if member_id is not None and annual_data:
            # 找到成员数据行
            member_rows = [item for item in annual_data if item.get('is_member_row') and item.get('member_id') == member_id]
            if member_rows:
                # 获取该成员的持股比例
                member_row = member_rows[0]
                latest_joint_row = [item for item in annual_data if not item.get('is_member_row')][-1] if annual_data else None
                if latest_joint_row:
                    # 计算持股比例
                    joint_total_assets = latest_joint_row.get('total_assets', 0)
                    member_total_assets = member_row.get('total_assets', 0)
                    if joint_total_assets > 0:
                        ownership_ratio = member_total_assets / joint_total_assets
                        current_total_unrealized = current_total_unrealized * ownership_ratio

        total_income_sum = sum((item.get('annual_dividends', 0) or 0) + (item.get('annual_interest', 0) or 0)
                               for item in annual_data
                               if not item.get('is_member_row', False) and not item.get('is_member_account_type_row', False))

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
                'years_covered': len([item for item in annual_data if not item.get('is_member_row', False) and not item.get('is_member_account_type_row', False)]),
                'total_years_gain': sum(item['annual_realized_gain'] + item['annual_unrealized_gain']
                                      for item in annual_data
                                      if not item.get('is_member_row', False) and not item.get('is_member_account_type_row', False)),
                'total_dividends': sum(item['annual_dividends'] for item in annual_data
                                     if not item.get('is_member_row', False) and not item.get('is_member_account_type_row', False)),
                'total_interest': sum(item['annual_interest'] for item in annual_data
                                    if not item.get('is_member_row', False) and not item.get('is_member_account_type_row', False)),
                'average_annual_return': self._calculate_average_annual_return(annual_data),
                'total_realized_gain': total_realized_sum,
                'total_unrealized_gain': current_total_unrealized,
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

        asset_service = AssetValuationService()
        usd_to_cad_rate = currency_service.get_current_rate('USD', 'CAD') or 1
        try:
            usd_to_cad_decimal = Decimal(str(usd_to_cad_rate))
        except (InvalidOperation, TypeError):
            usd_to_cad_decimal = Decimal('1')

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

        def calculate_realized_totals(portfolio_summary: Optional[Dict]) -> Dict[str, Decimal]:
            totals = {
                'total': Decimal('0'),
                'cad': Decimal('0'),
                'usd': Decimal('0')
            }
            if not portfolio_summary:
                return totals

            for collection in ('current_holdings', 'cleared_holdings'):
                for holding in (portfolio_summary.get(collection, []) or []):
                    try:
                        proportion_dec = get_proportion(holding.get('account_id'))
                    except Exception:
                        proportion_dec = Decimal('1')
                    if proportion_dec <= 0:
                        continue
                    realized_value = Decimal(str(holding.get('realized_gain', 0) or 0)) * proportion_dec
                    totals['total'] += realized_value
                    currency = (holding.get('currency') or 'USD').upper()
                    if currency == 'CAD':
                        totals['cad'] += realized_value
                    elif currency == 'USD':
                        totals['usd'] += realized_value
            return totals

        def compute_totals_for_date(as_of_date: date) -> Dict[str, float]:
            portfolio = self.get_portfolio_summary(account_ids, TimePeriod.CUSTOM, end_date=as_of_date)
            realized_totals = calculate_realized_totals(portfolio)
            current_holdings = portfolio.get('current_holdings', []) or []

            total_assets_stock_dec = Decimal('0')
            total_assets_cad = 0.0
            total_assets_usd = 0.0
            unrealized_gain_cad = 0.0
            unrealized_gain_usd = 0.0
            total_unrealized_dec = Decimal('0')

            for holding in current_holdings:
                proportion_dec = get_proportion(holding.get('account_id'))
                if proportion_dec <= 0:
                    continue
                value_dec = Decimal(str(holding.get('current_value', 0) or 0)) * proportion_dec
                unrealized_dec = Decimal(str(holding.get('unrealized_gain', 0) or 0)) * proportion_dec
                total_assets_stock_dec += value_dec
                total_unrealized_dec += unrealized_dec
                currency = (holding.get('currency') or 'USD').upper()
                if currency == 'CAD':
                    total_assets_cad += float(value_dec)
                    unrealized_gain_cad += float(unrealized_dec)
                elif currency == 'USD':
                    total_assets_usd += float(value_dec)
                    unrealized_gain_usd += float(unrealized_dec)

            cash_total_cad_dec = Decimal('0')
            cash_total_usd_dec = Decimal('0')
            for account_id in account_ids:
                proportion_dec = get_proportion(account_id)
                if proportion_dec <= 0:
                    continue
                try:
                    snapshot = asset_service.get_asset_snapshot(account_id, as_of_date)
                except Exception:
                    continue
                cash_total_cad_dec += Decimal(str(snapshot.cash_balance_cad or 0)) * proportion_dec
                cash_total_usd_dec += Decimal(str(snapshot.cash_balance_usd or 0)) * proportion_dec

            total_assets_dec = total_assets_stock_dec + cash_total_cad_dec + (cash_total_usd_dec * usd_to_cad_decimal)
            if cash_total_cad_dec < 0:
                cash_total_cad_dec = Decimal('0')
            if cash_total_usd_dec < 0:
                cash_total_usd_dec = Decimal('0')

            total_assets_float = float(total_assets_dec)
            total_assets_cad += float(cash_total_cad_dec)
            total_assets_usd += float(cash_total_usd_dec)

            # Apply current exchange rate to realized and unrealized gains
            total_realized_with_rate = float(realized_totals['cad']) + float(realized_totals['usd']) * float(usd_to_cad_decimal)
            total_unrealized_with_rate = unrealized_gain_cad + unrealized_gain_usd * float(usd_to_cad_decimal)

            total_unrealized_float = float(total_unrealized_dec)
            total_realized_float = float(realized_totals['total'])
            overall_return_percent = 0.0
            if total_assets_float > 0:
                overall_return_percent = ((total_realized_with_rate + total_unrealized_with_rate) / total_assets_float) * 100

            return {
                'total_assets': total_assets_float,
                'total_assets_cad': total_assets_cad,
                'total_assets_usd': total_assets_usd,
                'realized_gain': total_realized_with_rate,
                'realized_gain_cad': float(realized_totals['cad']),
                'realized_gain_usd': float(realized_totals['usd']),
                'unrealized_gain': total_unrealized_with_rate,
                'unrealized_gain_cad': unrealized_gain_cad,
                'unrealized_gain_usd': unrealized_gain_usd,
                'cash_cad': float(cash_total_cad_dec),
                'cash_usd': float(cash_total_usd_dec),
                'return_percent': overall_return_percent
            }

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

                current_realized_totals = calculate_realized_totals(quarter_portfolio)
                previous_end = quarter_start - timedelta(days=1)
                previous_totals = {
                    'total': Decimal('0'),
                    'cad': Decimal('0'),
                    'usd': Decimal('0')
                }
                if previous_end.year >= 1:
                    previous_portfolio = self.get_portfolio_summary(
                        account_ids,
                        TimePeriod.CUSTOM,
                        end_date=previous_end
                    )
                    previous_totals = calculate_realized_totals(previous_portfolio)

                quarterly_realized_gain_dec = current_realized_totals['total'] - previous_totals['total']
                quarterly_realized_gain_cad_dec = current_realized_totals['cad'] - previous_totals['cad']
                quarterly_realized_gain_usd_dec = current_realized_totals['usd'] - previous_totals['usd']

                total_assets_cad = 0.0
                total_assets_usd = 0.0
                unrealized_gain_cad = 0.0
                unrealized_gain_usd = 0.0
                total_assets_stock_dec = Decimal('0')

                current_holdings = quarter_portfolio.get('current_holdings', [])
                for holding in current_holdings:
                    proportion_dec = get_proportion(holding.get('account_id'))
                    if proportion_dec <= 0:
                        continue
                    value_dec = Decimal(str(holding.get('current_value', 0))) * proportion_dec
                    total_assets_stock_dec += value_dec
                    if (holding.get('currency') or '').upper() == 'CAD':
                        total_assets_cad += float(value_dec)
                        unrealized_gain_cad += float(Decimal(str(holding.get('unrealized_gain', 0))) * proportion_dec)
                    elif (holding.get('currency') or '').upper() == 'USD':
                        total_assets_usd += float(value_dec)
                        unrealized_gain_usd += float(Decimal(str(holding.get('unrealized_gain', 0))) * proportion_dec)

                # 使用统一的 get_portfolio_summary 计算季度增量未实现收益
                quarter_end_portfolio = self.get_portfolio_summary(
                    account_ids, TimePeriod.CUSTOM, effective_end, effective_end
                )
                quarter_start_portfolio = self.get_portfolio_summary(
                    account_ids, TimePeriod.CUSTOM, quarter_start - timedelta(days=1), quarter_start - timedelta(days=1)
                )

                # 计算季度增量未实现收益
                quarter_end_unrealized = quarter_end_portfolio.get('summary', {}).get('total_unrealized_gain', 0)
                quarter_start_unrealized = quarter_start_portfolio.get('summary', {}).get('total_unrealized_gain', 0)
                quarterly_unrealized_gain = quarter_end_unrealized - quarter_start_unrealized

                quarterly_realized_gain = float(quarterly_realized_gain_dec)
                quarterly_realized_gain_cad = float(quarterly_realized_gain_cad_dec)
                quarterly_realized_gain_usd = float(quarterly_realized_gain_usd_dec)

                # 计算按货币分类的季度增量未实现收益
                quarter_end_unrealized_cad_dec = Decimal('0')
                quarter_end_unrealized_usd_dec = Decimal('0')
                quarter_start_unrealized_cad_dec = Decimal('0')
                quarter_start_unrealized_usd_dec = Decimal('0')

                for holding in quarter_end_portfolio.get('current_holdings', []):
                    proportion = get_proportion(holding.get('account_id'))
                    if proportion <= 0:
                        continue
                    unrealized = Decimal(str(holding.get('unrealized_gain', 0) or 0)) * proportion
                    currency = (holding.get('currency') or '').upper()
                    if currency == 'CAD':
                        quarter_end_unrealized_cad_dec += unrealized
                    elif currency == 'USD':
                        quarter_end_unrealized_usd_dec += unrealized

                for holding in quarter_start_portfolio.get('current_holdings', []):
                    proportion = get_proportion(holding.get('account_id'))
                    if proportion <= 0:
                        continue
                    unrealized = Decimal(str(holding.get('unrealized_gain', 0) or 0)) * proportion
                    currency = (holding.get('currency') or '').upper()
                    if currency == 'CAD':
                        quarter_start_unrealized_cad_dec += unrealized
                    elif currency == 'USD':
                        quarter_start_unrealized_usd_dec += unrealized

                unrealized_gain_cad = float(quarter_end_unrealized_cad_dec - quarter_start_unrealized_cad_dec)
                unrealized_gain_usd = float(quarter_end_unrealized_usd_dec - quarter_start_unrealized_usd_dec)

                cash_total_cad_dec = Decimal('0')
                cash_total_usd_dec = Decimal('0')
                total_assets_with_cash_dec = Decimal('0')

                for account_id in account_ids:
                    proportion_dec = get_proportion(account_id)
                    if proportion_dec <= 0:
                        continue
                    try:
                        snapshot = asset_service.get_asset_snapshot(account_id, effective_end)
                    except Exception:
                        continue

                    cash_cad_dec = Decimal(str(snapshot.cash_balance_cad or 0)) * proportion_dec
                    cash_usd_dec = Decimal(str(snapshot.cash_balance_usd or 0)) * proportion_dec
                    cash_total_cad_dec += cash_cad_dec
                    cash_total_usd_dec += cash_usd_dec
                    total_assets_with_cash_dec += Decimal(str(snapshot.total_assets or 0)) * proportion_dec

                total_assets_dec = total_assets_stock_dec + cash_total_cad_dec + (cash_total_usd_dec * usd_to_cad_decimal)
                if total_assets_with_cash_dec > 0:
                    total_assets_dec = total_assets_with_cash_dec

                if cash_total_cad_dec < 0:
                    cash_total_cad_dec = Decimal('0')
                if cash_total_usd_dec < 0:
                    cash_total_usd_dec = Decimal('0')

                total_assets_float = float(total_assets_dec)
                total_assets_cad += float(cash_total_cad_dec)
                total_assets_usd += float(cash_total_usd_dec * usd_to_cad_decimal)

                quarterly_return_percent = 0.0
                if total_assets_float > 0:
                    quarterly_return_percent = ((quarterly_realized_gain + quarterly_unrealized_gain) / total_assets_float) * 100

                quarterly_data.append({
                    'year': year,
                    'quarter': quarter,
                    'total_assets': total_assets_float,
                    'quarterly_realized_gain': quarterly_realized_gain,
                    'quarterly_unrealized_gain': quarterly_unrealized_gain,
                    'quarterly_return_percent': quarterly_return_percent,
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
                        'cash_cad': float(cash_total_cad_dec),
                        'cash_usd': float(cash_total_usd_dec),
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
    
        current_totals = compute_totals_for_date(today)

        return {
            'quarterly_data': quarterly_data,
            'summary': {
                'quarters_covered': len(quarterly_data),
                'total_quarters_gain': sum(item['quarterly_realized_gain'] + item['quarterly_unrealized_gain'] 
                                         for item in quarterly_data),
                'total_dividends': sum(item['quarterly_dividends'] for item in quarterly_data),
                'total_interest': sum(item['quarterly_interest'] for item in quarterly_data),
                'average_quarterly_return': self._calculate_average_return(quarterly_data, 'quarterly_realized_gain', 'quarterly_unrealized_gain')
            },
            'current_totals': current_totals
        }


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
                            months: Optional[int] = None,
                            member_id: Optional[int] = None) -> Dict:
        """获取月度分析数据

        Args:
            account_ids: 账户ID列表
            months: 要分析的月数，如果为None则自动计算从第一条交易记录到现在的所有月份

        Returns:
            包含月度统计数据的字典
        """
        today = date.today()
        base_month_start = today.replace(day=1)

        # 如果没有指定月数，从第一条交易记录开始计算
        if months is None:
            # 获取最早的交易日期
            earliest_transaction = Transaction.query.filter(
                Transaction.account_id.in_(account_ids)
            ).order_by(Transaction.trade_date.asc()).first()

            if earliest_transaction:
                earliest_date = earliest_transaction.trade_date
                earliest_month_start = earliest_date.replace(day=1)

                # 计算从最早交易月份到当前月份的月数
                months_diff = (today.year - earliest_month_start.year) * 12 + (today.month - earliest_month_start.month) + 1
                months = months_diff
            else:
                months = 12  # 如果没有交易记录，默认12个月

        monthly_data = []

        asset_service = AssetValuationService()
        usd_to_cad_rate = currency_service.get_current_rate('USD', 'CAD') or 1
        try:
            usd_to_cad_decimal = Decimal(str(usd_to_cad_rate))
        except (InvalidOperation, TypeError):
            usd_to_cad_decimal = Decimal('1')

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

        def calculate_realized_totals(portfolio_summary: Optional[Dict]) -> Dict[str, Decimal]:
            totals = {
                'total': Decimal('0'),
                'cad': Decimal('0'),
                'usd': Decimal('0')
            }
            if not portfolio_summary:
                return totals

            for collection in ('current_holdings', 'cleared_holdings'):
                for holding in (portfolio_summary.get(collection, []) or []):
                    try:
                        proportion_dec = get_proportion(holding.get('account_id'))
                    except Exception:
                        proportion_dec = Decimal('1')
                    if proportion_dec <= 0:
                        continue
                    realized_value = Decimal(str(holding.get('realized_gain', 0) or 0)) * proportion_dec
                    totals['total'] += realized_value
                    currency = (holding.get('currency') or 'USD').upper()
                    if currency == 'CAD':
                        totals['cad'] += realized_value
                    elif currency == 'USD':
                        totals['usd'] += realized_value
            return totals

        def compute_totals_for_date(as_of_date: date) -> Dict[str, float]:
            portfolio = self.get_portfolio_summary(account_ids, TimePeriod.CUSTOM, end_date=as_of_date)
            realized_totals = calculate_realized_totals(portfolio)
            current_holdings = portfolio.get('current_holdings', []) or []

            total_assets_stock_dec = Decimal('0')
            total_assets_cad = 0.0
            total_assets_usd = 0.0
            unrealized_gain_cad = 0.0
            unrealized_gain_usd = 0.0
            total_unrealized_dec = Decimal('0')

            for holding in current_holdings:
                proportion_dec = get_proportion(holding.get('account_id'))
                if proportion_dec <= 0:
                    continue
                value_dec = Decimal(str(holding.get('current_value', 0) or 0)) * proportion_dec
                unrealized_dec = Decimal(str(holding.get('unrealized_gain', 0) or 0)) * proportion_dec
                total_assets_stock_dec += value_dec
                total_unrealized_dec += unrealized_dec
                currency = (holding.get('currency') or 'USD').upper()
                if currency == 'CAD':
                    total_assets_cad += float(value_dec)
                    unrealized_gain_cad += float(unrealized_dec)
                elif currency == 'USD':
                    total_assets_usd += float(value_dec)
                    unrealized_gain_usd += float(unrealized_dec)

            cash_total_cad_dec = Decimal('0')
            cash_total_usd_dec = Decimal('0')
            for account_id in account_ids:
                proportion_dec = get_proportion(account_id)
                if proportion_dec <= 0:
                    continue
                try:
                    snapshot = asset_service.get_asset_snapshot(account_id, as_of_date)
                except Exception:
                    continue
                cash_total_cad_dec += Decimal(str(snapshot.cash_balance_cad or 0)) * proportion_dec
                cash_total_usd_dec += Decimal(str(snapshot.cash_balance_usd or 0)) * proportion_dec

            total_assets_dec = total_assets_stock_dec + cash_total_cad_dec + (cash_total_usd_dec * usd_to_cad_decimal)
            if cash_total_cad_dec < 0:
                cash_total_cad_dec = Decimal('0')
            if cash_total_usd_dec < 0:
                cash_total_usd_dec = Decimal('0')

            total_assets_float = float(total_assets_dec)
            total_assets_cad += float(cash_total_cad_dec)
            total_assets_usd += float(cash_total_usd_dec)

            # Apply current exchange rate to realized and unrealized gains
            total_realized_with_rate = float(realized_totals['cad']) + float(realized_totals['usd']) * float(usd_to_cad_decimal)
            total_unrealized_with_rate = unrealized_gain_cad + unrealized_gain_usd * float(usd_to_cad_decimal)

            total_unrealized_float = float(total_unrealized_dec)
            total_realized_float = float(realized_totals['total'])
            overall_return_percent = 0.0
            if total_assets_float > 0:
                overall_return_percent = ((total_realized_with_rate + total_unrealized_with_rate) / total_assets_float) * 100

            return {
                'total_assets': total_assets_float,
                'total_assets_cad': total_assets_cad,
                'total_assets_usd': total_assets_usd,
                'realized_gain': total_realized_with_rate,
                'realized_gain_cad': float(realized_totals['cad']),
                'realized_gain_usd': float(realized_totals['usd']),
                'unrealized_gain': total_unrealized_with_rate,
                'unrealized_gain_cad': unrealized_gain_cad,
                'unrealized_gain_usd': unrealized_gain_usd,
                'cash_cad': float(cash_total_cad_dec),
                'cash_usd': float(cash_total_usd_dec),
                'return_percent': overall_return_percent
            }

        earliest_month_start: Optional[date] = None
        months_span = 1
        earliest_transaction = Transaction.query.filter(
            Transaction.account_id.in_(account_ids)
        ).order_by(Transaction.trade_date.asc()).first()
        if earliest_transaction and earliest_transaction.trade_date:
            earliest_trade_date = earliest_transaction.trade_date
            earliest_month_start = earliest_trade_date.replace(day=1)
            months_span = ((base_month_start.year - earliest_month_start.year) * 12 +
                           (base_month_start.month - earliest_month_start.month) + 1)
            if months_span < 1:
                months_span = 1
        else:
            earliest_month_start = base_month_start
            months_span = max(1, months or 1 if months else 1)

        total_months = max(months or 0, months_span)
        if total_months <= 0:
            total_months = months_span

        for offset in range(total_months):
            target_start = self._shift_month(base_month_start, offset)
            if not target_start:
                continue

            if earliest_month_start and target_start < earliest_month_start:
                break

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
            
            current_realized_totals = calculate_realized_totals(month_portfolio)
            previous_end = target_start - timedelta(days=1)
            previous_realized_totals = {
                'total': Decimal('0'),
                'cad': Decimal('0'),
                'usd': Decimal('0')
            }
            if previous_end.year >= 1:
                previous_portfolio = self.get_portfolio_summary(
                    account_ids,
                    TimePeriod.CUSTOM,
                    end_date=previous_end
                )
                previous_realized_totals = calculate_realized_totals(previous_portfolio)

            monthly_realized_gain_dec = current_realized_totals['total'] - previous_realized_totals['total']
            monthly_realized_gain_cad_dec = current_realized_totals['cad'] - previous_realized_totals['cad']
            monthly_realized_gain_usd_dec = current_realized_totals['usd'] - previous_realized_totals['usd']
            
            # 计算按货币分组的总资产和浮动收益（含现金）
            total_assets_cad = 0.0
            total_assets_usd = 0.0
            unrealized_gain_cad = 0.0
            unrealized_gain_usd = 0.0
            cash_total_cad_dec = Decimal('0')
            cash_total_usd_dec = Decimal('0')
            total_assets_stock_dec = Decimal('0')
            total_assets_with_cash_dec = Decimal('0')

            current_holdings = month_portfolio.get('current_holdings', [])
            for holding in current_holdings:
                proportion_dec = get_proportion(holding.get('account_id'))
                value_dec = Decimal(str(holding.get('current_value', 0))) * proportion_dec
                total_assets_stock_dec += value_dec
                if (holding.get('currency') or '').upper() == 'CAD':
                    total_assets_cad += float(value_dec)
                    unrealized_gain_cad += float(Decimal(str(holding.get('unrealized_gain', 0))) * proportion_dec)
                elif (holding.get('currency') or '').upper() == 'USD':
                    total_assets_usd += float(value_dec)
                    unrealized_gain_usd += float(Decimal(str(holding.get('unrealized_gain', 0))) * proportion_dec)

            # 使用统一的 get_portfolio_summary 计算月度增量未实现收益
            month_end_portfolio = self.get_portfolio_summary(
                account_ids, TimePeriod.CUSTOM, effective_end, effective_end
            )
            month_start_portfolio = self.get_portfolio_summary(
                account_ids, TimePeriod.CUSTOM, target_start - timedelta(days=1), target_start - timedelta(days=1)
            )

            # 计算月度增量未实现收益
            month_end_unrealized = month_end_portfolio.get('summary', {}).get('total_unrealized_gain', 0)
            month_start_unrealized = month_start_portfolio.get('summary', {}).get('total_unrealized_gain', 0)
            monthly_unrealized_gain = month_end_unrealized - month_start_unrealized

            monthly_realized_gain = float(monthly_realized_gain_dec)
            monthly_realized_gain_cad = float(monthly_realized_gain_cad_dec)
            monthly_realized_gain_usd = float(monthly_realized_gain_usd_dec)

            # 计算按货币分类的月度增量未实现收益
            month_end_unrealized_cad_dec = Decimal('0')
            month_end_unrealized_usd_dec = Decimal('0')
            month_start_unrealized_cad_dec = Decimal('0')
            month_start_unrealized_usd_dec = Decimal('0')

            for holding in month_end_portfolio.get('current_holdings', []):
                proportion = get_proportion(holding.get('account_id'))
                if proportion <= 0:
                    continue
                unrealized = Decimal(str(holding.get('unrealized_gain', 0) or 0)) * proportion
                currency = (holding.get('currency') or '').upper()
                if currency == 'CAD':
                    month_end_unrealized_cad_dec += unrealized
                elif currency == 'USD':
                    month_end_unrealized_usd_dec += unrealized

            for holding in month_start_portfolio.get('current_holdings', []):
                proportion = get_proportion(holding.get('account_id'))
                if proportion <= 0:
                    continue
                unrealized = Decimal(str(holding.get('unrealized_gain', 0) or 0)) * proportion
                currency = (holding.get('currency') or '').upper()
                if currency == 'CAD':
                    month_start_unrealized_cad_dec += unrealized
                elif currency == 'USD':
                    month_start_unrealized_usd_dec += unrealized

            unrealized_gain_cad = float(month_end_unrealized_cad_dec - month_start_unrealized_cad_dec)
            unrealized_gain_usd = float(month_end_unrealized_usd_dec - month_start_unrealized_usd_dec)

            # 统计现金余额（按月末快照）
            for account_id in account_ids:
                proportion_dec = get_proportion(account_id)
                if proportion_dec <= 0:
                    continue
                try:
                    snapshot = asset_service.get_asset_snapshot(account_id, effective_end)
                except Exception:
                    continue

                cash_cad_dec = Decimal(str(snapshot.cash_balance_cad or 0)) * proportion_dec
                cash_usd_dec = Decimal(str(snapshot.cash_balance_usd or 0)) * proportion_dec
                cash_total_cad_dec += cash_cad_dec
                cash_total_usd_dec += cash_usd_dec
                total_assets_with_cash_dec += Decimal(str(snapshot.total_assets or 0)) * proportion_dec

            total_assets_dec = total_assets_stock_dec + cash_total_cad_dec + (cash_total_usd_dec * usd_to_cad_decimal)

            if total_assets_with_cash_dec > 0:
                total_assets_dec = total_assets_with_cash_dec

            if cash_total_cad_dec < 0:
                cash_total_cad_dec = Decimal('0')
            if cash_total_usd_dec < 0:
                cash_total_usd_dec = Decimal('0')

            total_assets_float = float(total_assets_dec)
            total_assets_cad += float(cash_total_cad_dec)
            total_assets_usd += float(cash_total_usd_dec)

            # Apply current exchange rate to all monetary values
            monthly_realized_gain_with_rate = monthly_realized_gain_cad + monthly_realized_gain_usd * float(usd_to_cad_decimal)
            monthly_unrealized_gain_with_rate = unrealized_gain_cad + unrealized_gain_usd * float(usd_to_cad_decimal)
            dividends_with_rate = dividends_cad + dividends_usd * float(usd_to_cad_decimal)
            interest_with_rate = interest_cad + interest_usd * float(usd_to_cad_decimal)
            buy_amount_with_rate = buy_cad + buy_usd * float(usd_to_cad_decimal)
            sell_amount_with_rate = sell_cad + sell_usd * float(usd_to_cad_decimal)

            monthly_return_percent = 0.0
            if total_assets_float > 0:
                monthly_return_percent = ((monthly_realized_gain_with_rate + monthly_unrealized_gain_with_rate) / total_assets_float) * 100

            monthly_data.append({
                'year': target_start.year,
                'month': target_start.month,
                'month_name': target_start.strftime('%Y-%m'),
                'total_assets': total_assets_float,
                'monthly_realized_gain': monthly_realized_gain_with_rate,
                'monthly_unrealized_gain': monthly_unrealized_gain_with_rate,
                'monthly_return_percent': monthly_return_percent,
                'monthly_dividends': dividends_with_rate,
                'monthly_interest': interest_with_rate,
                'transaction_count': transaction_count,
                'buy_amount': buy_amount_with_rate,
                'sell_amount': sell_amount_with_rate,
                'currency_breakdown': {
                    'total_assets_cad': total_assets_cad,
                    'total_assets_usd': total_assets_usd,
                    'realized_gain_cad': monthly_realized_gain_cad,
                    'realized_gain_usd': monthly_realized_gain_usd,
                    'unrealized_gain_cad': unrealized_gain_cad,
                    'unrealized_gain_usd': unrealized_gain_usd,
                    'cash_cad': float(cash_total_cad_dec),
                    'cash_usd': float(cash_total_usd_dec),
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
        
        current_totals = compute_totals_for_date(today)

        return {
            'monthly_data': monthly_data,
            'summary': {
                'months_covered': len(monthly_data),
                'total_months_gain': sum(item['monthly_realized_gain'] + item['monthly_unrealized_gain'] 
                                       for item in monthly_data),
                'total_dividends': sum(item['monthly_dividends'] for item in monthly_data),
                'total_interest': sum(item['monthly_interest'] for item in monthly_data),
                'average_monthly_return': self._calculate_average_return(monthly_data, 'monthly_realized_gain', 'monthly_unrealized_gain')
            },
            'current_totals': current_totals
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
            # 使用统一的计算逻辑：股票价值 + 现金余额 = 总资产

            # 1. 获取股票价值（使用统一的get_portfolio_summary）
            try:
                portfolio_data = self.get_portfolio_summary(
                    account_ids, TimePeriod.CUSTOM, current_date, current_date
                )
                stock_value = Decimal(str(portfolio_data.get('summary', {}).get('total_current_value', 0)))
            except Exception as e:
                logger.warning(f"获取{current_date}股票数据失败，使用0: {e}")
                stock_value = Decimal('0')

            # 2. 获取现金余额（使用统一的get_cash_balance）
            cash_total = Decimal('0')
            for account_id in account_ids:
                proportion = get_proportion(account_id)
                if proportion <= 0:
                    continue

                try:
                    # 统一使用Cash表数据，不做历史推算
                    cash_balance = asset_service.get_cash_balance(account_id, current_date)
                    cash_total += Decimal(str(cash_balance['total_cad'])) * proportion
                except Exception as e:
                    logger.warning(f"获取账户{account_id}在{current_date}现金数据失败: {e}")
                    continue

            # 3. 计算总资产 = 股票 + 现金
            total_value = stock_value + cash_total
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
        # 修复：使用第一天的值作为基准，而不是第一个大于0的值
        actual_base_value = portfolio_values[0][1] if portfolio_values else Decimal('0')

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
            # 只计算联合账户数据，跳过成员数据
            if not item.get('is_member_row', False) and item['total_assets'] > 0:
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
            portfolio_summary = self.get_portfolio_summary(account_ids, TimePeriod.CUSTOM, end_date=current_date)
            
            # 使用正确的数据结构：current_holdings是直接的持仓列表
            current_holdings = portfolio_summary.get('current_holdings', [])
            
            asset_service = AssetValuationService()

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
            unique_stock_count = len(by_stocks)

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
            total_value_base = portfolio_summary.get('summary', {}).get('total_current_value', 0)
            try:
                total_value_decimal = Decimal(str(total_value_base or 0))
            except (InvalidOperation, TypeError):
                total_value_decimal = Decimal('0')

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

            # 4. 按账户分布
            by_account = defaultdict(lambda: {'value': 0, 'holdings_count': 0})
            account_info = {}

            def ensure_account_info(account_identifier: int):
                if account_identifier in account_info:
                    return
                account = Account.query.get(account_identifier)
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
                account_info[account_identifier] = {
                    'name': account.name if account else f'Account {account_identifier}',
                    'is_joint': account.is_joint if account else False,
                    'members': members
                }

            for holding in current_holdings:
                account_id = holding['account_id']
                current_value = float(holding['current_value'])
                
                if current_value > 0:
                    by_account[account_id]['value'] += current_value
                    by_account[account_id]['holdings_count'] += 1
                    
                    # 获取账户信息
                    ensure_account_info(account_id)

            cash_total_cad = Decimal('0')
            cash_cad_total = Decimal('0')
            cash_usd_total = Decimal('0')

            for account_id in account_ids:
                try:
                    snapshot = asset_service.get_asset_snapshot(account_id, current_date)
                except Exception as exc:
                    logger.warning(f"无法获取账户{account_id}的现金余额: {exc}")
                    continue

                ensure_account_info(account_id)

                cash_total_cad += snapshot.cash_balance_total_cad
                cash_cad_total += snapshot.cash_balance_cad
                cash_usd_total += snapshot.cash_balance_usd

                cash_value_cad = float(snapshot.cash_balance_total_cad)
                if cash_value_cad > 0:
                    by_account[account_id]['value'] += cash_value_cad

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

            # 5. 按成员分布（仅在存在成员信息时计算）
            member_totals: Dict[int, Dict[str, float]] = {}
            for account_entry in by_account_list:
                for member_entry in account_entry.get('members', []):
                    member_id = member_entry.get('member_id')
                    if member_id is None:
                        continue
                    member_value = member_entry.get('value', 0) or 0
                    if member_value <= 0:
                        continue
                    if member_id not in member_totals:
                        member_totals[member_id] = {
                            'member_id': member_id,
                            'name': member_entry.get('name') or 'Member',
                            'value': 0.0
                        }
                    member_totals[member_id]['value'] += float(member_value)

            by_member_list = sorted(
                (member for member in member_totals.values() if member['value'] > 0),
                key=lambda item: item['value'],
                reverse=True
            )

            cash_total_cad_float = float(cash_total_cad)
            if cash_total_cad_float > 0:
                by_stocks.append({
                    'symbol': 'Cash',
                    'name': 'Cash',
                    'value': cash_total_cad_float,
                    'is_cash': True
                })
                by_category_list.append({
                    'category': 'Cash',
                    'value': cash_total_cad_float,
                    'stocks_count': None,
                    'is_cash': True
                })

            if cash_cad_total > 0:
                cad_value += float(cash_cad_total)
            if cash_usd_total > 0:
                usd_value += float(cash_usd_total)

            # 重新构建币种列表，确保现金也被计入
            by_currency = []
            if cad_value > 0:
                by_currency.append({'currency': 'CAD', 'value': cad_value})
            if usd_value > 0:
                by_currency.append({'currency': 'USD', 'value': usd_value})

            total_value_decimal += cash_total_cad
            total_value = float(total_value_decimal)

            return {
                'summary': {
                    'total_value_cad': total_value,  # 所有值已经转换为CAD
                    'unique_stocks': unique_stock_count,
                    'categories_count': len(by_category_list),
                    'accounts_count': len(by_account_list),
                    'members_count': len(by_member_list)
                },
                'by_stocks': by_stocks,
                'by_category': by_category_list,
                'by_currency': by_currency,
                'by_account': by_account_list,
                'by_member': by_member_list
            }
            
        except Exception as e:
            logger.error(f"Error getting holdings distribution: {e}")
            raise

    def clear_cache(self):
        """清除缓存（无操作 - 不再使用本地缓存）"""
        pass

# 全局服务实例
portfolio_service = PortfolioService()
