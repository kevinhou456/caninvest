"""
投资组合统计分析服务

设计理念：
1. 可扩展性：使用基础统计类和时间段枚举，便于添加新的统计维度
2. 易维护性：统计逻辑集中，避免代码重复
3. 性能优化：缓存计算结果，批量查询数据库
4. 多货币支持：CAD/USD自动转换显示
"""

from datetime import datetime, date, timedelta
from typing import Dict, Optional, Tuple, Union
from decimal import Decimal, ROUND_HALF_UP
from enum import Enum
from sqlalchemy import func, and_, or_, text
from collections import defaultdict
import logging

from app import db
from app.models.family import Family
from app.models.member import Member
from app.models.account import Account, AccountType
from app.models.transaction import Transaction
from app.models.stocks_cache import StocksCache
from app.models.contribution import Contribution
from app.models.price_cache import StockPriceCache
from app.services.holdings_service import holdings_service

logger = logging.getLogger(__name__)


class TimePeriod(Enum):
    """时间段枚举"""
    ALL_TIME = "all_time"
    YTD = "ytd"
    LAST_YEAR = "last_year"
    LAST_30_DAYS = "last_30_days"
    LAST_90_DAYS = "last_90_days"
    LAST_365_DAYS = "last_365_days"
    CUSTOM = "custom"


class Currency(Enum):
    """货币枚举"""
    CAD = "CAD"
    USD = "USD"


class PortfolioMetrics:
    """投资组合指标数据类"""
    
    def __init__(self):
        # 基础资产统计
        self.total_assets_cad = Decimal('0')
        self.total_assets_usd = Decimal('0')
        
        # 收益统计
        self.total_return_cad = Decimal('0')
        self.total_return_usd = Decimal('0')
        self.realized_gain_cad = Decimal('0')
        self.realized_gain_usd = Decimal('0')
        self.unrealized_gain_cad = Decimal('0')
        self.unrealized_gain_usd = Decimal('0')
        
        # 股息统计
        self.total_dividends_cad = Decimal('0')
        self.total_dividends_usd = Decimal('0')
        
        # 利息统计
        self.total_interest_cad = Decimal('0')
        self.total_interest_usd = Decimal('0')
        
        # 资金流统计
        self.total_deposits_cad = Decimal('0')
        self.total_deposits_usd = Decimal('0')
        self.total_withdrawals_cad = Decimal('0')
        self.total_withdrawals_usd = Decimal('0')
        
        # 费用统计
        self.total_fees_cad = Decimal('0')
        self.total_fees_usd = Decimal('0')
        
        # 汇率
        self.exchange_rate = Decimal('1.35')  # 默认CAD/USD汇率，应从外部获取
        
        # 持仓列表
        self.holdings = []
        
        # 清仓股票列表
        self.cleared_holdings = []
        
        # 账户统计
        self.account_stats = {}
    
    def to_dict(self) -> Dict:
        """转换为字典格式，便于模板使用"""
        return {
            'total_assets': {
                'cad': float(self.total_assets_cad),
                'usd': float(self.total_assets_usd),
                'cad_only': float(getattr(self, 'cad_assets_only', 0)),
                'usd_only': float(getattr(self, 'usd_assets_only', 0))
            },
            'total_return': {
                'cad': float(self.total_return_cad),
                'usd': float(self.total_return_usd),
                'cad_only': float(getattr(self, 'cad_return_only', 0)),
                'usd_only': float(getattr(self, 'usd_return_only', 0))
            },
            'realized_gain': {
                'cad': float(self.realized_gain_cad),
                'usd': float(self.realized_gain_usd),
                'cad_only': float(getattr(self, 'cad_realized_only', 0)),
                'usd_only': float(getattr(self, 'usd_realized_only', 0))
            },
            'unrealized_gain': {
                'cad': float(self.unrealized_gain_cad),
                'usd': float(self.unrealized_gain_usd),
                'cad_only': float(getattr(self, 'cad_unrealized_only', 0)),
                'usd_only': float(getattr(self, 'usd_unrealized_only', 0))
            },
            'total_dividends': {
                'cad': float(self.total_dividends_cad),
                'usd': float(self.total_dividends_usd),
                'cad_only': float(getattr(self, 'cad_dividends_only', 0)),
                'usd_only': float(getattr(self, 'usd_dividends_only', 0))
            },
            'total_interest': {
                'cad': float(self.total_interest_cad),
                'usd': float(self.total_interest_usd),
                'cad_only': float(getattr(self, 'cad_interest_only', 0)),
                'usd_only': float(getattr(self, 'usd_interest_only', 0))
            },
            'total_deposits': {
                'cad': float(self.total_deposits_cad),
                'usd': float(self.total_deposits_usd),
                'cad_only': float(getattr(self, 'cad_deposits_only', 0)),
                'usd_only': float(getattr(self, 'usd_deposits_only', 0))
            },
            'total_withdrawals': {
                'cad': float(self.total_withdrawals_cad),
                'usd': float(self.total_withdrawals_usd),
                'cad_only': float(getattr(self, 'cad_withdrawals_only', 0)),
                'usd_only': float(getattr(self, 'usd_withdrawals_only', 0))
            },
            'exchange_rate': float(self.exchange_rate),
            'holdings': self.holdings,
            'cleared_holdings': self.cleared_holdings,
            'account_stats': self.account_stats
        }


class HoldingInfo:
    """持仓信息数据类"""
    
    def __init__(self, symbol: str, account_id: int):
        self.symbol = symbol
        self.account_id = account_id
        self.account_name = ""
        self.currency = "USD"
        
        # 持仓数量和成本
        self.shares = Decimal('0')
        self.average_cost = Decimal('0')
        self.total_cost = Decimal('0')
        
        # 当前价值
        self.current_price = Decimal('0')
        self.current_value = Decimal('0')
        
        # 收益数据
        self.unrealized_gain = Decimal('0')
        self.unrealized_gain_percent = Decimal('0')
        self.realized_gain = Decimal('0')
        self.dividends = Decimal('0')
        self.interest = Decimal('0')
        
        # 股票信息
        self.company_name = ""
        self.sector = "Unknown"
    
    def to_dict(self) -> Dict:
        """转换为字典格式"""
        return {
            'symbol': self.symbol,
            'account_id': self.account_id,
            'account_name': self.account_name,
            'currency': self.currency,
            'shares': float(self.shares),
            'average_cost': float(self.average_cost),
            'total_cost': float(self.total_cost),
            'current_price': float(self.current_price),
            'current_value': float(self.current_value),
            'unrealized_gain': float(self.unrealized_gain),
            'unrealized_gain_percent': float(self.unrealized_gain_percent),
            'realized_gain': float(self.realized_gain),
            'dividends': float(self.dividends),
            'interest': float(self.interest),
            'company_name': self.company_name,
            'sector': self.sector
        }


class AnalyticsService:
    """投资组合统计分析服务"""
    
    def __init__(self, exchange_rate: Optional[Union[Decimal, float]] = None):
        """
        初始化分析服务
        
        Args:
            exchange_rate: CAD/USD汇率，如果不提供则使用默认值
        """
        if exchange_rate is None:
            self.exchange_rate = Decimal('1.35')
        elif isinstance(exchange_rate, (int, float)):
            self.exchange_rate = Decimal(str(exchange_rate))
        else:
            self.exchange_rate = exchange_rate
        
    def get_time_period_dates(self, period: TimePeriod, 
                            start_date: Optional[date] = None,
                            end_date: Optional[date] = None) -> Tuple[Optional[date], Optional[date]]:
        """
        获取时间段的起始和结束日期
        
        Args:
            period: 时间段枚举
            start_date: 自定义开始日期（用于CUSTOM期间）
            end_date: 自定义结束日期（用于CUSTOM期间）
            
        Returns:
            (start_date, end_date) 元组
        """
        today = date.today()
        
        if period == TimePeriod.ALL_TIME:
            return None, None
        elif period == TimePeriod.YTD:
            return date(today.year, 1, 1), today
        elif period == TimePeriod.LAST_YEAR:
            last_year = today.year - 1
            return date(last_year, 1, 1), date(last_year, 12, 31)
        elif period == TimePeriod.LAST_30_DAYS:
            return today - timedelta(days=30), today
        elif period == TimePeriod.LAST_90_DAYS:
            return today - timedelta(days=90), today
        elif period == TimePeriod.LAST_365_DAYS:
            return today - timedelta(days=365), today
        elif period == TimePeriod.CUSTOM:
            return start_date, end_date
        else:
            return None, None
    
    def get_portfolio_metrics(self, 
                            family_id: int,
                            member_id: Optional[int] = None,
                            account_id: Optional[int] = None,
                            period: TimePeriod = TimePeriod.ALL_TIME,
                            start_date: Optional[date] = None,
                            end_date: Optional[date] = None) -> PortfolioMetrics:
        """
        获取投资组合指标
        
        Args:
            family_id: 家庭ID
            member_id: 成员ID（可选，筛选特定成员）
            account_id: 账户ID（可选，筛选特定账户）  
            period: 时间段
            start_date: 自定义开始日期
            end_date: 自定义结束日期
            
        Returns:
            PortfolioMetrics对象
        """
        # 直接计算，不使用缓存
        
        # 获取时间范围
        period_start, period_end = self.get_time_period_dates(period, start_date, end_date)
        
        # 获取相关账户
        accounts = self._get_filtered_accounts(family_id, member_id, account_id)
        
        # 创建指标对象
        metrics = PortfolioMetrics()
        metrics.exchange_rate = self.exchange_rate
        
        if not accounts:
            return metrics
        
        # 计算各项指标（注意：_calculate_assets需要在_calculate_holdings之后调用）
        self._calculate_holdings(metrics, accounts)
        self._calculate_assets(metrics, accounts)  # 移到这里，在holdings计算之后
        self._calculate_returns(metrics, accounts, period_start, period_end)
        self._calculate_dividends(metrics, accounts, period_start, period_end)
        self._calculate_interest(metrics, accounts, period_start, period_end)
        self._calculate_deposits(metrics, accounts, period_start, period_end)
        self._calculate_withdrawals(metrics, accounts, period_start, period_end)
        self._calculate_fees(metrics, accounts, period_start, period_end)
        self._calculate_account_stats(metrics, accounts)
        
        return metrics
    
    def _get_filtered_accounts(self, family_id: int, member_id: Optional[int], account_id: Optional[int]) -> list:
        """获取过滤后的账户列表"""
        if account_id:
            # 特定账户
            return Account.query.filter_by(id=account_id, family_id=family_id).all()
        elif member_id:
            # 特定成员的账户
            from app.models.account import AccountMember
            member_accounts = AccountMember.query.filter_by(member_id=member_id).all()
            account_ids = [am.account_id for am in member_accounts]
            return Account.query.filter(Account.id.in_(account_ids), Account.family_id == family_id).all()
        else:
            # 所有家庭账户
            return Account.query.filter_by(family_id=family_id).all()
    
    def _calculate_assets(self, metrics: PortfolioMetrics, accounts: list):
        """计算总资产 - 从持仓计算而不是账户的current_value"""
        # 注意：由于账户模型中的current_value被禁用，我们需要从持仓计算总资产
        # 这个方法会在_calculate_holdings之后调用，所以可以使用已计算的持仓数据
        
        # 先从持仓计算各币种的资产
        cad_assets = Decimal('0')
        usd_assets = Decimal('0')
        
        for holding in metrics.holdings:
            current_value = Decimal(str(holding['current_value']))
            currency = holding.get('currency', 'USD')
            
            if currency == 'CAD' or holding['symbol'].endswith('.TO') or holding['symbol'].endswith('.V'):
                # 加拿大股票
                cad_assets += current_value
            else:
                # 美国股票
                usd_assets += current_value
        
        # 设置总资产
        metrics.total_assets_cad = cad_assets + (usd_assets * self.exchange_rate)
        metrics.total_assets_usd = usd_assets + (cad_assets / self.exchange_rate)
        
        # 存储分币种资产数据
        metrics.cad_assets_only = cad_assets
        metrics.usd_assets_only = usd_assets
        
        # 注意：分币种收益数据会在各自的计算方法中设置
    
    def _calculate_returns(self, metrics: PortfolioMetrics, accounts: list, 
                         start_date: Optional[date], end_date: Optional[date]):
        """计算收益 - 基于持仓数据而不是账户数据"""
        account_ids = [acc.id for acc in accounts]
        
        # 构建查询条件
        query = Transaction.query.filter(Transaction.account_id.in_(account_ids))
        if start_date:
            query = query.filter(Transaction.trade_date >= start_date)
        if end_date:
            query = query.filter(Transaction.trade_date <= end_date)
        
        transactions = query.all()
        
        # 按账户分组计算已实现收益
        cad_realized = Decimal('0')
        usd_realized = Decimal('0')
        
        for account in accounts:
            account_transactions = [t for t in transactions if t.account_id == account.id]
            realized_gain = self._calculate_realized_gain(account_transactions)
            
            if account.currency == 'CAD':
                metrics.realized_gain_cad += realized_gain
                metrics.realized_gain_usd += realized_gain / self.exchange_rate
                cad_realized += realized_gain
            else:
                metrics.realized_gain_usd += realized_gain
                metrics.realized_gain_cad += realized_gain * self.exchange_rate
                usd_realized += realized_gain
        
        # 存储分币种已实现收益
        metrics.cad_realized_only = cad_realized
        metrics.usd_realized_only = usd_realized
        
        # 计算未实现收益 - 从持仓数据计算
        cad_unrealized = Decimal('0')
        usd_unrealized = Decimal('0')
        
        for holding in metrics.holdings:
            unrealized_gain = Decimal(str(holding['unrealized_gain']))
            
            # 根据股票符号判断币种
            if holding['symbol'].endswith('.TO') or holding['symbol'].endswith('.V'):
                # 加拿大股票
                cad_unrealized += unrealized_gain
            else:
                # 美国股票
                usd_unrealized += unrealized_gain
        
        # 设置未实现收益
        metrics.unrealized_gain_cad = cad_unrealized + (usd_unrealized * self.exchange_rate)
        metrics.unrealized_gain_usd = usd_unrealized + (cad_unrealized / self.exchange_rate)
        
        # 存储分币种未实现收益
        metrics.cad_unrealized_only = cad_unrealized
        metrics.usd_unrealized_only = usd_unrealized
        
        # 总收益 = 已实现收益 + 未实现收益
        metrics.total_return_cad = metrics.realized_gain_cad + metrics.unrealized_gain_cad
        metrics.total_return_usd = metrics.realized_gain_usd + metrics.unrealized_gain_usd
        
        # 设置分币种总收益（已实现 + 未实现）
        metrics.cad_return_only = metrics.cad_realized_only + cad_unrealized
        metrics.usd_return_only = metrics.usd_realized_only + usd_unrealized
    
    def _calculate_realized_gain(self, transactions: list) -> Decimal:
        """
        计算已实现收益（简化版FIFO方法）
        
        这是一个简化的实现，实际应用中可能需要更复杂的成本基础计算
        """
        holdings = {}  # {symbol: [(quantity, price, date), ...]}
        realized_gain = Decimal('0')
        
        # 按日期排序交易
        sorted_transactions = sorted(transactions, key=lambda t: t.trade_date)
        
        for tx in sorted_transactions:
            symbol = tx.stock
            
            if tx.type == 'BUY':
                # 买入交易，添加到持仓
                if symbol not in holdings:
                    holdings[symbol] = []
                holdings[symbol].append({
                    'quantity': tx.quantity,
                    'price': tx.price,
                    'fee': tx.fee / tx.quantity if tx.quantity > 0 else Decimal('0')  # 平摊费用
                })
            
            elif tx.type == 'SELL':
                # 卖出交易，计算已实现收益
                if symbol in holdings and holdings[symbol]:
                    remaining_to_sell = tx.quantity
                    sell_price = tx.price
                    sell_fee_per_share = tx.fee / tx.quantity if tx.quantity > 0 else Decimal('0')
                    
                    # FIFO方式出售
                    while remaining_to_sell > 0 and holdings[symbol]:
                        buy_lot = holdings[symbol][0]
                        
                        if buy_lot['quantity'] <= remaining_to_sell:
                            # 完全卖出这个批次
                            sold_quantity = buy_lot['quantity']
                            cost_basis = buy_lot['price'] + buy_lot['fee']
                            net_sell_price = sell_price - sell_fee_per_share
                            
                            realized_gain += (net_sell_price - cost_basis) * sold_quantity
                            
                            remaining_to_sell -= sold_quantity
                            holdings[symbol].pop(0)
                        else:
                            # 部分卖出这个批次
                            sold_quantity = remaining_to_sell
                            cost_basis = buy_lot['price'] + buy_lot['fee']
                            net_sell_price = sell_price - sell_fee_per_share
                            
                            realized_gain += (net_sell_price - cost_basis) * sold_quantity
                            
                            buy_lot['quantity'] -= sold_quantity
                            remaining_to_sell = Decimal('0')
        
        return realized_gain
    
    def _calculate_dividends(self, metrics: PortfolioMetrics, accounts: list,
                           start_date: Optional[date], end_date: Optional[date]):
        """计算股息收入"""
        account_ids = [acc.id for acc in accounts]
        
        # 查询股息交易（这里假设有DIVIDEND类型的交易记录）
        query = db.session.query(
            Transaction.currency,
            func.sum(Transaction.quantity * Transaction.price).label('dividend_amount')
        ).filter(
            Transaction.account_id.in_(account_ids),
            Transaction.type == 'DIVIDEND'
        )
        
        if start_date:
            query = query.filter(Transaction.trade_date >= start_date)
        if end_date:
            query = query.filter(Transaction.trade_date <= end_date)
        
        dividend_results = query.group_by(Transaction.currency).all()
        
        cad_dividends = Decimal('0')
        usd_dividends = Decimal('0')
        
        for currency, dividend_amount in dividend_results:
            dividend_amount = Decimal(str(dividend_amount or 0))
            
            if currency == 'CAD':
                metrics.total_dividends_cad += dividend_amount
                metrics.total_dividends_usd += dividend_amount / self.exchange_rate
                cad_dividends += dividend_amount
            else:  # USD
                metrics.total_dividends_usd += dividend_amount
                metrics.total_dividends_cad += dividend_amount * self.exchange_rate
                usd_dividends += dividend_amount
        
        # 存储分币种分红
        metrics.cad_dividends_only = cad_dividends
        metrics.usd_dividends_only = usd_dividends

    def _calculate_interest(self, metrics: PortfolioMetrics, accounts: list,
                          start_date: Optional[date], end_date: Optional[date]):
        """计算利息收入"""
        account_ids = [acc.id for acc in accounts]
        
        # 查询利息交易
        query = db.session.query(
            Transaction.currency,
            func.sum(Transaction.amount).label('total_interest')
        ).filter(
            Transaction.account_id.in_(account_ids),
            Transaction.type == 'INTEREST'
        )
        
        if start_date:
            query = query.filter(Transaction.trade_date >= start_date)
        if end_date:
            query = query.filter(Transaction.trade_date <= end_date)
        
        query = query.group_by(Transaction.currency)
        interest_results = query.all()
        
        # 分币种统计
        cad_interest = Decimal('0')
        usd_interest = Decimal('0')
        
        for currency, interest_amount in interest_results:
            interest_amount = Decimal(str(interest_amount or 0))
            
            if currency == 'CAD':
                metrics.total_interest_cad += interest_amount
                metrics.total_interest_usd += interest_amount / self.exchange_rate
                cad_interest += interest_amount
            else:  # USD
                metrics.total_interest_usd += interest_amount
                metrics.total_interest_cad += interest_amount * self.exchange_rate
                usd_interest += interest_amount
        
        # 存储分币种利息
        metrics.cad_interest_only = cad_interest
        metrics.usd_interest_only = usd_interest

    def _calculate_deposits(self, metrics: PortfolioMetrics, accounts: list,
                          start_date: Optional[date], end_date: Optional[date]):
        """计算存入金额"""
        account_ids = [acc.id for acc in accounts]
        
        # 查询存入交易
        query = db.session.query(
            Transaction.currency,
            func.sum(Transaction.amount).label('total_deposits')
        ).filter(
            Transaction.account_id.in_(account_ids),
            Transaction.type == 'DEPOSIT'
        )
        
        if start_date:
            query = query.filter(Transaction.trade_date >= start_date)
        if end_date:
            query = query.filter(Transaction.trade_date <= end_date)
        
        query = query.group_by(Transaction.currency)
        deposit_results = query.all()
        
        # 分币种统计
        cad_deposits = Decimal('0')
        usd_deposits = Decimal('0')
        
        for currency, deposit_amount in deposit_results:
            deposit_amount = Decimal(str(deposit_amount or 0))
            
            if currency == 'CAD':
                metrics.total_deposits_cad += deposit_amount
                metrics.total_deposits_usd += deposit_amount / self.exchange_rate
                cad_deposits += deposit_amount
            else:  # USD
                metrics.total_deposits_usd += deposit_amount
                metrics.total_deposits_cad += deposit_amount * self.exchange_rate
                usd_deposits += deposit_amount
        
        # 存储分币种存入
        metrics.cad_deposits_only = cad_deposits
        metrics.usd_deposits_only = usd_deposits

    def _calculate_withdrawals(self, metrics: PortfolioMetrics, accounts: list,
                             start_date: Optional[date], end_date: Optional[date]):
        """计算取出金额"""
        account_ids = [acc.id for acc in accounts]
        
        # 查询取出交易
        query = db.session.query(
            Transaction.currency,
            func.sum(Transaction.amount).label('total_withdrawals')
        ).filter(
            Transaction.account_id.in_(account_ids),
            Transaction.type == 'WITHDRAWAL'
        )
        
        if start_date:
            query = query.filter(Transaction.trade_date >= start_date)
        if end_date:
            query = query.filter(Transaction.trade_date <= end_date)
        
        query = query.group_by(Transaction.currency)
        withdrawal_results = query.all()
        
        # 分币种统计
        cad_withdrawals = Decimal('0')
        usd_withdrawals = Decimal('0')
        
        for currency, withdrawal_amount in withdrawal_results:
            withdrawal_amount = Decimal(str(withdrawal_amount or 0))
            
            if currency == 'CAD':
                metrics.total_withdrawals_cad += withdrawal_amount
                metrics.total_withdrawals_usd += withdrawal_amount / self.exchange_rate
                cad_withdrawals += withdrawal_amount
            else:  # USD
                metrics.total_withdrawals_usd += withdrawal_amount
                metrics.total_withdrawals_cad += withdrawal_amount * self.exchange_rate
                usd_withdrawals += withdrawal_amount
        
        # 存储分币种取出
        metrics.cad_withdrawals_only = cad_withdrawals
        metrics.usd_withdrawals_only = usd_withdrawals
    
    def _calculate_fees(self, metrics: PortfolioMetrics, accounts: list,
                      start_date: Optional[date], end_date: Optional[date]):
        """计算交易费用"""
        account_ids = [acc.id for acc in accounts]
        
        query = db.session.query(
            Transaction.currency,
            func.sum(Transaction.fee).label('total_fees')
        ).filter(Transaction.account_id.in_(account_ids))
        
        if start_date:
            query = query.filter(Transaction.trade_date >= start_date)
        if end_date:
            query = query.filter(Transaction.trade_date <= end_date)
        
        fee_results = query.group_by(Transaction.currency).all()
        
        for currency, total_fees in fee_results:
            total_fees = Decimal(str(total_fees or 0))
            
            if currency == 'CAD':
                metrics.total_fees_cad += total_fees
                metrics.total_fees_usd += total_fees / self.exchange_rate
            else:  # USD
                metrics.total_fees_usd += total_fees
                metrics.total_fees_cad += total_fees * self.exchange_rate
    
    def _calculate_holdings(self, metrics: PortfolioMetrics, accounts: list):
        """计算当前持仓 - 使用统一的PortfolioService"""
        from app.services.portfolio_service import portfolio_service
        
        account_ids = [acc.id for acc in accounts]
        
        # 使用新的统一服务获取投资组合汇总
        portfolio_summary = portfolio_service.get_portfolio_summary(account_ids)
        
        metrics.holdings = portfolio_summary['current_holdings']
        metrics.cleared_holdings = portfolio_summary['cleared_holdings']
    
    def _calculate_account_stats(self, metrics: PortfolioMetrics, accounts: list):
        """计算账户统计"""
        account_stats = {}
        
        for account in accounts:
            current_value = account.current_value or Decimal('0')
            total_cost = account.total_cost or Decimal('0')
            
            stats = {
                'id': account.id,
                'name': account.name,
                'type': account.account_type,
                'currency': account.currency,
                'current_value': float(current_value),
                'total_cost': float(total_cost),
                'unrealized_gain': float(current_value - total_cost),
                'unrealized_gain_percent': float((current_value - total_cost) / total_cost * 100) if total_cost > 0 else 0
            }
            
            account_stats[account.id] = stats
        
        metrics.account_stats = account_stats
    
    def _get_current_stock_price(self, symbol: str, currency: str) -> Optional[Decimal]:
        """获取股票当前价格 - 使用统一的缓存机制"""
        try:
            from app.services.stock_price_service import StockPriceService
            price_service = StockPriceService()
            price = price_service.get_cached_stock_price(symbol, currency)
            return price if price > 0 else None
        except Exception as e:
            print(f"Failed to get stock price for {symbol}: {e}")
            return None
    
    def get_performance_summary(self, family_id: int, periods: list = None) -> Dict:
        """
        获取多时间段性能摘要
        
        Args:
            family_id: 家庭ID
            periods: 要计算的时间段列表
            
        Returns:
            包含多个时间段性能数据的字典
        """
        if periods is None:
            periods = [TimePeriod.YTD, TimePeriod.LAST_30_DAYS, TimePeriod.LAST_365_DAYS, TimePeriod.ALL_TIME]
        
        summary = {}
        
        for period in periods:
            metrics = self.get_portfolio_metrics(family_id, period=period)
            summary[period.value] = metrics.to_dict()
        
        return summary
    
    def get_holdings_by_sector(self, family_id: int) -> Dict[str, list]:
        """
        按行业分组获取持仓
        
        Returns:
            按行业分组的持仓字典
        """
        metrics = self.get_portfolio_metrics(family_id)
        
        sector_holdings = defaultdict(list)
        for holding in metrics.holdings:
            sector = holding.get('sector', 'Unknown')
            sector_holdings[sector].append(holding)
        
        return dict(sector_holdings)
    
    def _get_stock_dividends(self, symbol: str, account_ids: list) -> Decimal:
        """获取股票的分红收入"""
        # 查询DIVIDEND类型的交易记录
        dividend_transactions = Transaction.query.filter(
            Transaction.type == 'DIVIDEND',
            Transaction.stock == symbol,
            Transaction.account_id.in_(account_ids)
        ).all()
        
        total_dividends = Decimal('0')
        for tx in dividend_transactions:
            total_dividends += Decimal(str(tx.amount or 0))
            
        return total_dividends

    def _get_stock_interest(self, symbol: str, account_ids: list) -> Decimal:
        """获取股票的利息收入"""
        # 查询INTEREST类型的交易记录
        interest_transactions = Transaction.query.filter(
            Transaction.type == 'INTEREST',
            Transaction.stock == symbol,
            Transaction.account_id.in_(account_ids)
        ).all()
        
        total_interest = Decimal('0')
        for tx in interest_transactions:
            total_interest += Decimal(str(tx.amount or 0))
            
        return total_interest
    
    def _determine_currency_by_symbol(self, symbol: str) -> str:
        """根据股票代码判断货币"""
        if symbol.endswith('.TO') or symbol.endswith('.V'):
            return 'CAD'
        return 'USD'
    
    def _get_stock_sector(self, stock_info) -> str:
        """获取股票行业信息"""
        if stock_info:
            # 如果有分类信息
            if hasattr(stock_info, 'category') and stock_info.category:
                return stock_info.category.name
            # 可以在这里添加更多的行业判断逻辑
        return 'Unknown'
    
    def clear_cache(self):
        """清空持仓服务缓存（analytics不再使用缓存）"""
        # 同时清除HoldingsService的缓存
        holdings_service.clear_cache()


# 全局服务实例
analytics_service = AnalyticsService()