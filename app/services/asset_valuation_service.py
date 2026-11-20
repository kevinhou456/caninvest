#!/usr/bin/env python3
"""
统一资产估值服务
负责现金余额（今日读 Cash 表、历史按交易倒推）、分红/利息/存取款/预扣税汇总，
以及在核心函数 PortfolioService.get_portfolio_summary 结果之上的组合指标汇总。
本服务不自行计算持仓、成本或收益（ACB 只在核心函数中实现）
"""

from datetime import date
from typing import Dict, Optional, List, Set
from decimal import Decimal, ROUND_HALF_UP
import logging

from app import db
from app.models.transaction import Transaction
from app.models.account import Account
from app.models.cash import Cash
from app.services.stock_price_service import StockPriceService
from app.services.currency_service import CurrencyService
from app.services.stock_history_cache_service import StockHistoryCacheService
from app.models.stocks_cache import StocksCache
from app.models.stock_category import StockCategory

logger = logging.getLogger(__name__)


class AssetValuationService:
    """统一资产估值服务 - 所有资产计算的单一入口"""
    
    def __init__(self, *, auto_refresh_prices: bool = False):
        # auto_refresh_prices 控制是否在读取缓存价格时触发外部刷新
        self.auto_refresh_prices = auto_refresh_prices
        self.stock_price_service = StockPriceService()
        self.currency_service = CurrencyService()
        self.history_cache_service = StockHistoryCacheService()
        
    
    def get_detailed_portfolio_data(self, account_ids: List[int], target_date: Optional[date] = None) -> Dict:
        """
        获取详细的投资组合数据，包括持仓和清仓股票列表
        使用统一的AssetValuationService计算逻辑，确保与汇总数据一致

        Args:
            account_ids: 账户ID列表
            target_date: 目标日期，None表示今天

        Returns:
            包含current_holdings和cleared_holdings的字典
        """
        if target_date is None:
            target_date = date.today()

        return self._get_unified_portfolio_data(account_ids, target_date)

    def _get_unified_portfolio_data(self, account_ids: List[int], target_date: date) -> Dict:
        """
        使用统一的AssetValuationService计算逻辑获取详细投资组合数据
        确保与get_comprehensive_portfolio_metrics的计算结果一致

        修复多账户清仓逻辑：只有在所有账户中都清仓的股票才进入cleared_holdings
        """
        current_holdings = []
        cleared_holdings = []

        # 获取汇率
        exchange_rate = self.currency_service.get_current_rate('USD', 'CAD')

        # 第一步：收集所有账户的股票数据，按股票符号分组
        all_stock_data = {}  # {symbol: {account_id: stock_data, ...}, ...}

        for account_id in account_ids:
            account = Account.query.get(account_id)
            if not account:
                continue

            # 获取该账户的所有股票交易
            all_symbols = Transaction.query.filter(
                Transaction.account_id == account_id,
                Transaction.stock.isnot(None),
                Transaction.stock != '',
                Transaction.trade_date <= target_date
            ).with_entities(Transaction.stock).distinct().all()

            # 持仓、成本、收益、价格统一来自核心函数
            summary = self._core_portfolio_summary([account_id], target_date)
            core_positions = {h['symbol']: h for h in summary['current_holdings'] + summary['cleared_holdings']}

            for (symbol,) in all_symbols:
                if not symbol:
                    continue

                # 只有分红/利息、没有买卖记录的股票，核心函数里没有持仓条目
                position = core_positions.get(symbol)
                currency = position['currency'] if position else Transaction.get_currency_by_stock_symbol(symbol)
                current_shares = position['current_shares'] if position else 0.0

                # 计算分红利息
                dividend_interest = self._calculate_stock_dividend_interest(
                    account_id=account_id,
                    symbol=symbol,
                    target_date=target_date,
                    currency=currency,
                    exchange_rate=exchange_rate
                )

                # 初始化股票数据结构
                if symbol not in all_stock_data:
                    all_stock_data[symbol] = {
                        'currency': currency,
                        'accounts': {}
                    }

                # 存储每个账户的数据
                all_stock_data[symbol]['accounts'][account_id] = {
                    'account': account,
                    'position': position,
                    'current_shares': current_shares,
                    'bought_total': position['total_bought_value'] if position else 0.0,
                    'sold_total': position['total_sold_value'] if position else 0.0,
                    'total_bought_shares': position['total_bought_shares'] if position else 0.0,
                    'total_sold_shares': position['total_sold_shares'] if position else 0.0,
                    'realized_gain': position['realized_gain'] if position else 0.0,
                    'dividend_interest': dividend_interest
                }

        # 第二步：根据所有账户的持仓状态决定每只股票的分类
        for symbol, stock_info in all_stock_data.items():
            currency = stock_info['currency']
            accounts_data = stock_info['accounts']

            # 检查是否有任何账户还持有这只股票
            has_current_holdings = any(acc_data['current_shares'] > 0 for acc_data in accounts_data.values())

            if has_current_holdings:
                # 有持仓：为每个有持仓的账户创建持仓记录
                for account_id, acc_data in accounts_data.items():
                    if acc_data['current_shares'] > 0:
                        account = acc_data['account']
                        position = acc_data['position']
                        current_shares = acc_data['current_shares']
                        dividend_interest = acc_data['dividend_interest']
                        total_bought_shares = acc_data['total_bought_shares']
                        total_sold_shares = acc_data['total_sold_shares']

                        # 当前持仓 - 市值（核心函数的价格与市值）
                        current_price = position['current_price']
                        current_value = position['current_value']
                        current_value_cad = current_value * float(exchange_rate) if currency == 'USD' else current_value

                        # 成本：原始币种为核心函数的剩余成本；CAD 口径按当前汇率换算（与原显示口径一致）
                        total_cost_native = position['total_cost']
                        total_cost_cad = total_cost_native * float(exchange_rate) if currency == 'USD' else total_cost_native

                        # average_cost_cad 供内部计算，average_cost_native 用于展示
                        average_cost_cad = (total_cost_cad / float(current_shares)
                                            if current_shares > 0 else 0)
                        average_cost_native = (total_cost_native / float(current_shares)
                                               if current_shares > 0 else 0)

                        # 计算收益 (统一使用CAD进行计算，避免币种不匹配)
                        unrealized_gain_cad = current_value_cad - total_cost_cad
                        # USD股票的收益转换回USD显示
                        unrealized_gain = unrealized_gain_cad / float(exchange_rate) if currency == 'USD' else unrealized_gain_cad

                        # 获取股票信息（公司名和分类）
                        stock_cache = StocksCache.query.filter_by(symbol=symbol, currency=currency).first()
                        if stock_cache:
                            company_name = stock_cache.name or symbol
                            sector = 'Unknown'
                            if stock_cache.category_id and stock_cache.category:
                                sector = stock_cache.category.name or stock_cache.category.name_en or 'Unknown'
                        else:
                            company_name = symbol
                            sector = 'Unknown'

                        # 计算Daily Change
                        daily_change_value = 0
                        daily_change_percent = 0
                        from app.models.market_holiday import MarketHoliday as _MH
                        _market = self.history_cache_service._get_market(symbol, currency)
                        _is_today_holiday = (
                            target_date >= date.today() and
                            (target_date.weekday() >= 5 or _MH.is_holiday(target_date, _market))
                        )
                        if not _is_today_holiday:
                            daily_change_value = position['daily_change_value']
                            daily_change_percent = position['daily_change_percent']

                        percent_base_cost = total_cost_native if currency == 'USD' else total_cost_cad

                        holding_data = {
                            'symbol': symbol,
                            'account_id': account_id,
                            'account_name': self._get_account_name_with_members(account),
                            'currency': currency,
                            'shares': float(current_shares),
                            'total_bought_shares': float(total_bought_shares),
                            'total_sold_shares': float(total_sold_shares),
                            'total_bought_value': float(acc_data['bought_total']),
                            'total_sold_value': float(acc_data['sold_total']),
                            'average_cost': average_cost_native,
                            'average_cost_cad': average_cost_cad,
                            'total_cost': total_cost_cad,
                            'total_cost_cad': total_cost_cad,
                            'total_cost_native': total_cost_native,
                            'current_price': float(current_price),
                            'current_value': current_value,
                            'current_value_cad': current_value_cad,
                            'unrealized_gain': unrealized_gain,
                            'unrealized_gain_cad': unrealized_gain_cad,
                            'unrealized_gain_percent': float((unrealized_gain / percent_base_cost * 100)) if percent_base_cost > 0 else 0,
                            'realized_gain': acc_data['realized_gain'],
                            'realized_gain_cad': acc_data['realized_gain'] * float(exchange_rate) if currency == 'USD' else acc_data['realized_gain'],
                            'withholding_tax': dividend_interest['withholding_tax'],
                            'net_dividends': dividend_interest['net_dividends'],
                            'dividends': dividend_interest['dividend_received'],
                            'interest': dividend_interest['interest_received'],
                            'dividend_received': dividend_interest['dividend_received'],
                            'interest_received': dividend_interest['interest_received'],
                            'dividend_received_cad': dividend_interest['dividend_received_cad'],
                            'interest_received_cad': dividend_interest['interest_received_cad'],
                            'average_cost_display': average_cost_native,
                            'company_name': company_name,
                            'sector': sector,
                            'daily_change_value': daily_change_value,
                            'daily_change_percent': daily_change_percent
                        }
                        current_holdings.append(holding_data)

            else:
                # 完全清仓：合并所有账户的数据创建一条清仓记录
                total_bought_shares = sum(acc_data['total_bought_shares'] for acc_data in accounts_data.values())
                total_sold_shares = sum(acc_data['total_sold_shares'] for acc_data in accounts_data.values())
                total_bought_value = sum(acc_data['bought_total'] for acc_data in accounts_data.values())
                total_sold_value = sum(acc_data['sold_total'] for acc_data in accounts_data.values())
                total_realized_gain = sum(acc_data['realized_gain'] for acc_data in accounts_data.values())

                # 合并分红利息
                total_dividends = sum(acc_data['dividend_interest']['dividend_received'] for acc_data in accounts_data.values())
                total_interest = sum(acc_data['dividend_interest']['interest_received'] for acc_data in accounts_data.values())

                # 选择一个账户作为代表账户（用于显示账户名称）
                representative_account = next(iter(accounts_data.values()))['account']

                # 获取股票信息（公司名和分类）
                stock_cache = StocksCache.query.filter_by(symbol=symbol, currency=currency).first()
                if stock_cache:
                    company_name = stock_cache.name or symbol
                    sector = 'Unknown'
                    if stock_cache.category_id and stock_cache.category:
                        sector = stock_cache.category.name or stock_cache.category.name_en or 'Unknown'
                else:
                    company_name = symbol
                    sector = 'Unknown'

                cleared_data = {
                    'symbol': symbol,
                    'account_id': representative_account.id,  # 使用代表账户
                    'account_name': f"合计 ({len(accounts_data)}个账户)" if len(accounts_data) > 1 else self._get_account_name_with_members(representative_account),
                    'currency': currency,
                    'total_bought_shares': total_bought_shares,
                    'total_sold_shares': total_sold_shares,
                    'total_bought_value': total_bought_value,
                    'total_sold_value': total_sold_value,
                    'realized_gain': total_realized_gain,
                    'realized_gain_percent': float((total_realized_gain / total_bought_value * 100)) if total_bought_value > 0 else 0,
                    'withholding_tax': sum(a['dividend_interest']['withholding_tax'] for a in accounts_data.values()),
                    'net_dividends': sum(a['dividend_interest']['net_dividends'] for a in accounts_data.values()),
                    'dividends': total_dividends,
                    'interest': total_interest,
                    'dividend_received': total_dividends,
                    'interest_received': total_interest,
                    'company_name': company_name,
                    'sector': sector
                }
                cleared_holdings.append(cleared_data)

        return {
            'current_holdings': current_holdings,
            'cleared_holdings': cleared_holdings
        }

    def _calculate_stock_dividend_interest(self, account_id: int, symbol: str, target_date: date, currency: str, exchange_rate: float) -> Dict:
        """
        计算单个股票的分红和利息 - 统一方法，避免重复代码

        Returns:
            Dict包含dividend_received, interest_received, dividend_received_cad, interest_received_cad
        """
        # 查询分红交易
        dividend_transactions = Transaction.query.filter(
            Transaction.account_id == account_id,
            Transaction.stock == symbol,
            Transaction.type == 'DIVIDEND',
            Transaction.trade_date <= target_date
        ).all()

        # 查询利息交易
        interest_transactions = Transaction.query.filter(
            Transaction.account_id == account_id,
            Transaction.stock == symbol,
            Transaction.type == 'INTEREST',
            Transaction.trade_date <= target_date
        ).all()

        # 计算总额
        dividend_received = sum(float(tx.amount or 0) for tx in dividend_transactions)
        interest_received = sum(float(tx.amount or 0) for tx in interest_transactions)

        # 股息/利息不应为负；如遇负值（录入错误或费用），按0处理保持口径一致
        dividend_received = max(dividend_received, 0.0)
        interest_received = max(interest_received, 0.0)

        # 转换为CAD等价
        dividend_received_cad = dividend_received * float(exchange_rate) if currency == 'USD' else dividend_received
        interest_received_cad = interest_received * float(exchange_rate) if currency == 'USD' else interest_received

        tax = self.get_withholding_tax_totals([account_id], target_date, symbol=symbol)
        native_tax = float(tax[currency.lower()])
        return {
            'withholding_tax': native_tax,
            'net_dividends': dividend_received - native_tax,
            'dividend_received': dividend_received,
            'interest_received': interest_received,
            'dividend_received_cad': dividend_received_cad,
            'interest_received_cad': interest_received_cad
        }


    def get_comprehensive_portfolio_metrics(self,
                                           account_ids: List[int],
                                           target_date: Optional[date] = None,
                                           ownership_map: Optional[Dict[int, Decimal]] = None) -> Dict:
        """
        获取完整的投资组合指标，包括正确的总回报计算
        
        返回统计指标:
        - 总资产 = 股票市值 + 现金
        - 总回报 = 已实现盈亏 + 未实现盈亏 + 股息 + 利息
        - 已实现盈亏：通过FIFO计算所有已完成买卖的盈亏
        - 未实现盈亏：当前持仓的市值 - 成本基础  
        - 股息利息：所有DIVIDEND和INTEREST交易的总和
        """
        if target_date is None:
            target_date = date.today()
        
        logger.info(f"计算投资组合综合指标，账户数量: {len(account_ids)}, 日期: {target_date}")
        
        # 初始化统计数据 - 分CAD和USD
        stock_value_cad = Decimal('0')
        stock_value_usd = Decimal('0')
        cash_cad = Decimal('0')
        cash_usd = Decimal('0')
        realized_gain_cad = Decimal('0')
        realized_gain_usd = Decimal('0')
        unrealized_gain_cad = Decimal('0')
        unrealized_gain_usd = Decimal('0')
        dividends_cad = Decimal('0')
        dividends_usd = Decimal('0')
        interest_cad = Decimal('0')
        interest_usd = Decimal('0')
        deposits_cad = Decimal('0')
        deposits_usd = Decimal('0')
        withdrawals_cad = Decimal('0')
        withdrawals_usd = Decimal('0')
        
        # 获取汇率
        exchange_rate = self.currency_service.get_current_rate('USD', 'CAD')
        exchange_rate_decimal = Decimal(str(exchange_rate))
        
        # 使用AssetValuationService自己的计算逻辑确保数据一致性
        total_stock_value_cad = Decimal('0')
        total_unrealized_gain_cad = Decimal('0')
        total_realized_gain = Decimal('0')

        # 按币种分别计算
        stock_value_cad = Decimal('0')
        stock_value_usd = Decimal('0')
        realized_gain_cad = Decimal('0')
        realized_gain_usd = Decimal('0')
        unrealized_gain_cad = Decimal('0')
        unrealized_gain_usd = Decimal('0')

        # 按账户计算所有数据
        for account_id in account_ids:
            proportion = self._get_account_proportion(account_id, ownership_map)
            if proportion <= 0:
                continue

            # 累计现金余额
            cash_balance_cad, cash_balance_usd = self._calculate_cash_balance(account_id, target_date)
            cash_cad += cash_balance_cad * proportion
            cash_usd += cash_balance_usd * proportion

            # 按币种分别计算股票和收益
            account_stock_cad, account_stock_usd, account_realized_cad, account_realized_usd, account_unrealized_cad, account_unrealized_usd = self._calculate_account_metrics_by_currency(account_id, target_date)

            stock_value_cad += account_stock_cad * proportion
            stock_value_usd += account_stock_usd * proportion
            realized_gain_cad += account_realized_cad * proportion
            realized_gain_usd += account_realized_usd * proportion
            unrealized_gain_cad += account_unrealized_cad * proportion
            unrealized_gain_usd += account_unrealized_usd * proportion
            # 计算股息、利息、存款和取款
            dividend_interest_stats = self._calculate_dividend_interest_by_currency(account_id, target_date)
            dividends_cad += dividend_interest_stats['dividends_cad'] * proportion
            dividends_usd += dividend_interest_stats['dividends_usd'] * proportion
            interest_cad += dividend_interest_stats['interest_cad'] * proportion
            interest_usd += dividend_interest_stats['interest_usd'] * proportion

            deposits_withdrawals = self._calculate_deposits_withdrawals_by_currency(account_id, target_date)
            deposits_cad += deposits_withdrawals['deposits_cad'] * proportion
            deposits_usd += deposits_withdrawals['deposits_usd'] * proportion
            withdrawals_cad += deposits_withdrawals['withdrawals_cad'] * proportion
            withdrawals_usd += deposits_withdrawals['withdrawals_usd'] * proportion

        # 计算总和（CAD等价）
        total_stock_value = stock_value_cad + stock_value_usd * exchange_rate_decimal
        total_cash_value = cash_cad + cash_usd * exchange_rate_decimal
        total_realized_gain = realized_gain_cad + realized_gain_usd * exchange_rate_decimal
        total_unrealized_gain = unrealized_gain_cad + unrealized_gain_usd * exchange_rate_decimal
        total_dividends = dividends_cad + dividends_usd * exchange_rate_decimal
        total_interest = interest_cad + interest_usd * exchange_rate_decimal
        total_deposits = deposits_cad + deposits_usd * exchange_rate_decimal
        total_withdrawals = withdrawals_cad + withdrawals_usd * exchange_rate_decimal

        # 计算总值
        total_assets = total_stock_value + total_cash_value
        tax = self.get_withholding_tax_totals(account_ids, target_date, ownership_map=ownership_map)
        total_return = total_realized_gain + total_unrealized_gain + total_dividends + total_interest - tax['total_cad']

        # 计算总回报率
        total_return_rate = float(total_return / total_deposits * 100) if total_deposits > 0 else 0.0
        
        # 计算当日变化
        daily_change = self._calculate_daily_change(account_ids, target_date, ownership_map)

        return {
            'total_assets': {
                'cad': float(total_assets),
                'cad_only': float(stock_value_cad + cash_cad),  # CAD股票+CAD现金
                'usd_only': float(stock_value_usd + cash_usd),  # USD股票+USD现金
                'stock_value': float(total_stock_value),
                'stock_value_cad': float(stock_value_cad),  # CAD股票市值
                'stock_value_usd': float(stock_value_usd),  # USD股票市值
                'cash_cad': float(cash_cad),
                'cash_usd': float(cash_usd)
            },
            'total_return': {
                'cad': float(total_return),
                'cad_only': float(realized_gain_cad + unrealized_gain_cad + dividends_cad + interest_cad - tax['cad']),  # CAD币种总回报
                'usd_only': float(realized_gain_usd + unrealized_gain_usd + dividends_usd + interest_usd - tax['usd']),  # USD币种总回报
                'realized_gain': float(total_realized_gain),
                'unrealized_gain': float(total_unrealized_gain),
                'dividends': float(total_dividends),
                'interest': float(total_interest)
            },
            'realized_gain': {
                'cad': float(total_realized_gain),
                'cad_only': float(realized_gain_cad),
                'usd_only': float(realized_gain_usd)
            },
            'unrealized_gain': {
                'cad': float(total_unrealized_gain),
                'cad_only': float(unrealized_gain_cad),
                'usd_only': float(unrealized_gain_usd)
            },
            'withholding_tax': {'cad': float(tax['total_cad']), 'cad_only': float(tax['cad']), 'usd_only': float(tax['usd'])},
            'net_dividends': {'cad': float(total_dividends - tax['total_cad']),
                              'cad_only': float(dividends_cad - tax['cad']),
                              'usd_only': float(dividends_usd - tax['usd'])},
            'dividends': {
                'cad': float(total_dividends),
                'cad_only': float(dividends_cad),
                'usd_only': float(dividends_usd)
            },
            'interest': {
                'cad': float(total_interest),
                'cad_only': float(interest_cad),
                'usd_only': float(interest_usd)
            },
            'total_deposits': {
                'cad': float(total_deposits),
                'cad_only': float(deposits_cad),
                'usd_only': float(deposits_usd)
            },
            'total_withdrawals': {
                'cad': float(total_withdrawals),
                'cad_only': float(withdrawals_cad),
                'usd_only': float(withdrawals_usd)
            },
            'stock_value': {
                'cad': float(total_stock_value),
                'total_cad': float(total_stock_value)
            },
            'cash_balance': {
                'cad': float(cash_cad),
                'usd': float(cash_usd),
                'total_cad': float(total_cash_value)
            },
            'daily_change': {
                'cad': float(daily_change['cad']),
                'cad_only': float(daily_change['cad_only']),
                'usd_only': float(daily_change['usd_only'])
            },
            'exchange_rate': float(exchange_rate_decimal),
            'total_return_rate': total_return_rate
        }

    def _calculate_daily_change(self, account_ids: List[int], target_date: date,
                                ownership_map: Optional[Dict[int, Decimal]] = None) -> Dict:
        """计算当日变化 - 汇总核心函数中每只持仓的 daily_change_value（按出资比例换算）"""
        try:
            # 节假日/周末不计算当日变化
            from app.models.market_holiday import MarketHoliday as _MH
            if target_date >= date.today() and (
                target_date.weekday() >= 5 or
                _MH.is_holiday(target_date, 'US') or
                _MH.is_holiday(target_date, 'CA')
            ):
                return {'cad': 0.0, 'cad_only': 0.0, 'usd_only': 0.0}

            exchange_rate_decimal = Decimal(str(self.currency_service.get_current_rate('USD', 'CAD')))
            change = {'CAD': Decimal('0'), 'USD': Decimal('0')}
            for account_id in account_ids:
                proportion = self._get_account_proportion(account_id, ownership_map)
                if proportion <= 0:
                    continue
                summary = self._core_portfolio_summary([account_id], target_date)
                for holding in summary.get('current_holdings', []):
                    bucket = 'CAD' if holding.get('currency') == 'CAD' else 'USD'
                    change[bucket] += Decimal(str(holding.get('daily_change_value', 0) or 0)) * proportion

            return {
                'cad': float(change['CAD'] + change['USD'] * exchange_rate_decimal),
                'cad_only': float(change['CAD']),
                'usd_only': float(change['USD'])
            }

        except Exception as e:
            logger.error(f"计算当日变化失败: {e}")
            return {
                'cad': 0.0,
                'cad_only': 0.0,
                'usd_only': 0.0
            }


    def _get_account_proportion(self, account_id: int,
                                ownership_map: Optional[Dict[int, Decimal]]) -> Decimal:
        if not ownership_map:
            return Decimal('1')
        return ownership_map.get(account_id, Decimal('0'))


    def get_income_totals(self, account_ids, target_date, ownership_map=None):
        """Cumulative gross income and net income, using the shared income/tax calculators."""
        dividends = {'cad': Decimal('0'), 'usd': Decimal('0')}
        interest = {'cad': Decimal('0'), 'usd': Decimal('0')}
        for account_id in account_ids:
            proportion = self._get_account_proportion(account_id, ownership_map)
            if proportion <= 0:
                continue
            income = self._calculate_dividend_interest_by_currency(account_id, target_date)
            for currency in dividends:
                dividends[currency] += income['dividends_' + currency] * proportion
                interest[currency] += income['interest_' + currency] * proportion
        tax = self.get_withholding_tax_totals(account_ids, target_date, ownership_map=ownership_map)
        rate = Decimal(str(self.currency_service.get_current_rate('USD', 'CAD')))
        gross = dividends['cad'] + dividends['usd'] * rate
        return {'gross_dividends': float(gross), 'withholding_tax': float(tax['total_cad']),
                'net_dividends': float(gross - tax['total_cad']),
                'net_income': float(gross + interest['cad'] + interest['usd'] * rate - tax['total_cad'])}

    def get_withholding_tax_totals(self, account_ids, target_date, start_date=None,
                                   ownership_map=None, symbol=None):
        """Signed withheld tax: positive deductions, negative refunds, never external flows."""
        query = Transaction.query.filter(Transaction.account_id.in_(account_ids),
                                         Transaction.type == 'TAX', Transaction.trade_date <= target_date)
        if start_date is not None:
            query = query.filter(Transaction.trade_date >= start_date)
        if symbol is not None:
            query = query.filter(Transaction.stock == symbol)
        totals = {'cad': Decimal('0'), 'usd': Decimal('0')}
        for tx in query.all():
            proportion = self._get_account_proportion(tx.account_id, ownership_map)
            totals[(tx.currency or 'USD').lower()] += Decimal(str(tx.amount or 0)) * proportion
        rate = Decimal(str(self.currency_service.get_current_rate('USD', 'CAD')))
        return dict(totals, total_cad=totals['cad'] + totals['usd'] * rate)

    def _calculate_dividend_interest_by_currency(self, account_id: int, target_date: date) -> Dict:
        """
        按币种计算股息和利息
        """
        stats = {
            'dividends_cad': Decimal('0'),
            'dividends_usd': Decimal('0'),
            'interest_cad': Decimal('0'),
            'interest_usd': Decimal('0')
        }
        
        # 获取股息和利息交易
        transactions = Transaction.query.filter(
            Transaction.account_id == account_id,
            Transaction.trade_date <= target_date,
            Transaction.type.in_(['DIVIDEND', 'INTEREST'])
        ).all()
        
        for tx in transactions:
            amount = Decimal(str(tx.amount or 0))
            currency = tx.currency or 'USD'  # 默认USD
            
            if tx.type == 'DIVIDEND':
                if currency == 'CAD':
                    stats['dividends_cad'] += amount
                else:
                    stats['dividends_usd'] += amount
            elif tx.type == 'INTEREST':
                if currency == 'CAD':
                    stats['interest_cad'] += amount
                else:
                    stats['interest_usd'] += amount

        # 遇到录入为负的股息/利息时，按0处理以免出现负的Div+Int
        for key, value in list(stats.items()):
            if value < 0:
                stats[key] = Decimal('0')
        
        return stats
    
    def _calculate_deposits_withdrawals_by_currency(self, account_id: int, target_date: date) -> Dict:
        """
        按币种计算存款和取款
        """
        stats = {
            'deposits_cad': Decimal('0'),
            'deposits_usd': Decimal('0'),
            'withdrawals_cad': Decimal('0'),
            'withdrawals_usd': Decimal('0')
        }
        
        # 获取存款和取款交易
        transactions = Transaction.query.filter(
            Transaction.account_id == account_id,
            Transaction.trade_date <= target_date,
            Transaction.type.in_(['DEPOSIT', 'WITHDRAWAL'])
        ).all()
        
        for tx in transactions:
            # 优先使用amount字段，如果没有则使用quantity*price
            if tx.amount:
                amount = Decimal(str(tx.amount))
            else:
                amount = Decimal(str(tx.quantity or 0)) * Decimal(str(tx.price or 0))
            
            currency = tx.currency or 'USD'  # 默认USD
            
            if tx.type == 'DEPOSIT':
                if currency == 'CAD':
                    stats['deposits_cad'] += amount
                else:
                    stats['deposits_usd'] += amount
            elif tx.type == 'WITHDRAWAL':
                if currency == 'CAD':
                    stats['withdrawals_cad'] += amount
                else:
                    stats['withdrawals_usd'] += amount
        
        return stats


    def get_cash_balance(self, account_id: int, target_date: Optional[date] = None) -> Dict[str, Decimal]:
        """获取现金余额（仅现金，不触发股票估值）"""
        if target_date is None:
            target_date = date.today()
        return self.get_cash_balances(account_id, [target_date])[target_date]

    def get_cash_balances(self, account_id: int, target_dates: List[date]) -> Dict[date, Dict[str, Decimal]]:
        """一次计算多个日期的现金余额：{日期: {'cad', 'usd', 'total_cad'}}。
        每个日期的结果与 get_cash_balance(account_id, 日期) 完全相同。"""
        balances = self._calculate_cash_balances(account_id, list(target_dates))
        usd_to_cad = self.currency_service.get_current_rate('USD', 'CAD')
        usd_to_cad_decimal = (
            Decimal(str(usd_to_cad))
            if not isinstance(usd_to_cad, Decimal)
            else usd_to_cad
        )
        return {
            d: {
                'cad': cad_balance,
                'usd': usd_balance,
                'total_cad': cad_balance + (usd_balance * usd_to_cad_decimal)
            }
            for d, (cad_balance, usd_balance) in balances.items()
        }


    def _calculate_cash_balance(self, account_id: int, target_date: date) -> tuple[Decimal, Decimal]:
        """
        计算指定日期的现金余额
        - 今天或未来日期：使用Cash表的当前数据
        - 历史日期：通过交易记录倒推计算

        Returns:
            (CAD余额, USD余额)
        """
        return self._calculate_cash_balances(account_id, [target_date])[target_date]

    def _calculate_cash_balances(self, account_id: int, target_dates: List[date]) -> Dict[date, tuple]:
        """现金余额计算的唯一实现：{日期: (CAD余额, USD余额)}。
        - 今天或未来日期：使用Cash表的当前数据（没有记录则为 0）
        - 历史日期：从Cash表当前数据出发，按时间倒序倒推交易；多个日期共用一次倒推
        同一请求内相同 (账户, 日期) 的结果会被记住（有写入时自动失效）。
        """
        from app.utils.query_memo import memo, peek
        result: Dict[date, tuple] = {}
        missing = []
        for d in target_dates:
            cached = peek('cash_balance', (account_id, d))
            if cached is None:
                missing.append(d)
            else:
                result[d] = cached
        if missing:
            for d, value in self._compute_cash_balances(account_id, missing).items():
                memo('cash_balance', (account_id, d), lambda value=value: value)
                result[d] = value
        return result

    def _compute_cash_balances(self, account_id: int, target_dates: List[date]) -> Dict[date, tuple]:
        today = date.today()
        cash_record = Cash.get_account_cash(account_id)
        if cash_record:
            current_cad = Decimal(str(cash_record.cad or 0))
            current_usd = Decimal(str(cash_record.usd or 0))
        else:
            current_cad = Decimal('0')
            current_usd = Decimal('0')

        result: Dict[date, tuple] = {}
        historical = sorted({d for d in target_dates if d < today}, reverse=True)
        for d in target_dates:
            if d >= today:
                result[d] = (current_cad, current_usd)
        if not historical:
            return result

        # 最早日期之后的全部现金相关交易，按时间倒序（从最近开始倒推）
        transactions = Transaction.query.filter(
            Transaction.account_id == account_id,
            Transaction.trade_date > historical[-1],
            Transaction.type.in_(['DEPOSIT', 'WITHDRAW', 'WITHDRAWAL', 'BUY', 'SELL', 'DIVIDEND', 'INTEREST', 'FEE', 'TAX'])
        ).order_by(Transaction.trade_date.desc(), Transaction.id.desc()).all()

        cad_balance = current_cad
        usd_balance = current_usd
        applied = 0
        # 日期从近到远：每个日期只需在上一个日期的基础上继续倒推“该日期之后”的交易
        for d in historical:
            while applied < len(transactions) and transactions[applied].trade_date > d:
                cad_balance, usd_balance = self._reverse_transaction_impact(
                    cad_balance, usd_balance, transactions[applied]
                )
                applied += 1
            result[d] = (cad_balance, usd_balance)
        return result


    def _reverse_transaction_impact(self, cad: Decimal, usd: Decimal, transaction: Transaction) -> tuple[Decimal, Decimal]:
        """Reverse the same cash deltas used by forward processing."""
        cad_delta, usd_delta = self._apply_transaction_impact(Decimal('0'), Decimal('0'), transaction)
        return cad - cad_delta, usd - usd_delta


    def _core_portfolio_summary(self, account_ids: List[int], target_date: date) -> Dict:
        """持仓、成本、收益统一来自核心函数 PortfolioService.get_portfolio_summary。"""
        from app.services.portfolio_service import PortfolioService, TimePeriod
        if getattr(self, '_portfolio_service', None) is None:
            self._portfolio_service = PortfolioService(auto_refresh_prices=self.auto_refresh_prices)
        return self._portfolio_service.get_portfolio_summary(account_ids, TimePeriod.CUSTOM, end_date=target_date)

    def _calculate_account_metrics_by_currency(self, account_id: int, target_date: date) -> tuple[Decimal, Decimal, Decimal, Decimal, Decimal, Decimal]:
        """按币种分别计算账户的股票市值、已实现收益、浮动盈亏（数据来自核心函数）

        Returns:
            (stock_value_cad, stock_value_usd, realized_gain_cad, realized_gain_usd, unrealized_gain_cad, unrealized_gain_usd)
        """
        stock_value = {'CAD': Decimal('0'), 'USD': Decimal('0')}
        realized = {'CAD': Decimal('0'), 'USD': Decimal('0')}
        unrealized = {'CAD': Decimal('0'), 'USD': Decimal('0')}

        summary = self._core_portfolio_summary([account_id], target_date)
        for holding in summary.get('current_holdings', []):
            bucket = 'CAD' if holding.get('currency') == 'CAD' else 'USD'
            stock_value[bucket] += Decimal(str(holding['current_value']))
            unrealized[bucket] += Decimal(str(holding['unrealized_gain']))
            realized[bucket] += Decimal(str(holding['realized_gain']))
        for holding in summary.get('cleared_holdings', []):
            bucket = 'CAD' if holding.get('currency') == 'CAD' else 'USD'
            realized[bucket] += Decimal(str(holding['realized_gain']))

        return (stock_value['CAD'], stock_value['USD'], realized['CAD'], realized['USD'],
                unrealized['CAD'], unrealized['USD'])
    
    def _apply_transaction_impact(self, cad: Decimal, usd: Decimal, transaction: Transaction) -> tuple[Decimal, Decimal]:
        """应用交易对现金的影响"""
        currency = transaction.currency or 'USD'
        
        if transaction.type == 'DEPOSIT':
            # 存入：增加现金
            amount = Decimal(str(transaction.amount or 0))
            if currency == 'CAD':
                cad += amount
            else:
                usd += amount
                
        elif transaction.type in ['WITHDRAW', 'WITHDRAWAL', 'FEE', 'TAX']:
            # 取出：减少现金
            amount = Decimal(str(transaction.amount or 0))
            if currency == 'CAD':
                cad -= amount
            else:
                usd -= amount
                
        elif transaction.type == 'BUY':
            # 买入：减少现金（交易总额 + 手续费）
            # 确保所有值都是Decimal类型以避免类型错误
            quantity = Decimal(str(transaction.quantity or 0))
            price = Decimal(str(transaction.price or 0))
            total_cost = quantity * price + Decimal(str(transaction.fee or 0))
            if currency == 'CAD':
                cad -= total_cost
            else:
                usd -= total_cost
                
        elif transaction.type == 'SELL':
            # 卖出：增加现金（交易总额 - 手续费）
            # 确保所有值都是Decimal类型以避免类型错误
            quantity = Decimal(str(transaction.quantity or 0))
            price = Decimal(str(transaction.price or 0))
            net_proceeds = quantity * price - Decimal(str(transaction.fee or 0))
            if currency == 'CAD':
                cad += net_proceeds
            else:
                usd += net_proceeds
                
        elif transaction.type == 'DIVIDEND':
            # 分红：增加现金
            amount = Decimal(str(transaction.amount or 0))
            if currency == 'CAD':
                cad += amount
            else:
                usd += amount
                
        elif transaction.type == 'INTEREST':
            # 利息：增加现金
            amount = Decimal(str(transaction.amount or 0))
            if currency == 'CAD':
                cad += amount
            else:
                usd += amount
        
        return cad, usd


    def _get_account_name_with_members(self, account):
        """
        获取带有成员信息的账户名称
        格式: "账户名称 - 所有者"
        """
        if not account:
            return ""
        
        display_text = account.name
        
        # 为所有账户显示成员信息，不仅仅是联名账户
        if account.account_members:
            member_names = [am.member.name for am in account.account_members]
            display_text += f" - {', '.join(member_names)}"
        
        return display_text
