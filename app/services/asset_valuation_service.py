#!/usr/bin/env python3
"""
统一资产估值服务
负责计算账户在任意时间点的总资产、股票市值、现金余额
确保数据准确性和一致性
"""

from datetime import datetime, date, timedelta
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


class AssetSnapshot:
    """资产快照数据类"""
    def __init__(self, date: date, account_id: int):
        self.date = date
        self.account_id = account_id
        self.total_assets = Decimal('0')
        self.stock_market_value = Decimal('0')
        self.cash_balance_cad = Decimal('0')
        self.cash_balance_usd = Decimal('0')
        self.cash_balance_total_cad = Decimal('0')
        self.holdings = {}  # {symbol: {shares, market_value, currency}}
        
    def to_dict(self):
        """转换为字典格式"""
        return {
            'date': self.date.isoformat(),
            'account_id': self.account_id,
            'total_assets': float(self.total_assets),
            'stock_market_value': float(self.stock_market_value),
            'cash_balance': {
                'cad': float(self.cash_balance_cad),
                'usd': float(self.cash_balance_usd),
                'total_cad': float(self.cash_balance_total_cad)
            },
            'holdings': {
                symbol: {
                    'shares': float(data['shares']),
                    'market_value': float(data['market_value']),
                    'currency': data['currency']
                } for symbol, data in self.holdings.items()
            }
        }


class AssetValuationService:
    """统一资产估值服务 - 所有资产计算的单一入口"""
    
    def __init__(self, *, auto_refresh_prices: bool = False):
        # auto_refresh_prices 控制是否在读取缓存价格时触发外部刷新
        self.auto_refresh_prices = auto_refresh_prices
        self.stock_price_service = StockPriceService()
        self.currency_service = CurrencyService()
        self.history_cache_service = StockHistoryCacheService()
        
    def get_asset_snapshot(self, account_id: int, target_date: Optional[date] = None) -> AssetSnapshot:
        """
        获取指定日期的完整资产快照
        
        Args:
            account_id: 账户ID
            target_date: 目标日期，None表示今天
            
        Returns:
            AssetSnapshot对象，包含完整的资产信息
        """
        if target_date is None:
            target_date = date.today()
            
        logger.info(f"计算账户{account_id}在{target_date}的资产快照")
        
        snapshot = AssetSnapshot(target_date, account_id)
        
        # 1. 计算股票市值
        snapshot.stock_market_value, snapshot.holdings = self._calculate_stock_market_value(
            account_id, target_date
        )
        
        # 2. 计算现金余额
        snapshot.cash_balance_cad, snapshot.cash_balance_usd = self._calculate_cash_balance(
            account_id, target_date
        )
        
        # 3. 统一转换为CAD计算总资产
        usd_to_cad = self.currency_service.get_current_rate('USD', 'CAD')
        usd_to_cad_decimal = Decimal(str(usd_to_cad)) if not isinstance(usd_to_cad, Decimal) else usd_to_cad
        snapshot.cash_balance_total_cad = (
            snapshot.cash_balance_cad + 
            snapshot.cash_balance_usd * usd_to_cad_decimal
        )
        
        snapshot.total_assets = snapshot.stock_market_value + snapshot.cash_balance_total_cad
        
        logger.info(f"账户{account_id}资产快照计算完成: 总资产=${snapshot.total_assets}")
        
        return snapshot
    
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

        try:
            # 使用统一的AssetValuationService计算逻辑
            logger.info("调用_get_unified_portfolio_data方法")
            return self._get_unified_portfolio_data(account_ids, target_date)
        except Exception as e:
            logger.error(f"获取详细投资组合数据失败: {e}", exc_info=True)
            # 回退到简化版本
            logger.warning("回退到_get_simplified_portfolio_data方法")
            return self._get_simplified_portfolio_data(account_ids, target_date)

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

            for (symbol,) in all_symbols:
                if not symbol:
                    continue

                # 获取当前持仓
                holdings = self._get_holdings_at_date(account_id, target_date)
                current_shares = holdings.get(symbol, 0)

                # 获取股票基本信息
                currency = Transaction.get_currency_by_stock_symbol(symbol)
                current_price = self.stock_price_service.get_cached_stock_price(
                    symbol, currency, auto_refresh=self.auto_refresh_prices
                ) or 0

                # 计算统计数据
                bought_total, sold_total, total_bought_shares, total_sold_shares, realized_gain = self._calculate_stock_statistics(
                    account_id, symbol, target_date
                )

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
                        'current_price': current_price,
                        'accounts': {}
                    }

                # 存储每个账户的数据
                all_stock_data[symbol]['accounts'][account_id] = {
                    'account': account,
                    'current_shares': current_shares,
                    'bought_total': float(bought_total),
                    'sold_total': float(sold_total),
                    'total_bought_shares': float(total_bought_shares),
                    'total_sold_shares': float(total_sold_shares),
                    'realized_gain': float(realized_gain),
                    'dividend_interest': dividend_interest
                }

        # 第二步：根据所有账户的持仓状态决定每只股票的分类
        for symbol, stock_info in all_stock_data.items():
            currency = stock_info['currency']
            current_price = stock_info['current_price']
            accounts_data = stock_info['accounts']

            # 检查是否有任何账户还持有这只股票
            has_current_holdings = any(acc_data['current_shares'] > 0 for acc_data in accounts_data.values())

            if has_current_holdings:
                # 有持仓：为每个有持仓的账户创建持仓记录
                for account_id, acc_data in accounts_data.items():
                    if acc_data['current_shares'] > 0:
                        account = acc_data['account']
                        current_shares = acc_data['current_shares']
                        dividend_interest = acc_data['dividend_interest']

                        # 当前持仓 - 计算市值
                        current_value = float(current_shares) * float(current_price)
                        current_value_cad = current_value * float(exchange_rate) if currency == 'USD' else current_value

                        # 计算成本基础，获取CAD与原始币种的成本
                        cost_breakdown = self._calculate_cost_basis_breakdown(
                            account_id, symbol, target_date, current_shares
                        )
                        total_cost_cad = float(cost_breakdown['total_cost_cad'])
                        total_cost_native = float(cost_breakdown['total_cost_native'])

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
                        previous_price = self._get_previous_close_price(symbol, currency, target_date)
                        if previous_price and previous_price > 0:
                            daily_change_per_share = float(current_price) - float(previous_price)
                            daily_change_value = daily_change_per_share * float(current_shares)
                            daily_change_percent = (daily_change_per_share / float(previous_price)) * 100

                        percent_base_cost = total_cost_native if currency == 'USD' else total_cost_cad

                        holding_data = {
                            'symbol': symbol,
                            'account_id': account_id,
                            'account_name': self._get_account_name_with_members(account),
                            'currency': currency,
                            'shares': float(current_shares),
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

        # 转换为CAD等价
        dividend_received_cad = dividend_received * float(exchange_rate) if currency == 'USD' else dividend_received
        interest_received_cad = interest_received * float(exchange_rate) if currency == 'USD' else interest_received

        return {
            'dividend_received': dividend_received,
            'interest_received': interest_received,
            'dividend_received_cad': dividend_received_cad,
            'interest_received_cad': interest_received_cad
        }

    def _get_previous_close_price(self, symbol: str, currency: str, target_date: date) -> Optional[float]:
        """
        获取前一个交易日的收盘价，排除股市休市日
        """
        from datetime import timedelta

        if not symbol or not target_date:
            return None

        # 计算搜索窗口（向前14天）
        window_start = target_date - timedelta(days=14)
        effective_end = min(target_date, date.today())

        try:
            # 获取历史数据
            history = self.history_cache_service.get_cached_history(
                symbol,
                window_start,
                effective_end,
                currency=currency
            )

            if not history:
                return None

            # 确定市场类型（美国或加拿大）
            market = self.history_cache_service._get_market(symbol, currency)

            # 按日期排序，最新的在前
            history_sorted = sorted(
                (record for record in history if record.get('date') and record.get('close') is not None),
                key=lambda r: r['date'],
                reverse=True
            )

            # 寻找目标日期之前最近的交易日价格，排除休市日
            for record in history_sorted:
                try:
                    record_date = datetime.fromisoformat(record['date']).date()
                except ValueError:
                    continue

                # 如果是目标日期或之后，跳过
                if record_date >= target_date:
                    continue

                # 检查是否为交易日（排除周末和法定假期）
                if (record_date.weekday() < 5 and
                    not self.history_cache_service._is_market_holiday_by_market(market, record_date)):
                    return float(record['close'])

            return None

        except Exception as e:
            logger.error(f"获取{symbol}前一日收盘价失败: {e}")
            return None

    def _get_simplified_portfolio_data(self, account_ids: List[int], target_date: date) -> Dict:
        """
        获取简化版的投资组合数据（回退方案）
        """
        current_holdings = []
        cleared_holdings = []

        # 获取汇率
        exchange_rate = self.currency_service.get_current_rate('USD', 'CAD')

        for account_id in account_ids:
            account = Account.query.get(account_id)
            if not account:
                continue
            
            # 获取该账户的所有股票交易
            symbols = Transaction.query.filter(
                Transaction.account_id == account_id,
                Transaction.stock.isnot(None),
                Transaction.stock != '',
                Transaction.trade_date <= target_date
            ).with_entities(Transaction.stock).distinct().all()
            
            for (symbol,) in symbols:
                if not symbol:
                    continue
                
                # 获取持仓情况
                holdings = self._get_holdings_at_date(account_id, target_date)
                current_shares = holdings.get(symbol, 0)
                
                # 获取股票基本信息
                stock_info = self._get_stock_info(symbol)
                currency = stock_info.get('currency', 'USD')
                
                # 计算基本统计
                bought_total, sold_total, total_bought_shares, total_sold_shares, realized_gain = self._calculate_stock_statistics(
                    account_id, symbol, target_date
                )
                
                # 获取当前价格
                current_price = self.stock_price_service.get_cached_stock_price(
                    symbol, currency, auto_refresh=self.auto_refresh_prices
                ) or Decimal('0')
                
                if current_shares > 0:
                    # 当前持仓
                    avg_cost = bought_total / total_bought_shares if total_bought_shares > 0 else Decimal('0')
                    current_value = Decimal(str(current_shares)) * current_price
                    unrealized_gain = current_value - (Decimal(str(current_shares)) * avg_cost)
                    
                    current_holdings.append({
                        'symbol': symbol,
                        'account_id': account_id,
                        'account_name': self._get_account_name_with_members(account),
                        'currency': currency,
                        'shares': float(current_shares),
                        'average_cost': float(avg_cost),
                        'total_cost': float(Decimal(str(current_shares)) * avg_cost),
                        'current_price': float(current_price),
                        'current_value': float(current_value),
                        'unrealized_gain': float(unrealized_gain),
                        'unrealized_gain_percent': float((unrealized_gain / (Decimal(str(current_shares)) * avg_cost) * 100)) if avg_cost > 0 else 0,
                        'realized_gain': float(realized_gain),
                        'dividends': 0,  # 简化版本暂不计算
                        'interest': 0,   # 简化版本暂不计算
                        'company_name': symbol,
                        'sector': 'Unknown'
                    })
                
                elif total_sold_shares > 0:
                    # 清仓股票 - 计算分红利息
                    dividend_interest = self._calculate_stock_dividend_interest(
                        account_id=account_id,
                        symbol=symbol,
                        target_date=target_date,
                        currency=currency,
                        exchange_rate=exchange_rate
                    )

                    cleared_holdings.append({
                        'symbol': symbol,
                        'account_id': account_id,
                        'account_name': self._get_account_name_with_members(account),
                        'currency': currency,
                        'total_bought_shares': float(total_bought_shares),
                        'total_sold_shares': float(total_sold_shares),
                        'total_bought_value': float(bought_total),
                        'total_sold_value': float(sold_total),
                        'realized_gain': float(realized_gain),
                        'dividends': dividend_interest['dividend_received'],  # 使用计算得到的分红
                        'interest': dividend_interest['interest_received'],   # 使用计算得到的利息
                        'dividend_received': dividend_interest['dividend_received'],  # 保持兼容性
                        'interest_received': dividend_interest['interest_received'],   # 保持兼容性
                        'company_name': symbol,
                        'sector': 'Unknown'
                    })
        
        return {
            'current_holdings': current_holdings,
            'cleared_holdings': cleared_holdings
        }
    
    def _calculate_stock_statistics(self, account_id: int, symbol: str, target_date: date) -> tuple:
        """
        计算股票的基本统计数据
        
        Returns:
            (买入总额, 卖出总额, 买入总股数, 卖出总股数, 已实现收益)
        """
        transactions = Transaction.query.filter(
            Transaction.account_id == account_id,
            Transaction.stock == symbol,
            Transaction.trade_date <= target_date,
            Transaction.type.in_(['BUY', 'SELL'])
        ).order_by(Transaction.trade_date.asc()).all()
        
        bought_total = Decimal('0')
        sold_total = Decimal('0')
        total_bought_shares = Decimal('0')
        total_sold_shares = Decimal('0')
        
        # FIFO计算已实现收益
        buy_lots = []  # [(shares, price), ...]
        realized_gain = Decimal('0')
        
        for tx in transactions:
            quantity = Decimal(str(tx.quantity or 0))
            price = Decimal(str(tx.price or 0))
            fee = Decimal(str(tx.fee or 0))
            
            if tx.type == 'BUY':
                total_cost = quantity * price + fee
                bought_total += total_cost
                total_bought_shares += quantity
                buy_lots.append((quantity, price + fee / quantity if quantity > 0 else price))
            
            elif tx.type == 'SELL':
                net_proceeds = quantity * price - fee
                sold_total += net_proceeds
                total_sold_shares += quantity
                
                # FIFO计算已实现收益
                remaining_to_sell = quantity
                while remaining_to_sell > 0 and buy_lots:
                    lot_shares, lot_cost = buy_lots[0]
                    
                    if lot_shares <= remaining_to_sell:
                        # 完全卖出这个批次
                        sell_proceeds = lot_shares * (price - fee / quantity if quantity > 0 else price)
                        cost_basis = lot_shares * lot_cost
                        realized_gain += sell_proceeds - cost_basis
                        
                        remaining_to_sell -= lot_shares
                        buy_lots.pop(0)
                    else:
                        # 部分卖出这个批次
                        sell_proceeds = remaining_to_sell * (price - fee / quantity if quantity > 0 else price)
                        cost_basis = remaining_to_sell * lot_cost
                        realized_gain += sell_proceeds - cost_basis
                        
                        buy_lots[0] = (lot_shares - remaining_to_sell, lot_cost)
                        remaining_to_sell = Decimal('0')
        
        return bought_total, sold_total, total_bought_shares, total_sold_shares, realized_gain
    
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
        total_return = total_realized_gain + total_unrealized_gain + total_dividends + total_interest

        # 计算总回报率
        total_return_rate = float(total_return / total_deposits * 100) if total_deposits > 0 else 0.0
        
        # 计算当日变化
        daily_change = self._calculate_daily_change(account_ids, target_date)

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
                'cad_only': float(realized_gain_cad + unrealized_gain_cad + dividends_cad + interest_cad),  # CAD币种总回报
                'usd_only': float(realized_gain_usd + unrealized_gain_usd + dividends_usd + interest_usd),  # USD币种总回报
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

    def _calculate_daily_change(self, account_ids: List[int], target_date: date) -> Dict:
        """计算当日变化 - 基于个股的daily_change_value汇总"""
        try:
            total_daily_change_cad = Decimal('0')
            total_daily_change_cad_only = Decimal('0')
            total_daily_change_usd_only = Decimal('0')
            
            # 获取汇率
            exchange_rate = self.currency_service.get_current_rate('USD', 'CAD')
            exchange_rate_decimal = Decimal(str(exchange_rate))
            
            # 遍历所有账户，汇总个股的当日变化
            for account_id in account_ids:
                # 获取该账户的所有持仓
                holdings = self._get_holdings_at_date(account_id, target_date)
                
                for symbol, shares in holdings.items():
                    if shares <= 0:
                        continue
                    
                    # 获取股票信息
                    currency = Transaction.get_currency_by_stock_symbol(symbol)
                    current_price = self.stock_price_service.get_cached_stock_price(
                        symbol, currency, auto_refresh=self.auto_refresh_prices
                    ) or 0
                    
                    if current_price > 0:
                        # 获取前一日收盘价
                        previous_price = self._get_previous_close_price(symbol, currency, target_date)
                        if previous_price and previous_price > 0:
                            # 计算当日变化
                            daily_change_value = shares * (current_price - previous_price)
                            
                            # 按币种分类
                            if currency == 'CAD':
                                total_daily_change_cad_only += daily_change_value
                            else:  # USD
                                total_daily_change_usd_only += daily_change_value
                            
                            # 转换为CAD
                            if currency == 'USD':
                                total_daily_change_cad += daily_change_value * exchange_rate_decimal
                            else:
                                total_daily_change_cad += daily_change_value
            
            return {
                'cad': float(total_daily_change_cad),
                'cad_only': float(total_daily_change_cad_only),
                'usd_only': float(total_daily_change_usd_only)
            }
            
        except Exception as e:
            logger.error(f"计算当日变化失败: {e}")
            return {
                'cad': 0.0,
                'cad_only': 0.0,
                'usd_only': 0.0
            }

    def _get_previous_close_price(self, symbol: str, currency: str, target_date: date) -> Optional[Decimal]:
        """获取前一日收盘价"""
        try:
            # 获取前一个交易日
            previous_date = target_date - timedelta(days=1)
            while previous_date.weekday() >= 5:  # 跳过周末
                previous_date -= timedelta(days=1)
            
            # 获取历史价格
            return self._get_historical_stock_price(symbol, previous_date, currency)
            
        except Exception as e:
            logger.error(f"获取{symbol}前一日收盘价失败: {e}")
            return None

    def _get_account_proportion(self, account_id: int,
                                ownership_map: Optional[Dict[int, Decimal]]) -> Decimal:
        if not ownership_map:
            return Decimal('1')
        return ownership_map.get(account_id, Decimal('0'))
    
    def _get_account_stats_by_currency(self, account_id: int, target_date: date) -> Dict:
        """
        获取账户按币种分类的统计数据
        """
        # 初始化数据
        stats = {
            'stock_value_cad': Decimal('0'),
            'stock_value_usd': Decimal('0'),
            'realized_gain_cad': Decimal('0'),
            'realized_gain_usd': Decimal('0'),
            'unrealized_gain_cad': Decimal('0'),
            'unrealized_gain_usd': Decimal('0'),
            'dividends_cad': Decimal('0'),
            'dividends_usd': Decimal('0'),
            'interest_cad': Decimal('0'),
            'interest_usd': Decimal('0')
        }
        
        # 获取目标日期的实际持仓（只计算仍持有的股票）
        holdings = self._get_holdings_at_date(account_id, target_date)

        for symbol, shares in holdings.items():
            if shares <= 0:
                continue

            currency = Transaction.get_currency_by_stock_symbol(symbol)

            # 计算该股票的市值和收益
            stock_stats = self._calculate_stock_stats(account_id, symbol, target_date)

            if currency == 'CAD':
                stats['stock_value_cad'] += stock_stats['market_value']
                stats['realized_gain_cad'] += stock_stats['realized_gain']
                stats['unrealized_gain_cad'] += stock_stats['unrealized_gain']
            else:
                stats['stock_value_usd'] += stock_stats['market_value']
                stats['realized_gain_usd'] += stock_stats['realized_gain']
                stats['unrealized_gain_usd'] += stock_stats['unrealized_gain']
            
            
        
        # 计算股息和利息（按币种）
        dividend_interest_stats = self._calculate_dividend_interest_by_currency(account_id, target_date)
        stats.update(dividend_interest_stats)
        
        return stats
    

    


    def _calculate_stock_stats(self, account_id: int, symbol: str,  target_date: date) -> Dict:
        """
        计算单个股票的统计数据
        """
        # 获取所有相关交易
        transactions = Transaction.query.filter(
            Transaction.account_id == account_id,
            Transaction.stock == symbol,
            Transaction.trade_date <= target_date,
            Transaction.type.in_(['BUY', 'SELL'])
            
        ).order_by(Transaction.trade_date.asc()).all()
        
        # FIFO计算
        buy_lots = []
        current_shares = Decimal('0')
        total_cost = Decimal('0')
        realized_gain = Decimal('0')
        
       

        for tx in transactions:
           
            quantity = Decimal(str(tx.quantity or 0))
            net_amount = Decimal(str(tx.net_amount or 0))
            
            if tx.type == 'BUY':
                buy_lots.append({'shares': quantity, 'cost': net_amount})
                current_shares += quantity
                total_cost += net_amount
            elif tx.type == 'SELL':
                remaining_to_sell = quantity
                sell_proceeds = net_amount
                cost_basis = Decimal('0')
                
                while remaining_to_sell > 0 and buy_lots:
                    lot = buy_lots[0]
                    if lot['shares'] <= remaining_to_sell:
                        # 完全卖出这个lot
                        cost_basis += lot['cost']
                        remaining_to_sell -= lot['shares']
                        current_shares -= lot['shares']
                        total_cost -= lot['cost']
                        buy_lots.pop(0)
                    else:
                        # 部分卖出
                        sell_ratio = remaining_to_sell / lot['shares']
                        cost_from_lot = lot['cost'] * sell_ratio
                        cost_basis += cost_from_lot
                        
                        lot['shares'] -= remaining_to_sell
                        lot['cost'] -= cost_from_lot
                        current_shares -= remaining_to_sell
                        total_cost -= cost_from_lot
                        remaining_to_sell = Decimal('0')
                
                realized_gain += sell_proceeds - cost_basis
        
        # 计算市值和未实现收益
        market_value = Decimal('0')
        unrealized_gain = Decimal('0')
        
        if current_shares > 0:
            # 智能货币检测逻辑
            stock_info = self._get_stock_info(symbol)
            currency = stock_info['currency']
                
            price = self.stock_price_service.get_cached_stock_price(
                symbol, currency, auto_refresh=self.auto_refresh_prices
            )
            if price and price > 0:
                market_value = current_shares * Decimal(str(price))
                unrealized_gain = market_value - total_cost
        
        return {
            'market_value': market_value,
            'realized_gain': realized_gain,
            'unrealized_gain': unrealized_gain,
            'current_shares': current_shares
        }
    
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

    def _calculate_account_returns(self, account_id: int, target_date: date) -> tuple[Decimal, Decimal, Decimal]:
        """
        计算账户的已实现收益、股息和利息
        """
        # 获取所有相关交易
        transactions = Transaction.query.filter(
            Transaction.account_id == account_id,
            Transaction.trade_date <= target_date,
            Transaction.type.in_(['BUY', 'SELL', 'DIVIDEND', 'INTEREST'])
        ).order_by(Transaction.trade_date.asc()).all()
        
        # 按股票分组计算已实现收益
        realized_gain_by_stock = {}
        total_dividends = Decimal('0')
        total_interest = Decimal('0')
        
        for tx in transactions:
            if tx.type in ['DIVIDEND', 'INTEREST']:
                amount = Decimal(str(tx.amount or 0))
                # 统一转换为CAD
                if tx.currency == 'USD':
                    exchange_rate = self.currency_service.get_current_rate('USD', 'CAD')
                    amount = amount * Decimal(str(exchange_rate))
                
                if tx.type == 'DIVIDEND':
                    total_dividends += amount
                else:  # INTEREST
                    total_interest += amount
            
            elif tx.type in ['BUY', 'SELL'] and tx.stock:
                symbol = tx.stock
                if symbol not in realized_gain_by_stock:
                    realized_gain_by_stock[symbol] = []
                realized_gain_by_stock[symbol].append(tx)
        
        # 计算每个股票的已实现收益
        total_realized_gain = Decimal('0')
        for symbol, stock_transactions in realized_gain_by_stock.items():
            stock_realized_gain = self._calculate_stock_realized_gain(stock_transactions)
            total_realized_gain += stock_realized_gain
        
        return total_realized_gain, total_dividends, total_interest
    
    def _calculate_stock_realized_gain(self, transactions: List[Transaction]) -> Decimal:
        """
        使用FIFO方法计算单个股票的已实现收益
        """
        buy_lots = []  # [(shares, cost_per_share_cad), ...]
        realized_gain = Decimal('0')
        
        for tx in transactions:
            quantity = Decimal(str(tx.quantity or 0))
            price = Decimal(str(tx.price or 0))
            fee = Decimal(str(tx.fee or 0))
            
            # 统一转换为CAD
            if tx.currency == 'USD':
                exchange_rate = self.currency_service.get_current_rate('USD', 'CAD')  
                price = price * Decimal(str(exchange_rate))
                fee = fee * Decimal(str(exchange_rate))
            
            if tx.type == 'BUY':
                cost_per_share = price + (fee / quantity if quantity > 0 else Decimal('0'))
                buy_lots.append((quantity, cost_per_share))
                
            elif tx.type == 'SELL':
                net_price_per_share = price - (fee / quantity if quantity > 0 else Decimal('0'))
                remaining_to_sell = quantity
                
                # FIFO处理卖出
                while remaining_to_sell > 0 and buy_lots:
                    lot_shares, lot_cost_per_share = buy_lots[0]
                    
                    if lot_shares <= remaining_to_sell:
                        # 完全卖出这个批次
                        gain = lot_shares * (net_price_per_share - lot_cost_per_share)
                        realized_gain += gain
                        remaining_to_sell -= lot_shares
                        buy_lots.pop(0)
                    else:
                        # 部分卖出这个批次  
                        gain = remaining_to_sell * (net_price_per_share - lot_cost_per_share)
                        realized_gain += gain
                        buy_lots[0] = (lot_shares - remaining_to_sell, lot_cost_per_share)
                        remaining_to_sell = Decimal('0')
        
        return realized_gain
    
    def _calculate_cost_basis(self, account_id: int, symbol: str, target_date: date, current_shares: Decimal) -> Decimal:
        """计算当前持仓的成本基础（返回CAD等值）"""
        breakdown = self._calculate_cost_basis_breakdown(account_id, symbol, target_date, current_shares)
        return breakdown['total_cost_cad']

    def _calculate_cost_basis_breakdown(self, account_id: int, symbol: str,
                                        target_date: date, current_shares: Decimal) -> Dict[str, Decimal | str]:
        """计算成本基础的详细分解，包含CAD和原始币种"""
        transactions = Transaction.query.filter(
            Transaction.account_id == account_id,
            Transaction.stock == symbol,
            Transaction.trade_date <= target_date,
            Transaction.type.in_(['BUY', 'SELL'])
        ).order_by(Transaction.trade_date.asc()).all()
        
        buy_lots: List[tuple[Decimal, Decimal, Decimal]] = []  # (shares, cost_per_share_cad, cost_per_share_native)
        native_currency = None
        
        # 重建FIFO队列
        for tx in transactions:
            quantity = Decimal(str(tx.quantity or 0))
            price = Decimal(str(tx.price or 0))
            fee = Decimal(str(tx.fee or 0))
            currency = (tx.currency or 'USD').upper()

            if native_currency is None:
                native_currency = currency
            
            # 每股成本（原始币种）
            cost_per_share_native = price + (fee / quantity if quantity > 0 else Decimal('0'))

            # 转换为CAD
            to_cad_rate = Decimal(str(self.currency_service.get_current_rate(currency, 'CAD'))) if currency != 'CAD' else Decimal('1')
            cost_per_share_cad = cost_per_share_native * to_cad_rate
            
            if tx.type == 'BUY':
                buy_lots.append((quantity, cost_per_share_cad, cost_per_share_native))
                
            elif tx.type == 'SELL':
                remaining_to_sell = quantity
                while remaining_to_sell > 0 and buy_lots:
                    lot_shares, lot_cost_per_share_cad, lot_cost_per_share_native = buy_lots[0]
                    if lot_shares <= remaining_to_sell:
                        remaining_to_sell -= lot_shares
                        buy_lots.pop(0)
                    else:
                        buy_lots[0] = (
                            lot_shares - remaining_to_sell,
                            lot_cost_per_share_cad,
                            lot_cost_per_share_native
                        )
                        remaining_to_sell = Decimal('0')
        
        # 计算当前持仓的成本基础
        total_cost_cad = Decimal('0')
        total_cost_native = Decimal('0')
        remaining_shares = current_shares
        
        for lot_shares, lot_cost_per_share_cad, lot_cost_per_share_native in buy_lots:
            if remaining_shares <= 0:
                break
            shares_to_count = min(lot_shares, remaining_shares)
            total_cost_cad += shares_to_count * lot_cost_per_share_cad
            total_cost_native += shares_to_count * lot_cost_per_share_native
            remaining_shares -= shares_to_count
        
        if native_currency is None:
            native_currency = 'CAD'

        return {
            'total_cost_cad': total_cost_cad,
            'total_cost_native': total_cost_native,
            'native_currency': native_currency
        }
    
    def get_total_assets(self, account_id: int, target_date: Optional[date] = None) -> Decimal:
        """获取总资产金额"""
        snapshot = self.get_asset_snapshot(account_id, target_date)
        return snapshot.total_assets
    
    def get_stock_market_value(self, account_id: int, target_date: Optional[date] = None) -> Decimal:
        """获取股票市值"""
        snapshot = self.get_asset_snapshot(account_id, target_date)
        return snapshot.stock_market_value
    
    def get_cash_balance(self, account_id: int, target_date: Optional[date] = None) -> Dict[str, Decimal]:
        """获取现金余额"""
        snapshot = self.get_asset_snapshot(account_id, target_date)
        return {
            'cad': snapshot.cash_balance_cad,
            'usd': snapshot.cash_balance_usd,
            'total_cad': snapshot.cash_balance_total_cad
        }

    
    def _calculate_stock_market_value(self, account_id: int, target_date: date) -> tuple[Decimal, Dict]:
        """
        计算股票市值
        
        Returns:
            (总市值CAD, 持仓详情字典)
        """
        logger.debug(f"计算账户{account_id}在{target_date}的股票市值")
        
        # 获取该日期的持仓情况
        holdings = self._get_holdings_at_date(account_id, target_date)
        
        total_market_value = Decimal('0')
        holdings_detail = {}
        
        # 判断是否需要历史价格
        is_historical = target_date < date.today()
        
        for symbol, shares in holdings.items():
            if shares <= 0:
                continue
                
            # 获取股价和货币
            stock_info = self._get_stock_info(symbol)
            currency = stock_info.get('currency', 'USD')
            
            # 根据日期选择价格获取方式
            if is_historical:
                # 使用历史价格
                price = self._get_historical_stock_price(symbol, target_date, currency)
            else:
                # 使用当前缓存价格
                price = self.stock_price_service.get_cached_stock_price(
                    symbol, currency, auto_refresh=self.auto_refresh_prices
                )
            
            if price is None or price <= 0:
                logger.warning(f"无法获取{symbol}在{target_date}的价格")
                continue
                
            # 确保所有值都是Decimal类型以避免类型错误
            shares_decimal = Decimal(str(shares))
            price_decimal = Decimal(str(price)) if not isinstance(price, Decimal) else price
            market_value = shares_decimal * price_decimal
            
            # 转换为CAD
            if currency == 'USD':
                exchange_rate = self.currency_service.get_current_rate('USD', 'CAD')
                exchange_rate_decimal = Decimal(str(exchange_rate)) if not isinstance(exchange_rate, Decimal) else exchange_rate
                market_value_cad = market_value * exchange_rate_decimal
            else:  # CAD
                market_value_cad = market_value
            
            total_market_value += market_value_cad
            
            holdings_detail[symbol] = {
                'shares': shares_decimal,
                'market_value': market_value_cad,
                'currency': currency
            }
            
        return total_market_value, holdings_detail
    
    def _get_historical_stock_price(self, symbol: str, target_date: date, currency: str) -> Optional[Decimal]:
        """
        获取指定日期的历史股票价格
        
        Args:
            symbol: 股票代码
            target_date: 目标日期
            
        Returns:
            该日期的股票价格，如果无法获取返回None
        """
        try:
            # 未来日期改为今天，避免请求未来的价格
            if target_date > date.today():
                target_date = date.today()

            # 使用StockPriceService获取历史价格
            # 获取包含目标日期的历史数据范围
            start_date = target_date - timedelta(days=7)
            end_date = min(target_date + timedelta(days=1), date.today())

            history_records = self.history_cache_service.get_cached_history(
                symbol,
                start_date,
                end_date,
                currency=currency
            )

            history_map = {record['date']: record for record in history_records}

            date_str = target_date.strftime('%Y-%m-%d')
            record = history_map.get(date_str)
            if record and record.get('close') is not None:
                return Decimal(str(record['close']))

            # 如果目标日期没有数据，查找最近的前一个交易日
            current_date = target_date
            for i in range(7):  # 最多向前查找7天
                current_date -= timedelta(days=1)
                date_str = current_date.strftime('%Y-%m-%d')
                record = history_map.get(date_str)
                if record and record.get('close') is not None:
                    logger.debug(f"使用{current_date}的价格作为{target_date}的历史价格: {symbol}")
                    return Decimal(str(record['close']))
            
            # 如果都没有找到，返回None
            logger.warning(f"无法获取{symbol}在{target_date}附近的历史价格")
            return None
            
        except Exception as e:
            logger.error(f"获取{symbol}历史价格失败: {e}")
            return None
    
    def _calculate_cash_balance(self, account_id: int, target_date: date) -> tuple[Decimal, Decimal]:
        """
        计算指定日期的现金余额
        - 今天或未来日期：使用Cash表的当前数据
        - 历史日期：通过交易记录倒推计算

        Returns:
            (CAD余额, USD余额)
        """
        logger.debug(f"计算账户{account_id}在{target_date}的现金余额")

        today = date.today()

        if target_date >= today:
            # 今天或未来日期：使用Cash表的当前数据
            cash_record = Cash.get_account_cash(account_id)
            if cash_record:
                cad_balance = Decimal(str(cash_record.cad or 0))
                usd_balance = Decimal(str(cash_record.usd or 0))
                return cad_balance, usd_balance
            else:
                return Decimal('0'), Decimal('0')
        else:
            # 历史日期：通过交易记录倒推计算
            return self._calculate_historical_cash_balance_from_current(account_id, target_date)
    
    def _calculate_historical_cash_balance(self, account_id: int, target_date: date) -> tuple[Decimal, Decimal]:
        """
        通过交易记录计算历史现金余额
        直接返回计算结果，允许负余额（用户可能没有输入完整的现金记录）
        """
        # 获取目标日期及之前的所有现金相关交易
        transactions = Transaction.query.filter(
            Transaction.account_id == account_id,
            Transaction.trade_date <= target_date,
            Transaction.type.in_(['DEPOSIT', 'WITHDRAW', 'BUY', 'SELL', 'DIVIDEND', 'INTEREST'])
        ).order_by(Transaction.trade_date.asc()).all()

        # 从零开始累计计算
        cad_balance = Decimal('0')
        usd_balance = Decimal('0')

        for transaction in transactions:
            cad_balance, usd_balance = self._apply_transaction_impact(
                cad_balance, usd_balance, transaction
            )

        logger.info(f"账户{account_id}历史现金({target_date}): CAD=${cad_balance}, USD=${usd_balance}")
        return cad_balance, usd_balance

    def _calculate_historical_cash_balance_from_current(self, account_id: int, target_date: date) -> tuple[Decimal, Decimal]:
        """
        从Cash表的当前数据开始，倒推到目标日期的现金余额
        """
        # 获取当前现金余额作为基准
        cash_record = Cash.get_account_cash(account_id)
        if cash_record:
            current_cad = Decimal(str(cash_record.cad or 0))
            current_usd = Decimal(str(cash_record.usd or 0))
        else:
            current_cad = Decimal('0')
            current_usd = Decimal('0')

        # 获取目标日期之后的所有交易（需要倒推的交易）
        transactions = Transaction.query.filter(
            Transaction.account_id == account_id,
            Transaction.trade_date > target_date,
            Transaction.type.in_(['DEPOSIT', 'WITHDRAW', 'BUY', 'SELL', 'DIVIDEND', 'INTEREST'])
        ).order_by(Transaction.trade_date.desc()).all()  # 按时间倒序，从最近开始倒推

        cad_balance = current_cad
        usd_balance = current_usd

        # 倒推每笔交易的影响（反向操作）
        for transaction in transactions:
            cad_balance, usd_balance = self._reverse_transaction_impact(
                cad_balance, usd_balance, transaction
            )

        logger.info(f"账户{account_id}倒推历史现金({target_date}): CAD=${cad_balance}, USD=${usd_balance}")
        return cad_balance, usd_balance

    def _reverse_transaction_impact(self, cad: Decimal, usd: Decimal, transaction: Transaction) -> tuple[Decimal, Decimal]:
        """倒推交易对现金的影响（与_apply_transaction_impact相反的操作）"""
        currency = transaction.currency or 'USD'

        if transaction.type == 'DEPOSIT':
            # 倒推存入：减少现金
            amount = Decimal(str(transaction.amount or 0))
            if currency == 'CAD':
                cad -= amount
            else:
                usd -= amount

        elif transaction.type == 'WITHDRAW':
            # 倒推取出：增加现金
            amount = Decimal(str(transaction.amount or 0))
            if currency == 'CAD':
                cad += amount
            else:
                usd += amount

        elif transaction.type == 'BUY':
            # 倒推买入：增加现金（因为当时减少了现金）
            quantity = Decimal(str(transaction.quantity or 0))
            price = Decimal(str(transaction.price or 0))
            total_cost = quantity * price + Decimal(str(transaction.fee or 0))
            if currency == 'CAD':
                cad += total_cost
            else:
                usd += total_cost

        elif transaction.type == 'SELL':
            # 倒推卖出：减少现金（因为当时增加了现金）
            quantity = Decimal(str(transaction.quantity or 0))
            price = Decimal(str(transaction.price or 0))
            net_proceeds = quantity * price - Decimal(str(transaction.fee or 0))
            if currency == 'CAD':
                cad -= net_proceeds
            else:
                usd -= net_proceeds

        elif transaction.type == 'DIVIDEND' or transaction.type == 'INTEREST':
            # 倒推分红/利息：减少现金
            amount = Decimal(str(transaction.amount or 0))
            if currency == 'CAD':
                cad -= amount
            else:
                usd -= amount

        return cad, usd

    def _calculate_unrealized_gain_for_account(self, account_id: int, target_date: date) -> Decimal:
        """计算账户的浮动盈亏（市值 - 成本基础）"""
        total_unrealized_gain = Decimal('0')

        # 获取持仓
        holdings = self._get_holdings_at_date(account_id, target_date)

        for symbol, shares in holdings.items():
            if shares <= 0:
                continue

            # 获取市值
            stock_info = self._get_stock_info(symbol)
            currency = stock_info.get('currency', 'USD')
            price = self.stock_price_service.get_cached_stock_price(
                symbol, currency, auto_refresh=self.auto_refresh_prices
            )

            if price:
                market_value = shares * Decimal(str(price))

                # 转换为CAD
                if currency == 'USD':
                    exchange_rate = self.currency_service.get_current_rate('USD', 'CAD')
                    market_value_cad = market_value * Decimal(str(exchange_rate))
                else:
                    market_value_cad = market_value

                # 获取成本基础
                cost_basis = self._calculate_cost_basis(account_id, symbol, target_date, shares)

                # 浮动盈亏
                unrealized_gain = market_value_cad - cost_basis
                total_unrealized_gain += unrealized_gain

        return total_unrealized_gain

    def _calculate_realized_gain_for_account(self, account_id: int, target_date: date) -> Decimal:
        """计算账户的已实现收益（所有已完成交易的盈亏）"""
        total_realized_gain = Decimal('0')

        # 获取所有卖出交易
        sell_transactions = Transaction.query.filter(
            Transaction.account_id == account_id,
            Transaction.type == 'SELL',
            Transaction.trade_date <= target_date
        ).order_by(Transaction.trade_date.asc()).all()

        for sell_tx in sell_transactions:
            symbol = sell_tx.stock
            sell_shares = Decimal(str(sell_tx.quantity or 0))
            sell_price = Decimal(str(sell_tx.price or 0))
            sell_fee = Decimal(str(sell_tx.fee or 0))

            # 卖出净收入
            sell_proceeds = sell_shares * sell_price - sell_fee

            # 计算这些股份的成本基础（使用FIFO）
            cost_basis = self._calculate_cost_basis_for_sold_shares(
                account_id, symbol, sell_tx.trade_date, sell_shares
            )

            # 已实现盈亏 = 卖出收入 - 成本基础
            realized_gain = sell_proceeds - cost_basis

            # 转换为CAD
            currency = sell_tx.currency or 'USD'
            if currency == 'USD':
                exchange_rate = self.currency_service.get_current_rate('USD', 'CAD')
                realized_gain_cad = realized_gain * Decimal(str(exchange_rate))
            else:
                realized_gain_cad = realized_gain

            total_realized_gain += realized_gain_cad

        return total_realized_gain

    def _calculate_cost_basis_for_sold_shares(self, account_id: int, symbol: str, sell_date: date, shares_sold: Decimal) -> Decimal:
        """使用FIFO计算卖出股份的成本基础"""
        # 获取卖出日期之前的所有买入交易
        buy_transactions = Transaction.query.filter(
            Transaction.account_id == account_id,
            Transaction.stock == symbol,
            Transaction.type == 'BUY',
            Transaction.trade_date <= sell_date
        ).order_by(Transaction.trade_date.asc()).all()

        total_cost = Decimal('0')
        remaining_shares = shares_sold

        for buy_tx in buy_transactions:
            if remaining_shares <= 0:
                break

            buy_shares = Decimal(str(buy_tx.quantity or 0))
            buy_price = Decimal(str(buy_tx.price or 0))
            buy_fee = Decimal(str(buy_tx.fee or 0))

            # 使用的股份数（FIFO原则）
            shares_used = min(remaining_shares, buy_shares)

            # 按比例分配成本和手续费
            cost_per_share = buy_price + (buy_fee / buy_shares if buy_shares > 0 else Decimal('0'))
            total_cost += shares_used * cost_per_share

            remaining_shares -= shares_used

        return total_cost

    def _calculate_account_metrics_by_currency(self, account_id: int, target_date: date) -> tuple[Decimal, Decimal, Decimal, Decimal, Decimal, Decimal]:
        """按币种分别计算账户的股票市值、已实现收益、浮动盈亏

        Returns:
            (stock_value_cad, stock_value_usd, realized_gain_cad, realized_gain_usd, unrealized_gain_cad, unrealized_gain_usd)
        """
        stock_value_cad = Decimal('0')
        stock_value_usd = Decimal('0')
        realized_gain_cad = Decimal('0')
        realized_gain_usd = Decimal('0')
        unrealized_gain_cad = Decimal('0')
        unrealized_gain_usd = Decimal('0')

        # 获取持仓
        holdings = self._get_holdings_at_date(account_id, target_date)

        for symbol, shares in holdings.items():
            if shares <= 0:
                continue

            # 获取股票信息
            stock_info = self._get_stock_info(symbol)
            currency = stock_info.get('currency', 'USD')

            # 使用统一的股票统计计算方法
            stock_stats = self._calculate_stock_stats(account_id, symbol, target_date)

            # 按币种分类累加
            if currency == 'CAD':
                stock_value_cad += stock_stats['market_value']
                unrealized_gain_cad += stock_stats['unrealized_gain']
            else:
                stock_value_usd += stock_stats['market_value']
                unrealized_gain_usd += stock_stats['unrealized_gain']

        # 计算已实现收益（按币种分别计算）
        sell_transactions = Transaction.query.filter(
            Transaction.account_id == account_id,
            Transaction.type == 'SELL',
            Transaction.trade_date <= target_date
        ).order_by(Transaction.trade_date.asc()).all()

        for sell_tx in sell_transactions:
            symbol = sell_tx.stock
            sell_shares = Decimal(str(sell_tx.quantity or 0))
            sell_price = Decimal(str(sell_tx.price or 0))
            sell_fee = Decimal(str(sell_tx.fee or 0))
            currency = sell_tx.currency or 'USD'

            # 卖出净收入
            sell_proceeds = sell_shares * sell_price - sell_fee

            # 计算成本基础
            cost_basis = self._calculate_cost_basis_for_sold_shares(
                account_id, symbol, sell_tx.trade_date, sell_shares
            )

            # 已实现盈亏
            realized_gain = sell_proceeds - cost_basis
            if currency == 'CAD':
                realized_gain_cad += realized_gain
            else:
                realized_gain_usd += realized_gain

        return stock_value_cad, stock_value_usd, realized_gain_cad, realized_gain_usd, unrealized_gain_cad, unrealized_gain_usd
    
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
                
        elif transaction.type in ['WITHDRAW', 'WITHDRAWAL']:
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
    
    def _get_holdings_at_date(self, account_id: int, target_date: date) -> Dict[str, float]:
        """获取指定日期的持仓情况"""
        # 获取目标日期及之前的所有交易
        transactions = Transaction.query.filter(
            Transaction.account_id == account_id,
            Transaction.trade_date <= target_date,
            Transaction.type.in_(['BUY', 'SELL'])
        ).order_by(Transaction.trade_date.asc()).all()
        
        holdings = {}
        
        for transaction in transactions:
            symbol = transaction.stock
            if not symbol:
                continue
                
            if symbol not in holdings:
                holdings[symbol] = 0
                
            if transaction.type == 'BUY':
                holdings[symbol] += transaction.quantity or 0
            elif transaction.type == 'SELL':
                holdings[symbol] -= transaction.quantity or 0
        
        # 过滤掉零持仓
        return {symbol: shares for symbol, shares in holdings.items() if shares > 0}
    
    def _get_stock_info(self, symbol: str) -> Dict:
        """获取股票基本信息"""
        from app.models import StocksCache
        
        # 首先尝试从交易记录中获取币种
        currency = Transaction.get_currency_by_stock_symbol(symbol)
        if currency:
            return {'currency': currency}
        
        # 从StocksCache表中查找股票信息
        stocks = StocksCache.query.filter_by(symbol=symbol).all()
        
        if stocks:
            # 如果有多个记录，智能选择货币
            if len(stocks) == 1:
                return {'currency': stocks[0].currency}
            
            # 多个记录时的处理逻辑
            if symbol.endswith('.TO'):
                # 对于加拿大股票，优先选择CAD，除非是特殊的美元计价股票
                usd_stock = next((s for s in stocks if s.currency == 'USD'), None)
                cad_stock = next((s for s in stocks if s.currency == 'CAD'), None)
                
                # 特殊案例：-U结尾的.TO股票通常是美元计价
                if symbol.endswith('-U.TO') and usd_stock:
                    return {'currency': 'USD'}
                elif cad_stock:
                    return {'currency': 'CAD'}
                elif usd_stock:
                    return {'currency': 'USD'}
                else:
                    return {'currency': stocks[0].currency}
            else:
                # 非.TO股票使用第一个记录
                return {'currency': stocks[0].currency}
        
        # 如果没有找到，默认使用USD
        return {'currency': 'USD'}
    
    def validate_data_consistency(self, account_id: int) -> List[str]:
        """
        验证数据一致性
        检查计算结果与实际数据是否匹配
        
        Returns:
            错误信息列表，空列表表示数据一致
        """
        errors = []
        
        try:
            # 1. 验证当前持仓与交易记录一致性
            calculated_holdings = self._get_holdings_at_date(account_id, date.today())
            
            # 这里可以添加与数据库中实际持仓的对比逻辑
            # 暂时记录日志
            logger.info(f"账户{account_id}计算持仓: {calculated_holdings}")
            
            # 2. 验证现金余额计算（由于没有Account中的现金字段，这里只记录计算结果）
            calculated_cad, calculated_usd = self._calculate_cash_balance(account_id, date.today())
            logger.info(f"账户{account_id}计算现金余额: CAD=${calculated_cad}, USD=${calculated_usd}")
            
            # 注意：Account模型中没有cash_cad/cash_usd字段，所以无法进行对比验证
            # 这是正常的，因为现金余额是通过交易记录计算得出的
            
        except Exception as e:
            errors.append(f"数据一致性检查出错: {str(e)}")
            logger.error(f"数据一致性检查出错: {e}", exc_info=True)
        
        return errors
    
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
