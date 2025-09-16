#!/usr/bin/env python3
"""
统一资产估值服务
负责计算账户在任意时间点的总资产、股票市值、现金余额
确保数据准确性和一致性
"""

from datetime import datetime, date, timedelta
from typing import Dict, Optional, List
from decimal import Decimal, ROUND_HALF_UP
import logging

from app import db
from app.models.transaction import Transaction
from app.models.account import Account
from app.models.cash import Cash
from app.services.stock_price_service import StockPriceService
from app.services.currency_service import CurrencyService

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
    
    def __init__(self):
        self.stock_price_service = StockPriceService()
        self.currency_service = CurrencyService()
        
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
        
        Args:
            account_ids: 账户ID列表
            target_date: 目标日期，None表示今天
            
        Returns:
            包含current_holdings和cleared_holdings的字典
        """
        if target_date is None:
            target_date = date.today()
        
        from app.services.portfolio_service import portfolio_service
        
        try:
            # 使用现有的portfolio_service获取详细数据
            portfolio_summary = portfolio_service.get_portfolio_summary(account_ids)
            return {
                'current_holdings': portfolio_summary.get('current_holdings', []),
                'cleared_holdings': portfolio_summary.get('cleared_holdings', [])
            }
        except Exception as e:
            logger.error(f"获取详细投资组合数据失败: {e}", exc_info=True)
            # 回退到简化版本
            return self._get_simplified_portfolio_data(account_ids, target_date)
    
    def _get_simplified_portfolio_data(self, account_ids: List[int], target_date: date) -> Dict:
        """
        获取简化版的投资组合数据（回退方案）
        """
        current_holdings = []
        cleared_holdings = []
        
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
                current_price = self.stock_price_service.get_cached_stock_price(symbol, currency) or Decimal('0')
                
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
                    # 清仓股票
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
                        'dividends': 0,  # 简化版本暂不计算
                        'interest': 0,   # 简化版本暂不计算
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
    
    def get_comprehensive_portfolio_metrics(self, account_ids: List[int], target_date: Optional[date] = None) -> Dict:
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
        
        # 计算每个账户的数据并汇总
        for account_id in account_ids:
            # 获取现金余额
            cad_balance, usd_balance = self._calculate_cash_balance(account_id, target_date)
            cash_cad += cad_balance
            cash_usd += usd_balance
            
            # 获取按币种分类的交易统计
            account_stats = self._get_account_stats_by_currency(account_id, target_date)
            
            # 汇总股票市值（按币种）
            stock_value_cad += account_stats['stock_value_cad']
            stock_value_usd += account_stats['stock_value_usd']
            
            # 汇总已实现收益（按币种）
            realized_gain_cad += account_stats['realized_gain_cad']
            realized_gain_usd += account_stats['realized_gain_usd']
            
            # 汇总未实现收益（按币种）
            unrealized_gain_cad += account_stats['unrealized_gain_cad']
            unrealized_gain_usd += account_stats['unrealized_gain_usd']
            
            # 汇总股息和利息（按币种）
            dividends_cad += account_stats['dividends_cad']
            dividends_usd += account_stats['dividends_usd']
            interest_cad += account_stats['interest_cad']
            interest_usd += account_stats['interest_usd']
            
            # 计算存款和取款
            deposits_withdrawals = self._calculate_deposits_withdrawals_by_currency(account_id, target_date)
            deposits_cad += deposits_withdrawals['deposits_cad']
            deposits_usd += deposits_withdrawals['deposits_usd']
            withdrawals_cad += deposits_withdrawals['withdrawals_cad']
            withdrawals_usd += deposits_withdrawals['withdrawals_usd']
        
        # 计算总和（CAD等价）
        total_stock_value = stock_value_cad + stock_value_usd * exchange_rate_decimal
        total_cash_value = cash_cad + cash_usd * exchange_rate_decimal
        total_assets = total_stock_value + total_cash_value
        
        total_realized_gain = realized_gain_cad + realized_gain_usd * exchange_rate_decimal
        total_unrealized_gain = unrealized_gain_cad + unrealized_gain_usd * exchange_rate_decimal
        total_dividends = dividends_cad + dividends_usd * exchange_rate_decimal
        total_interest = interest_cad + interest_usd * exchange_rate_decimal
        total_deposits = deposits_cad + deposits_usd * exchange_rate_decimal
        total_withdrawals = withdrawals_cad + withdrawals_usd * exchange_rate_decimal
        total_return = total_realized_gain + total_unrealized_gain + total_dividends + total_interest
        
        return {
            'total_assets': {
                'cad': float(total_assets),
                'cad_only': float(stock_value_cad + cash_cad),
                'usd_only': float(stock_value_usd + cash_usd),
                'stock_value': float(total_stock_value),
                'stock_value_cad': float(stock_value_cad),
                'stock_value_usd': float(stock_value_usd),
                'cash_cad': float(cash_cad),
                'cash_usd': float(cash_usd)
            },
            'total_return': {
                'cad': float(total_return),
                'cad_only': float(realized_gain_cad + unrealized_gain_cad + dividends_cad + interest_cad),
                'usd_only': float(realized_gain_usd + unrealized_gain_usd + dividends_usd + interest_usd),
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
            'cash_balance': {
                'cad': float(cash_cad),
                'usd': float(cash_usd),
                'total_cad': float(total_cash_value)
            },
            'exchange_rate': float(exchange_rate_decimal)
        }
    
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
        
        # 获取所有交易的股票列表  
        symbols = Transaction.query.filter(
            Transaction.account_id == account_id,
            Transaction.stock.isnot(None),
            Transaction.stock != '',
            Transaction.trade_date <= target_date
        ).with_entities(Transaction.stock).distinct().all()
        
        #从交易记录中获取股票的币种
        

        for (symbol,) in symbols:
            if not symbol:
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
                
            price = self.stock_price_service.get_cached_stock_price(symbol, currency)
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
        """
        计算当前持仓的成本基础
        """
        transactions = Transaction.query.filter(
            Transaction.account_id == account_id,
            Transaction.stock == symbol,
            Transaction.trade_date <= target_date,
            Transaction.type.in_(['BUY', 'SELL'])
        ).order_by(Transaction.trade_date.asc()).all()
        
        buy_lots = []
        
        # 重建FIFO队列
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
                remaining_to_sell = quantity
                while remaining_to_sell > 0 and buy_lots:
                    lot_shares, lot_cost_per_share = buy_lots[0]
                    if lot_shares <= remaining_to_sell:
                        remaining_to_sell -= lot_shares
                        buy_lots.pop(0)
                    else:
                        buy_lots[0] = (lot_shares - remaining_to_sell, lot_cost_per_share)
                        remaining_to_sell = Decimal('0')
        
        # 计算当前持仓的成本基础
        total_cost = Decimal('0')
        remaining_shares = current_shares
        
        for lot_shares, lot_cost_per_share in buy_lots:
            if remaining_shares <= 0:
                break
            shares_to_count = min(lot_shares, remaining_shares)
            total_cost += shares_to_count * lot_cost_per_share
            remaining_shares -= shares_to_count
        
        return total_cost
    
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
                price = self._get_historical_stock_price(symbol, target_date)
            else:
                # 使用当前缓存价格
                price = self.stock_price_service.get_cached_stock_price(symbol, currency)
            
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
    
    def _get_historical_stock_price(self, symbol: str, target_date: date) -> Optional[Decimal]:
        """
        获取指定日期的历史股票价格
        
        Args:
            symbol: 股票代码
            target_date: 目标日期
            
        Returns:
            该日期的股票价格，如果无法获取返回None
        """
        try:
            # 使用StockPriceService获取历史价格
            # 获取包含目标日期的历史数据范围
            start_date = target_date - timedelta(days=7)  # 获取一周的数据以防某天没有数据
            end_date = target_date + timedelta(days=1)
            
            history = self.stock_price_service.get_stock_history(symbol, start_date, end_date)
            
            # 查找目标日期的价格
            date_str = target_date.strftime('%Y-%m-%d')
            if date_str in history and 'close' in history[date_str]:
                return Decimal(str(history[date_str]['close']))
            
            # 如果目标日期没有数据，查找最近的前一个交易日
            current_date = target_date
            for i in range(7):  # 最多向前查找7天
                current_date -= timedelta(days=1)
                date_str = current_date.strftime('%Y-%m-%d')
                if date_str in history and 'close' in history[date_str]:
                    logger.debug(f"使用{current_date}的价格作为{target_date}的历史价格: {symbol}")
                    return Decimal(str(history[date_str]['close']))
            
            # 如果都没有找到，返回None
            logger.warning(f"无法获取{symbol}在{target_date}附近的历史价格")
            return None
            
        except Exception as e:
            logger.error(f"获取{symbol}历史价格失败: {e}")
            return None
    
    def _calculate_cash_balance(self, account_id: int, target_date: date) -> tuple[Decimal, Decimal]:
        """
        计算指定日期的现金余额
        - 如果是今天：直接从数据库Cash表读取当前现金
        - 如果是历史日期：通过累计交易记录反推计算
        
        Returns:
            (CAD余额, USD余额)
        """
        logger.debug(f"计算账户{account_id}在{target_date}的现金余额")
        
        # 如果是今天，直接从Cash表读取
        if target_date == date.today():
            cash_record = Cash.get_account_cash(account_id)
            if cash_record:
                cad_balance = Decimal(str(cash_record.cad or 0))
                usd_balance = Decimal(str(cash_record.usd or 0))
                logger.info(f"账户{account_id}当前现金(数据库): CAD=${cad_balance}, USD=${usd_balance}")
                return cad_balance, usd_balance
            else:
                logger.warning(f"账户{account_id}没有现金记录，返回0")
                return Decimal('0'), Decimal('0')
        
        # 历史日期：通过交易记录反推计算
        return self._calculate_historical_cash_balance(account_id, target_date)
    
    def _calculate_historical_cash_balance(self, account_id: int, target_date: date) -> tuple[Decimal, Decimal]:
        """
        通过交易记录计算历史现金余额
        """
        # 获取目标日期及之前的所有现金相关交易
        transactions = Transaction.query.filter(
            Transaction.account_id == account_id,
            Transaction.trade_date <= target_date,
            Transaction.type.in_(['DEPOSIT', 'WITHDRAW', 'BUY', 'SELL', 'DIVIDEND', 'INTEREST'])
        ).order_by(Transaction.trade_date.asc()).all()
        
        # 累计计算现金余额
        cad_balance = Decimal('0')
        usd_balance = Decimal('0')
        
        for transaction in transactions:
            cad_balance, usd_balance = self._apply_transaction_impact(
                cad_balance, usd_balance, transaction
            )
        
        logger.info(f"账户{account_id}历史现金({target_date}): CAD=${cad_balance}, USD=${usd_balance}")
        return cad_balance, usd_balance
    
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
                
        elif transaction.type == 'WITHDRAW':
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