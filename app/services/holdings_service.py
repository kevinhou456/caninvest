"""
持仓计算服务
提供通用的持仓计算功能，支持按账户、成员、时间点的灵活查询
"""

from datetime import datetime, date
from typing import Dict, List, Optional, Union, Tuple
from decimal import Decimal, ROUND_HALF_UP
from collections import defaultdict
import logging

from app import db
from app.models.transaction import Transaction
from app.models.account import Account, AccountMember
from app.models.member import Member
from app.models.stocks_cache import StocksCache
from app.services.currency_service import currency_service

logger = logging.getLogger(__name__)


class AccountHolding:
    """单个账户的单只股票持仓信息"""
    
    def __init__(self, account_id: int, symbol: str, currency: str = 'USD'):
        self.account_id = account_id
        self.symbol = symbol
        self.currency = currency
        
        # 持仓基础信息
        self.current_shares = Decimal('0')
        self.average_cost = Decimal('0')
        self.total_cost = Decimal('0')
        
        # 交易统计
        self.total_bought_shares = Decimal('0')
        self.total_sold_shares = Decimal('0')
        self.total_bought_value = Decimal('0')
        self.total_sold_value = Decimal('0')
        
        # 收益信息
        self.realized_gain = Decimal('0')
        self.unrealized_gain = Decimal('0')
        self.unrealized_gain_percent = Decimal('0')
        
        # 当前市值信息
        self.current_price = Decimal('0')
        self.current_value = Decimal('0')
        
        # 分红信息
        self.total_dividends = Decimal('0')
        
        # 内部使用的买入批次（FIFO计算）
        self._buy_lots: List[Dict] = []
    
    def add_buy_transaction(self, quantity: Decimal, price: Decimal, fee: Decimal, trade_date: date):
        """添加买入交易"""
        net_cost = quantity * price + fee
        cost_per_share = net_cost / quantity if quantity > 0 else Decimal('0')
        
        # 更新持仓
        self.current_shares += quantity
        self.total_cost += net_cost
        
        # 更新统计
        self.total_bought_shares += quantity
        self.total_bought_value += net_cost
        
        # 添加到买入批次
        self._buy_lots.append({
            'quantity': quantity,
            'cost_per_share': cost_per_share,
            'total_cost': net_cost,
            'trade_date': trade_date
        })
        
        # 重新计算平均成本
        self._recalculate_average_cost()
    
    def add_sell_transaction(self, quantity: Decimal, price: Decimal, fee: Decimal, trade_date: date):
        """添加卖出交易"""
        net_proceeds = quantity * price - fee
        
        # 更新持仓
        self.current_shares -= quantity
        
        # 更新统计
        self.total_sold_shares += quantity
        self.total_sold_value += net_proceeds
        
        # FIFO方式计算已实现收益
        remaining_to_sell = quantity
        cost_basis = Decimal('0')
        
        while remaining_to_sell > 0 and self._buy_lots:
            lot = self._buy_lots[0]
            
            if lot['quantity'] <= remaining_to_sell:
                # 完全消耗这个批次
                sold_from_lot = lot['quantity']
                cost_from_lot = lot['total_cost']
                
                remaining_to_sell -= sold_from_lot
                cost_basis += cost_from_lot
                self.total_cost -= cost_from_lot
                
                self._buy_lots.pop(0)
            else:
                # 部分消耗这个批次
                sold_from_lot = remaining_to_sell
                cost_per_share = lot['cost_per_share']
                cost_from_lot = sold_from_lot * cost_per_share
                
                cost_basis += cost_from_lot
                self.total_cost -= cost_from_lot
                
                # 更新批次
                lot['quantity'] -= sold_from_lot
                lot['total_cost'] -= cost_from_lot
                
                remaining_to_sell = Decimal('0')
        
        # 计算这次交易的已实现收益
        trade_realized_gain = net_proceeds - cost_basis
        self.realized_gain += trade_realized_gain
        
        # 重新计算平均成本
        self._recalculate_average_cost()
    
    def add_dividend(self, amount: Decimal):
        """添加分红"""
        self.total_dividends += amount
    
    def set_current_price(self, price: Decimal):
        """设置当前价格并计算未实现收益"""
        self.current_price = price
        self.current_value = self.current_shares * price
        
        if self.current_shares > 0:
            self.unrealized_gain = self.current_value - self.total_cost
            if self.total_cost > 0:
                self.unrealized_gain_percent = (self.unrealized_gain / self.total_cost) * 100
    
    def _recalculate_average_cost(self):
        """重新计算平均成本"""
        if self.current_shares > 0:
            self.average_cost = self.total_cost / self.current_shares
        else:
            self.average_cost = Decimal('0')
    
    def to_dict(self) -> Dict:
        """转换为字典格式"""
        return {
            'account_id': self.account_id,
            'symbol': self.symbol,
            'currency': self.currency,
            'current_shares': float(self.current_shares),
            'average_cost': float(self.average_cost),
            'total_cost': float(self.total_cost),
            'current_price': float(self.current_price),
            'current_value': float(self.current_value),
            'total_bought_shares': float(self.total_bought_shares),
            'total_sold_shares': float(self.total_sold_shares),
            'total_bought_value': float(self.total_bought_value),
            'total_sold_value': float(self.total_sold_value),
            'realized_gain': float(self.realized_gain),
            'unrealized_gain': float(self.unrealized_gain),
            'unrealized_gain_percent': float(self.unrealized_gain_percent),
            'total_dividends': float(self.total_dividends)
        }


class HoldingsSnapshot:
    """某个时间点的持仓快照"""
    
    def __init__(self, as_of_date: date, account_ids: List[int] = None):
        self.as_of_date = as_of_date
        self.account_ids = account_ids or []
        self.holdings: Dict[str, Dict[int, AccountHolding]] = {}  # {symbol: {account_id: holding}}
        self.account_summaries: Dict[int, Dict] = {}
        self.total_summary: Dict = {}
    
    def add_holding(self, holding: AccountHolding):
        """添加持仓记录"""
        if holding.symbol not in self.holdings:
            self.holdings[holding.symbol] = {}
        
        self.holdings[holding.symbol][holding.account_id] = holding
    
    def get_stock_total_holding(self, symbol: str) -> Dict:
        """获取某只股票在所有账户中的总持仓"""
        if symbol not in self.holdings:
            return {}
        
        total = {
            'symbol': symbol,
            'current_shares': Decimal('0'),
            'total_cost': Decimal('0'),
            'current_value': Decimal('0'),
            'realized_gain': Decimal('0'),
            'unrealized_gain': Decimal('0'),
            'total_dividends': Decimal('0'),
            'accounts': []
        }
        
        for account_id, holding in self.holdings[symbol].items():
            total['current_shares'] += holding.current_shares
            total['total_cost'] += holding.total_cost
            total['current_value'] += holding.current_value
            total['realized_gain'] += holding.realized_gain
            total['unrealized_gain'] += holding.unrealized_gain
            total['total_dividends'] += holding.total_dividends
            
            if holding.current_shares > 0:
                total['accounts'].append({
                    'account_id': account_id,
                    'shares': float(holding.current_shares),
                    'cost': float(holding.total_cost)
                })
        
        # 计算加权平均成本
        if total['current_shares'] > 0:
            total['average_cost'] = total['total_cost'] / total['current_shares']
            total['unrealized_gain_percent'] = (total['unrealized_gain'] / total['total_cost'] * 100) if total['total_cost'] > 0 else Decimal('0')
        else:
            total['average_cost'] = Decimal('0')
            total['unrealized_gain_percent'] = Decimal('0')
        
        return {k: float(v) if isinstance(v, Decimal) else v for k, v in total.items()}
    
    def get_all_symbols(self) -> List[str]:
        """获取所有股票代码"""
        return list(self.holdings.keys())
    
    def get_account_holdings(self, account_id: int) -> List[AccountHolding]:
        """获取某个账户的所有持仓"""
        account_holdings = []
        for symbol_holdings in self.holdings.values():
            if account_id in symbol_holdings:
                account_holdings.append(symbol_holdings[account_id])
        return account_holdings


class HoldingsService:
    """持仓计算服务"""
    
    def __init__(self, *, auto_refresh_prices: bool = False):
        # 控制是否在读取缓存价格时触发外部刷新
        self.auto_refresh_prices = auto_refresh_prices
    
    def get_holdings_snapshot(self, 
                            target: Union[int, List[int], str] = 'all',
                            target_type: str = 'account',
                            as_of_date: Optional[date] = None,
                            family_id: Optional[int] = None) -> HoldingsSnapshot:
        """
        获取持仓快照
        
        Args:
            target: 目标对象
                - 如果是 'all'：获取所有账户
                - 如果是 int：单个账户ID或成员ID
                - 如果是 List[int]：多个账户ID或成员ID
            target_type: 目标类型 ('account' 或 'member')
            as_of_date: 截止日期，None表示当前时间
            family_id: 家庭ID，用于筛选范围
            
        Returns:
            HoldingsSnapshot: 持仓快照对象
        """
        if as_of_date is None:
            as_of_date = date.today()
        
        # 直接计算，不使用缓存
        
        # 获取目标账户列表
        account_ids = self._resolve_target_accounts(target, target_type, family_id)
        
        if not account_ids:
            return HoldingsSnapshot(as_of_date, [])
        
        # 创建快照对象
        snapshot = HoldingsSnapshot(as_of_date, account_ids)
        
        # 按账户计算持仓
        for account_id in account_ids:
            account_holdings = self._calculate_account_holdings(account_id, as_of_date)
            for holding in account_holdings.values():
                snapshot.add_holding(holding)
        
        return snapshot
    
    def _resolve_target_accounts(self, target: Union[int, List[int], str], 
                               target_type: str, family_id: Optional[int]) -> List[int]:
        """解析目标对象为账户ID列表"""
        if target == 'all':
            # 获取所有账户
            query = Account.query
            if family_id:
                query = query.filter_by(family_id=family_id)
            return [acc.id for acc in query.all()]
        
        elif target_type == 'account':
            # 直接是账户ID
            if isinstance(target, int):
                return [target]
            elif isinstance(target, list):
                return target
        
        elif target_type == 'member':
            # 成员ID，需要查找其账户
            if isinstance(target, int):
                member_ids = [target]
            elif isinstance(target, list):
                member_ids = target
            else:
                return []
            
            # 查找成员的账户
            account_members = AccountMember.query.filter(
                AccountMember.member_id.in_(member_ids)
            ).all()
            
            account_ids = [am.account_id for am in account_members]
            
            # 如果指定了family_id，进一步筛选
            if family_id and account_ids:
                accounts = Account.query.filter(
                    Account.id.in_(account_ids),
                    Account.family_id == family_id
                ).all()
                return [acc.id for acc in accounts]
            
            return account_ids
        
        return []
    
    def _calculate_account_holdings(self, account_id: int, as_of_date: date) -> Dict[str, AccountHolding]:
        """计算单个账户的持仓"""
        # 获取该账户在截止日期前的所有交易
        transactions = Transaction.query.filter(
            Transaction.account_id == account_id,
            Transaction.trade_date <= as_of_date,
            Transaction.stock.is_not(None)  # 只处理股票交易
        ).order_by(Transaction.trade_date.asc()).all()
        
        holdings = {}
        
        for tx in transactions:
            if tx.stock not in holdings:
                # 使用交易记录中的实际货币信息
                currency = tx.currency if tx.currency else 'USD'
                holdings[tx.stock] = AccountHolding(account_id, tx.stock, currency)
            
            holding = holdings[tx.stock]
            
            if tx.type == 'BUY':
                holding.add_buy_transaction(
                    quantity=Decimal(str(tx.quantity)),
                    price=Decimal(str(tx.price)),
                    fee=Decimal(str(tx.fee or 0)),
                    trade_date=tx.trade_date
                )
            elif tx.type == 'SELL':
                holding.add_sell_transaction(
                    quantity=Decimal(str(tx.quantity)),
                    price=Decimal(str(tx.price)),
                    fee=Decimal(str(tx.fee or 0)),
                    trade_date=tx.trade_date
                )
            elif tx.type == 'DIVIDEND':
                # 处理分红
                dividend_amount = Decimal(str(tx.quantity * tx.price))
                holding.add_dividend(dividend_amount)
        
        # 设置当前价格
        for symbol, holding in holdings.items():
            current_price = self._get_current_stock_price(symbol, holding.currency)
            if current_price:
                holding.set_current_price(current_price)
        
        # 只返回有持仓的股票
        return {symbol: holding for symbol, holding in holdings.items() 
                if holding.current_shares > 0}
    
    def _get_current_stock_price(self, symbol: str, currency: str) -> Optional[Decimal]:
        """获取股票当前价格 - 使用统一的缓存机制"""
        try:
            from app.services.stock_price_service import StockPriceService
            from app.models import StocksCache
            
            price_service = StockPriceService()

            
            #所有股票价格都是用stock price service获取的，所以货币就是传入的货币
                 
            
            price = price_service.get_cached_stock_price(
                symbol,
                currency,
                auto_refresh=self.auto_refresh_prices
            )

            return price if price > 0 else None
        except Exception as e:
            print(f"Failed to get stock price for {symbol} ({currency}): {e}")
            return None
    
    def get_portfolio_summary(self, 
                            target: Union[int, List[int], str] = 'all',
                            target_type: str = 'account',
                            as_of_date: Optional[date] = None,
                            family_id: Optional[int] = None) -> Dict:
        """
        获取投资组合汇总信息
        
        Returns:
            Dict: 包含总体统计和按股票分组的持仓信息
        """
        snapshot = self.get_holdings_snapshot(target, target_type, as_of_date, family_id)
        
        # 汇总统计
        total_current_value = Decimal('0')
        total_cost = Decimal('0')
        total_realized_gain = Decimal('0')
        total_unrealized_gain = Decimal('0')
        total_dividends = Decimal('0')
        
        # 按股票分组的持仓
        stock_holdings = []
        cleared_holdings = []
        
        for symbol in snapshot.get_all_symbols():
            stock_total = snapshot.get_stock_total_holding(symbol)
            
            if stock_total['current_shares'] > 0:
                stock_holdings.append(stock_total)
                
                total_current_value += Decimal(str(stock_total['current_value']))
                total_cost += Decimal(str(stock_total['total_cost']))
                total_realized_gain += Decimal(str(stock_total['realized_gain']))
                total_unrealized_gain += Decimal(str(stock_total['unrealized_gain']))
                total_dividends += Decimal(str(stock_total['total_dividends']))
        
        # 获取清仓股票信息 - 查找已经完全卖出的股票
        cleared_holdings = self._get_cleared_holdings(target, target_type, as_of_date, family_id)
        
        return {
            'as_of_date': as_of_date.isoformat() if as_of_date else date.today().isoformat(),
            'total_summary': {
                'total_current_value': float(total_current_value),
                'total_cost': float(total_cost),
                'total_realized_gain': float(total_realized_gain),
                'total_unrealized_gain': float(total_unrealized_gain),
                'total_return': float(total_realized_gain + total_unrealized_gain),
                'total_dividends': float(total_dividends),
                'return_percentage': float((total_realized_gain + total_unrealized_gain) / total_cost * 100) if total_cost > 0 else 0
            },
            'holdings': stock_holdings,
            'cleared_holdings': cleared_holdings,
            'account_count': len(snapshot.account_ids)
        }
    
    def _get_cleared_holdings(self, target: Union[int, List[int], str], 
                             target_type: str, as_of_date: Optional[date],
                             family_id: Optional[int]) -> List[Dict]:
        """获取清仓股票信息"""
        try:
            # 获取目标账户列表
            account_ids = self._resolve_target_accounts(target, target_type, family_id)
            
            if not account_ids:
                return []
            
            # 查找所有历史交易过的股票 - 排除空股票符号
            historical_stocks = db.session.query(Transaction.stock).filter(
                Transaction.account_id.in_(account_ids),
                Transaction.trade_date <= as_of_date,
                Transaction.stock.isnot(None),
                Transaction.stock != '',  # 明确排除空字符串
                Transaction.type.in_(['BUY', 'SELL'])
            ).distinct().all()
            
            cleared_holdings = []
            
            for (stock_symbol,) in historical_stocks:
                # 计算该股票的持仓
                stock_holdings = {}
                for account_id in account_ids:
                    account_holding = self._calculate_single_stock_holding(
                        account_id, stock_symbol, as_of_date
                    )
                    if account_holding:
                        stock_holdings[account_id] = account_holding
                
                # 检查是否已清仓（当前持股为0但有交易历史）
                total_current_shares = sum(
                    holding.current_shares for holding in stock_holdings.values()
                )
                
                if total_current_shares == 0:  # 已清仓
                    total_realized_gain = sum(
                        holding.realized_gain for holding in stock_holdings.values()
                    )
                    total_dividends = sum(
                        holding.total_dividends for holding in stock_holdings.values()
                    )
                    
                    # 计算总收益率
                    total_bought_value = sum(
                        holding.total_bought_value for holding in stock_holdings.values()
                    )
                    total_sold_value = sum(
                        holding.total_sold_value for holding in stock_holdings.values()
                    )
                    
                    realized_gain_percent = 0
                    if total_bought_value > 0:
                        realized_gain_percent = (total_realized_gain / total_bought_value) * 100
                    
                    # 获取币种信息
                    currency = 'USD'  # 默认值
                    if stock_holdings:
                        currency = list(stock_holdings.values())[0].currency
                    
                    cleared_holdings.append({
                        'symbol': stock_symbol,
                        'currency': currency,
                        'total_bought_value': float(total_bought_value),
                        'total_sold_value': float(total_sold_value),
                        'realized_gain': float(total_realized_gain),
                        'realized_gain_percent': float(realized_gain_percent),
                        'total_dividends': float(total_dividends),
                        'total_return': float(total_realized_gain + total_dividends)
                    })
            
            return cleared_holdings
            
        except Exception as e:
            logger.error(f"Error getting cleared holdings: {e}")
            return []
    
    def _calculate_single_stock_holding(self, account_id: int, stock_symbol: str, 
                                       as_of_date: date) -> Optional[AccountHolding]:
        """计算单个账户单只股票的持仓"""
        try:
            transactions = Transaction.query.filter(
                Transaction.account_id == account_id,
                Transaction.stock == stock_symbol,
                Transaction.trade_date <= as_of_date
            ).order_by(Transaction.trade_date.asc()).all()
            
            if not transactions:
                return None
            
            # 使用交易记录中的实际货币信息
            currency = transactions[0].currency if transactions[0].currency else 'USD'
            holding = AccountHolding(account_id, stock_symbol, currency)
            
            for tx in transactions:
                if tx.type == 'BUY':
                    holding.add_buy_transaction(
                        quantity=Decimal(str(tx.quantity)),
                        price=Decimal(str(tx.price)),
                        fee=Decimal(str(tx.fee or 0)),
                        trade_date=tx.trade_date
                    )
                elif tx.type == 'SELL':
                    holding.add_sell_transaction(
                        quantity=Decimal(str(tx.quantity)),
                        price=Decimal(str(tx.price)),
                        fee=Decimal(str(tx.fee or 0)),
                        trade_date=tx.trade_date
                    )
                elif tx.type == 'DIVIDEND':
                    dividend_amount = Decimal(str(tx.quantity * tx.price))
                    holding.add_dividend(dividend_amount)
            
            return holding
            
        except Exception as e:
            logger.error(f"Error calculating single stock holding for {stock_symbol}: {e}")
            return None
    
    def clear_cache(self):
        """清除缓存（无操作 - 不再使用缓存）"""
        pass


# 全局服务实例
holdings_service = HoldingsService()
