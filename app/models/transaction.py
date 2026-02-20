"""
交易记录模型
"""

from datetime import datetime
from app import db

class Transaction(db.Model):
    """交易记录模型"""
    
    __tablename__ = 'transactions'
    __table_args__ = (
        db.Index('idx_transactions_account_trade_date_id', 'account_id', 'trade_date', 'id'),
    )
    
    id = db.Column(db.Integer, primary_key=True)
    trade_date = db.Column(db.Date, nullable=False, comment='交易日期')
    type = db.Column(db.String(20), nullable=False, comment='交易类型: BUY/SELL/DIVIDEND/INTEREST/DEPOSIT/WITHDRAWAL/FEE')
    stock = db.Column(db.String(20), nullable=True, comment='股票代码')
    quantity = db.Column(db.Numeric(15, 4), nullable=False, comment='交易数量')
    price = db.Column(db.Numeric(15, 4), nullable=False, comment='单股价格')
    amount = db.Column(db.Numeric(15, 2), nullable=True, comment='总金额 - 用于存入/取出/分红/利息等非股票交易')
    currency = db.Column(db.String(3), nullable=False, comment='货币类型: USD/CAD')
    fee = db.Column(db.Numeric(10, 2), default=0, comment='交易手续费')
    account_id = db.Column(db.Integer, db.ForeignKey('accounts.id'), nullable=False, comment='账户ID')
    notes = db.Column(db.Text, comment='交易备注')
    created_at = db.Column(db.DateTime, default=datetime.utcnow, comment='创建时间')
    updated_at = db.Column(db.DateTime, default=datetime.utcnow, onupdate=datetime.utcnow, comment='更新时间')
    
    account = db.relationship('Account', back_populates='transactions')
    
    def __repr__(self):
        return f'<Transaction {self.type} {self.stock} {self.quantity}@{self.price}>'
    
    @classmethod
    def is_duplicate(cls, account_id, trade_date, type, stock, quantity, price, currency, fee, notes='', amount=None):
        """
        检查是否存在重复的交易记录

        Args:
            account_id: 账户ID
            trade_date: 交易日期
            type: 交易类型
            stock: 股票代码
            quantity: 数量
            price: 价格
            currency: 货币
            fee: 手续费
            notes: 备注

        Returns:
            bool: True if duplicate exists, False otherwise
        """
        # 对于存入/取出交易，使用不同的重复检测逻辑
        if type in ['DEPOSIT', 'WITHDRAWAL']:
            # 存入/取出交易检查amount字段而不是quantity，notes允许不同
            existing = cls.query.filter_by(
                account_id=account_id,
                trade_date=trade_date,
                type=type,
                amount=amount,
                currency=currency,
                fee=fee
            ).first()

            if existing:
                pass
        else:
            # 股票买卖交易检查所有关键字段，但notes可以不同
            existing = cls.query.filter_by(
                account_id=account_id,
                trade_date=trade_date,
                type=type,
                stock=stock,
                quantity=quantity,
                price=price,
                currency=currency,
                fee=fee
            ).first()

            if existing:
                pass

        return existing is not None
    
    @classmethod
    def count_same_day_trades(cls, account_id, trade_date, type, stock, quantity, price):
        """
        计算同一天相同交易的数量（用于调试和统计）
        
        Returns:
            int: 同一天相同交易的数量
        """
        count = cls.query.filter_by(
            account_id=account_id,
            trade_date=trade_date,
            type=type,
            stock=stock,
            quantity=quantity,
            price=price
        ).count()
        
        return count
    
    def to_dict(self):
        return {
            'id': self.id,
            'trade_date': self.trade_date.isoformat(),
            'type': self.type,
            'stock': self.stock,
            'quantity': float(self.quantity),
            'price': float(self.price),
            'currency': self.currency,
            'fee': float(self.fee),
            'account_id': self.account_id,
            'account': {
                'id': self.account.id,
                'name': self.account.name
            } if self.account else None,
            'notes': self.notes,
            'total_amount': self.total_amount,
            'net_amount': self.net_amount,
            'created_at': self.created_at.isoformat(),
            'updated_at': self.updated_at.isoformat()
        }
    
    @property
    def total_amount(self):
        """交易总金额（不含手续费）"""
        return float(self.quantity * self.price)
    
    @property
    def net_amount(self):
        """净金额（含手续费）"""
        if self.type == 'BUY':
            return self.total_amount + float(self.fee)
        else:  # SELL
            return self.total_amount - float(self.fee)
    
    
    @classmethod
    def get_by_account(cls, account_id, limit=None):
        """按账户ID获取交易记录"""
        query = cls.query.filter_by(account_id=account_id).order_by(cls.trade_date.desc())
        if limit:
            query = query.limit(limit)
        return query.all()
    
    @classmethod
    def get_by_stock(cls, stock_symbol, limit=None):
        """按股票代码获取交易记录"""
        query = cls.query.filter_by(stock=stock_symbol).order_by(cls.trade_date.desc())
        if limit:
            query = query.limit(limit)
        return query.all()
    
    @classmethod
    def get_by_date_range(cls, start_date=None, end_date=None, limit=None):
        """按日期范围获取交易记录"""
        query = cls.query
        if start_date:
            query = query.filter(cls.trade_date >= start_date)
        if end_date:
            query = query.filter(cls.trade_date <= end_date)
        
        query = query.order_by(cls.trade_date.desc())
        if limit:
            query = query.limit(limit)
        return query.all()
    
    @classmethod
    def get_portfolio_summary(cls, account_id=None, account_ids=None):
        """获取投资组合摘要 - 修复持仓计算逻辑"""
        query = cls.query
        if account_ids:
            # 支持多个账户ID的过滤
            query = query.filter(cls.account_id.in_(account_ids))
        elif account_id:
            # 向后兼容单个账户ID
            query = query.filter_by(account_id=account_id)
        
        # 按日期排序，确保FIFO计算的准确性
        transactions = query.order_by(cls.trade_date.asc()).all()
        portfolio = {}
        
        for tx in transactions:
            if tx.stock not in portfolio:
                portfolio[tx.stock] = {
                    'symbol': tx.stock,
                    'currency': tx.currency,
                    'total_shares': 0,
                    'total_cost': 0,  # 当前持仓的总成本
                    'total_bought_shares': 0,
                    'total_sold_shares': 0,
                    'total_bought_value': 0,
                    'total_sold_value': 0,
                    'realized_gain': 0,
                    'transactions': 0,
                    'buy_lots': []  # 存储买入批次，用于FIFO计算
                }
            
            stock_data = portfolio[tx.stock]
            
            if tx.type == 'BUY':
                # 买入交易
                stock_data['total_shares'] += float(tx.quantity)
                stock_data['total_cost'] += tx.net_amount
                stock_data['total_bought_shares'] += float(tx.quantity)
                stock_data['total_bought_value'] += tx.net_amount
                
                # 记录买入批次（用于FIFO计算）
                stock_data['buy_lots'].append({
                    'quantity': float(tx.quantity),
                    'cost_per_share': tx.net_amount / float(tx.quantity) if tx.quantity > 0 else 0,
                    'total_cost': tx.net_amount
                })
                
            elif tx.type == 'SELL':
                # 卖出交易
                stock_data['total_shares'] -= float(tx.quantity)
                stock_data['total_sold_shares'] += float(tx.quantity)
                stock_data['total_sold_value'] += tx.net_amount
                
                # FIFO方式计算已实现收益和调整持仓成本
                remaining_to_sell = float(tx.quantity)
                sell_proceeds = tx.net_amount
                cost_basis = 0
                
                # 从最早的买入批次开始卖出
                while remaining_to_sell > 0 and stock_data['buy_lots']:
                    lot = stock_data['buy_lots'][0]
                    
                    if lot['quantity'] <= remaining_to_sell:
                        # 完全卖出这个批次
                        sold_from_lot = lot['quantity']
                        cost_from_lot = lot['total_cost']
                        
                        remaining_to_sell -= sold_from_lot
                        cost_basis += cost_from_lot
                        
                        # 从持仓成本中减去这个批次的成本
                        stock_data['total_cost'] -= cost_from_lot
                        
                        # 移除这个批次
                        stock_data['buy_lots'].pop(0)
                        
                    else:
                        # 部分卖出这个批次
                        sold_from_lot = remaining_to_sell
                        cost_per_share = lot['cost_per_share']
                        cost_from_lot = sold_from_lot * cost_per_share
                        
                        cost_basis += cost_from_lot
                        
                        # 从持仓成本中减去卖出部分的成本
                        stock_data['total_cost'] -= cost_from_lot
                        
                        # 更新批次剩余数量和成本
                        lot['quantity'] -= sold_from_lot
                        lot['total_cost'] -= cost_from_lot
                        
                        remaining_to_sell = 0
                
                # 计算这次卖出的已实现收益（按比例分配）
                if stock_data['total_sold_shares'] > 0:
                    # 这次卖出的已实现收益 = 卖出收入 - 成本基础
                    trade_realized_gain = sell_proceeds - cost_basis
                    stock_data['realized_gain'] += trade_realized_gain
            
            stock_data['transactions'] += 1
        
        # 计算平均成本
        for stock_data in portfolio.values():
            if stock_data['total_shares'] > 0:
                stock_data['average_cost'] = stock_data['total_cost'] / stock_data['total_shares']
            else:
                stock_data['average_cost'] = 0
                
            # 清理buy_lots字段（不需要返回给前端）
            if 'buy_lots' in stock_data:
                del stock_data['buy_lots']
                
            # 如果是已清仓股票，已实现收益已经在卖出过程中计算完成
            if stock_data['total_shares'] == 0 and stock_data['total_sold_shares'] > 0:
                # 对于完全清仓的股票，确保总成本为0
                stock_data['total_cost'] = 0
        
        return portfolio
    
    @classmethod
    def get_monthly_summary(cls, year=None, month=None):
        """获取月度交易摘要"""
        from sqlalchemy import extract, func
        
        query = cls.query
        if year:
            query = query.filter(extract('year', cls.trade_date) == year)
        if month:
            query = query.filter(extract('month', cls.trade_date) == month)
        
        # 按类型统计
        buy_summary = query.filter_by(type='BUY').with_entities(
            func.count().label('count'),
            func.sum(cls.quantity * cls.price + cls.fee).label('total_amount')
        ).first()
        
        sell_summary = query.filter_by(type='SELL').with_entities(
            func.count().label('count'),
            func.sum(cls.quantity * cls.price - cls.fee).label('total_amount')
        ).first()
        
        return {
            'buy': {
                'count': buy_summary.count or 0,
                'amount': float(buy_summary.total_amount or 0)
            },
            'sell': {
                'count': sell_summary.count or 0,
                'amount': float(sell_summary.total_amount or 0)
            }
        }
    
    @classmethod
    def get_currency_by_stock_symbol(cls, stock_symbol):
        """
        根据股票代码获取币种
        从交易记录中读取该股票代码第一条记录的货币值
        
        Args:
            stock_symbol: 股票代码
            
        Returns:
            str: 货币代码 (USD/CAD)，如果没有找到返回 None
        """
        if not stock_symbol:
            return None
            
        # 查找该股票代码的第一条交易记录（按交易日期升序排列）
        transaction = cls.query.filter_by(stock=stock_symbol).order_by(cls.trade_date.asc()).first()
        
        if transaction:
            return transaction.currency
        else:
            return None
