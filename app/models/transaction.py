"""
交易记录模型
"""

from datetime import datetime
from app import db

class Transaction(db.Model):
    """交易记录模型"""
    
    __tablename__ = 'transactions'
    
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
    def is_duplicate(cls, account_id, trade_date, type, stock, quantity, price, currency, fee, notes=''):
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
        # 检查是否存在完全相同的记录
        existing = cls.query.filter_by(
            account_id=account_id,
            trade_date=trade_date,
            type=type,
            stock=stock,
            quantity=quantity,
            price=price,
            currency=currency,
            fee=fee,
            notes=notes
        ).first()
        
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
                'name': self.account.name,
                'currency': self.account.currency
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
        """获取投资组合摘要"""
        query = cls.query
        if account_ids:
            # 支持多个账户ID的过滤
            query = query.filter(cls.account_id.in_(account_ids))
        elif account_id:
            # 向后兼容单个账户ID
            query = query.filter_by(account_id=account_id)
        
        transactions = query.all()
        portfolio = {}
        
        for tx in transactions:
            if tx.stock not in portfolio:
                portfolio[tx.stock] = {
                    'symbol': tx.stock,
                    'currency': tx.currency,
                    'total_shares': 0,
                    'total_cost': 0,
                    'total_bought_shares': 0,
                    'total_sold_shares': 0,
                    'total_bought_value': 0,
                    'total_sold_value': 0,
                    'realized_gain': 0,
                    'transactions': 0
                }
            
            if tx.type == 'BUY':
                portfolio[tx.stock]['total_shares'] += float(tx.quantity)
                portfolio[tx.stock]['total_cost'] += tx.net_amount
                portfolio[tx.stock]['total_bought_shares'] += float(tx.quantity)
                portfolio[tx.stock]['total_bought_value'] += tx.net_amount
            else:  # SELL
                portfolio[tx.stock]['total_shares'] -= float(tx.quantity)
                portfolio[tx.stock]['total_cost'] -= tx.net_amount
                portfolio[tx.stock]['total_sold_shares'] += float(tx.quantity)
                portfolio[tx.stock]['total_sold_value'] += tx.net_amount
                
                # 计算已实现收益（基于FIFO或平均成本）
                if portfolio[tx.stock]['total_bought_shares'] > 0:
                    avg_cost_per_share = portfolio[tx.stock]['total_bought_value'] / portfolio[tx.stock]['total_bought_shares']
                    cost_of_sold = avg_cost_per_share * float(tx.quantity)
                    portfolio[tx.stock]['realized_gain'] += tx.net_amount - cost_of_sold
            
            portfolio[tx.stock]['transactions'] += 1
        
        # 计算平均成本和已实现收益
        for stock_data in portfolio.values():
            if stock_data['total_shares'] > 0:
                stock_data['average_cost'] = stock_data['total_cost'] / stock_data['total_shares']
            else:
                stock_data['average_cost'] = 0
                
            # 如果是已清仓股票，重新计算已实现收益
            if stock_data['total_shares'] == 0 and stock_data['total_sold_shares'] > 0:
                stock_data['realized_gain'] = stock_data['total_sold_value'] - stock_data['total_bought_value']
        
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