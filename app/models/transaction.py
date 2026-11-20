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
        # 注意：不要随意给 transactions 加索引。代码里有不少不带 ORDER BY 的查询（如个股详情页），
        # 新索引会改变 SQLite 返回行的顺序，从而改变页面显示顺序。
    )
    
    id = db.Column(db.Integer, primary_key=True)
    trade_date = db.Column(db.Date, nullable=False, comment='交易日期')
    type = db.Column(db.String(20), nullable=False, comment='交易类型: BUY/SELL/DIVIDEND/INTEREST/DEPOSIT/WITHDRAWAL/FEE/TAX')
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
            query = cls.query.filter_by(
                account_id=account_id,
                trade_date=trade_date,
                type=type,
                stock=stock,
                quantity=quantity,
                price=price,
                currency=currency,
                fee=fee
            )
            if type in ['DIVIDEND', 'INTEREST', 'FEE', 'TAX']:
                query = query.filter_by(amount=amount)
            existing = query.first()

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
            'amount': float(self.amount) if self.amount is not None else None,
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
            
        # 查找该股票代码的第一条交易记录（按交易日期升序排列）；同一请求内每个代码只查一次
        from app.utils.query_memo import memo

        def _load():
            transaction = cls.query.filter_by(stock=stock_symbol).order_by(cls.trade_date.asc()).first()
            return transaction.currency if transaction else None

        return memo('currency_by_symbol', stock_symbol, _load)
