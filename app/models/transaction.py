"""
交易记录模型
"""

from datetime import datetime
from app import db

class Transaction(db.Model):
    """交易记录模型"""
    
    __tablename__ = 'transactions'
    
    id = db.Column(db.Integer, primary_key=True)
    account_id = db.Column(db.Integer, db.ForeignKey('accounts.id'), nullable=False, comment='账户ID')
    stock_id = db.Column(db.Integer, db.ForeignKey('stocks.id'), nullable=False, comment='股票ID')
    member_id = db.Column(db.Integer, db.ForeignKey('members.id'), comment='操作成员ID')
    transaction_type = db.Column(db.String(10), nullable=False, comment='交易类型: BUY/SELL')
    quantity = db.Column(db.Numeric(15, 4), nullable=False, comment='数量')
    price_per_share = db.Column(db.Numeric(15, 4), nullable=False, comment='单价')
    transaction_fee = db.Column(db.Numeric(10, 2), default=0, comment='手续费')
    transaction_date = db.Column(db.Date, nullable=False, comment='交易日期')
    exchange_rate = db.Column(db.Numeric(10, 6), comment='汇率')
    notes = db.Column(db.Text, comment='交易备注')
    created_at = db.Column(db.DateTime, default=datetime.utcnow, comment='创建时间')
    updated_at = db.Column(db.DateTime, default=datetime.utcnow, onupdate=datetime.utcnow, comment='更新时间')
    
    def __repr__(self):
        return f'<Transaction {self.transaction_type} {self.quantity} {self.stock.symbol if self.stock else ""}>'
    
    def to_dict(self):
        return {
            'id': self.id,
            'account_id': self.account_id,
            'account': {
                'id': self.account.id,
                'name': self.account.name,
                'currency': self.account.currency
            } if self.account else None,
            'stock_id': self.stock_id,
            'stock': self.stock.to_dict() if self.stock else None,
            'member_id': self.member_id,
            'member': {
                'id': self.member.id,
                'name': self.member.name
            } if self.member else None,
            'transaction_type': self.transaction_type,
            'quantity': float(self.quantity),
            'price_per_share': float(self.price_per_share),
            'transaction_fee': float(self.transaction_fee),
            'total_amount': float(self.total_amount),
            'net_amount': float(self.net_amount),
            'transaction_date': self.transaction_date.isoformat(),
            'exchange_rate': float(self.exchange_rate) if self.exchange_rate else None,
            'notes': self.notes,
            'created_at': self.created_at.isoformat(),
            'updated_at': self.updated_at.isoformat()
        }
    
    @property
    def total_amount(self):
        """交易总金额（不含手续费）"""
        return self.quantity * self.price_per_share
    
    @property
    def net_amount(self):
        """净金额"""
        if self.transaction_type == 'BUY':
            return self.total_amount + self.transaction_fee
        else:  # SELL
            return self.total_amount - self.transaction_fee
    
    @property
    def amount_in_base_currency(self):
        """转换为基础货币的金额"""
        amount = self.net_amount
        if self.exchange_rate and self.exchange_rate != 1:
            amount = amount * self.exchange_rate
        return amount
    
    def update_holdings(self):
        """更新持仓记录"""
        from app.models.holding import CurrentHolding
        
        # 查找或创建持仓记录
        holding = CurrentHolding.query.filter_by(
            account_id=self.account_id,
            stock_id=self.stock_id
        ).first()
        
        if not holding:
            holding = CurrentHolding(
                account_id=self.account_id,
                stock_id=self.stock_id,
                total_shares=0,
                average_cost=0
            )
            db.session.add(holding)
        
        # 更新持仓
        if self.transaction_type == 'BUY':
            # 计算新的平均成本
            old_total_cost = holding.total_shares * holding.average_cost
            new_total_cost = old_total_cost + self.net_amount
            new_total_shares = holding.total_shares + self.quantity
            
            holding.total_shares = new_total_shares
            holding.average_cost = new_total_cost / new_total_shares if new_total_shares > 0 else 0
            
        elif self.transaction_type == 'SELL':
            holding.total_shares = max(0, holding.total_shares - self.quantity)
            # 平均成本保持不变
        
        holding.last_updated = datetime.utcnow()
        
        # 如果持仓为0，可以选择删除记录或保留
        if holding.total_shares == 0:
            # 保留记录但标记为0持仓
            pass
    
    @staticmethod
    def get_transactions_by_account(account_id, start_date=None, end_date=None, limit=None):
        """按账户获取交易记录"""
        query = Transaction.query.filter_by(account_id=account_id)
        
        if start_date:
            query = query.filter(Transaction.transaction_date >= start_date)
        if end_date:
            query = query.filter(Transaction.transaction_date <= end_date)
        
        query = query.order_by(Transaction.transaction_date.desc())
        
        if limit:
            query = query.limit(limit)
        
        return query.all()
    
    @staticmethod
    def get_transactions_by_member(member_id, start_date=None, end_date=None, limit=None):
        """按成员获取交易记录"""
        query = Transaction.query.filter_by(member_id=member_id)
        
        if start_date:
            query = query.filter(Transaction.transaction_date >= start_date)
        if end_date:
            query = query.filter(Transaction.transaction_date <= end_date)
        
        query = query.order_by(Transaction.transaction_date.desc())
        
        if limit:
            query = query.limit(limit)
        
        return query.all()
    
    @staticmethod
    def get_transactions_by_stock(stock_id, start_date=None, end_date=None, limit=None):
        """按股票获取交易记录"""
        query = Transaction.query.filter_by(stock_id=stock_id)
        
        if start_date:
            query = query.filter(Transaction.transaction_date >= start_date)
        if end_date:
            query = query.filter(Transaction.transaction_date <= end_date)
        
        query = query.order_by(Transaction.transaction_date.desc())
        
        if limit:
            query = query.limit(limit)
        
        return query.all()
    
    @staticmethod
    def calculate_realized_gain(account_id, stock_id=None):
        """计算已实现收益"""
        query = Transaction.query.filter_by(account_id=account_id)
        if stock_id:
            query = query.filter_by(stock_id=stock_id)
        
        transactions = query.order_by(Transaction.transaction_date).all()
        
        # 简化的FIFO计算
        holdings_queue = []  # [(quantity, price)]
        realized_gain = 0
        
        for transaction in transactions:
            if transaction.transaction_type == 'BUY':
                holdings_queue.append((transaction.quantity, transaction.price_per_share))
            
            elif transaction.transaction_type == 'SELL':
                remaining_to_sell = transaction.quantity
                
                while remaining_to_sell > 0 and holdings_queue:
                    held_quantity, held_price = holdings_queue.pop(0)
                    
                    if held_quantity <= remaining_to_sell:
                        # 全部卖出这批持仓
                        gain = held_quantity * (transaction.price_per_share - held_price)
                        realized_gain += gain
                        remaining_to_sell -= held_quantity
                    else:
                        # 部分卖出
                        gain = remaining_to_sell * (transaction.price_per_share - held_price)
                        realized_gain += gain
                        holdings_queue.insert(0, (held_quantity - remaining_to_sell, held_price))
                        remaining_to_sell = 0
        
        return realized_gain