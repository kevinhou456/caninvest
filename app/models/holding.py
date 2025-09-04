"""
持仓模型
"""

from datetime import datetime
from app import db

class CurrentHolding(db.Model):
    """当前持仓模型"""
    
    __tablename__ = 'current_holdings'
    
    id = db.Column(db.Integer, primary_key=True)
    account_id = db.Column(db.Integer, db.ForeignKey('accounts.id'), nullable=False, comment='账户ID')
    stock_id = db.Column(db.Integer, db.ForeignKey('stocks.id'), nullable=False, comment='股票ID')
    total_shares = db.Column(db.Numeric(15, 4), nullable=False, default=0, comment='持有股数')
    average_cost = db.Column(db.Numeric(15, 4), nullable=False, default=0, comment='平均成本')
    last_updated = db.Column(db.DateTime, default=datetime.utcnow, comment='最后更新时间')
    
    __table_args__ = (
        db.UniqueConstraint('account_id', 'stock_id', name='uq_account_stock_holding'),
    )
    
    def __repr__(self):
        return f'<CurrentHolding {self.stock.symbol if self.stock else ""} {self.total_shares}>'
    
    def to_dict(self, include_current_price=True):
        result = {
            'id': self.id,
            'account_id': self.account_id,
            'stock_id': self.stock_id,
            'stock': self.stock.to_dict() if self.stock else None,
            'total_shares': float(self.total_shares),
            'average_cost': float(self.average_cost),
            'cost_value': float(self.cost_value),
            'last_updated': self.last_updated.isoformat()
        }
        
        if include_current_price:
            current_price = self.current_price
            result.update({
                'current_price': float(current_price) if current_price else None,
                'current_value': float(self.current_value) if current_price else None,
                'unrealized_gain': float(self.unrealized_gain) if current_price else None,
                'unrealized_gain_percent': self.unrealized_gain_percent if current_price else None
            })
        
        return result
    
    @property
    def cost_value(self):
        """成本价值"""
        return self.total_shares * self.average_cost
    
    @property
    def current_price(self):
        """当前价格"""
        if self.stock:
            latest_price_data = self.stock.get_latest_price()
            return latest_price_data['price'] if latest_price_data else None
        return None
    
    @property
    def current_value(self):
        """当前市值"""
        current_price = self.current_price
        if current_price:
            return self.total_shares * current_price
        return None
    
    @property
    def unrealized_gain(self):
        """未实现收益"""
        current_value = self.current_value
        if current_value is not None:
            return current_value - self.cost_value
        return None
    
    @property
    def unrealized_gain_percent(self):
        """未实现收益率"""
        unrealized_gain = self.unrealized_gain
        if unrealized_gain is not None and self.cost_value > 0:
            return (unrealized_gain / self.cost_value) * 100
        return None
    
    @staticmethod
    def get_holdings_by_account(account_id):
        """按账户获取持仓"""
        return CurrentHolding.query.filter_by(account_id=account_id).filter(
            CurrentHolding.total_shares > 0
        ).all()
    
    @staticmethod
    def get_holdings_by_member(member_id):
        """按成员获取持仓（考虑出资比例）"""
        from app.models.account import AccountMember
        
        holdings = db.session.query(CurrentHolding, AccountMember).join(
            AccountMember, CurrentHolding.account_id == AccountMember.account_id
        ).filter(
            AccountMember.member_id == member_id,
            CurrentHolding.total_shares > 0
        ).all()
        
        result = []
        for holding, account_member in holdings:
            holding_data = holding.to_dict()
            ownership_ratio = account_member.ownership_percentage / 100.0
            
            # 按出资比例调整持仓
            holding_data.update({
                'effective_shares': float(holding.total_shares * ownership_ratio),
                'effective_cost_value': float(holding.cost_value * ownership_ratio),
                'effective_current_value': float(holding.current_value * ownership_ratio) if holding.current_value else None,
                'effective_unrealized_gain': float(holding.unrealized_gain * ownership_ratio) if holding.unrealized_gain else None,
                'ownership_percentage': float(account_member.ownership_percentage)
            })
            
            result.append(holding_data)
        
        return result
    
    @staticmethod
    def get_holdings_by_category(category_id, account_id=None, member_id=None):
        """按分类获取持仓"""
        query = db.session.query(CurrentHolding).join(
            CurrentHolding.stock
        ).filter(
            Stock.category_id == category_id,
            CurrentHolding.total_shares > 0
        )
        
        if account_id:
            query = query.filter(CurrentHolding.account_id == account_id)
        
        if member_id:
            from app.models.account import AccountMember
            query = query.join(
                AccountMember, CurrentHolding.account_id == AccountMember.account_id
            ).filter(AccountMember.member_id == member_id)
        
        return query.all()
    
    @staticmethod
    def recalculate_holding(account_id, stock_id):
        """重新计算指定股票的持仓"""
        from app.models.transaction import Transaction
        
        # 获取所有相关交易
        transactions = Transaction.query.filter_by(
            account_id=account_id,
            stock_id=stock_id
        ).order_by(Transaction.transaction_date).all()
        
        if not transactions:
            # 没有交易，删除持仓记录
            holding = CurrentHolding.query.filter_by(
                account_id=account_id,
                stock_id=stock_id
            ).first()
            if holding:
                db.session.delete(holding)
            return None
        
        # 重新计算持仓
        total_shares = 0
        total_cost = 0
        
        for transaction in transactions:
            if transaction.transaction_type == 'BUY':
                total_shares += transaction.quantity
                total_cost += transaction.net_amount
            elif transaction.transaction_type == 'SELL':
                # 按比例减少成本
                if total_shares > 0:
                    cost_per_share = total_cost / total_shares
                    sold_cost = transaction.quantity * cost_per_share
                    total_cost -= sold_cost
                    total_shares -= transaction.quantity
        
        # 更新或创建持仓记录
        holding = CurrentHolding.query.filter_by(
            account_id=account_id,
            stock_id=stock_id
        ).first()
        
        if total_shares > 0:
            if not holding:
                holding = CurrentHolding(
                    account_id=account_id,
                    stock_id=stock_id
                )
                db.session.add(holding)
            
            holding.total_shares = total_shares
            holding.average_cost = total_cost / total_shares if total_shares > 0 else 0
            holding.last_updated = datetime.utcnow()
        else:
            # 持仓为0，删除记录
            if holding:
                db.session.delete(holding)
        
        return holding
    
    @staticmethod
    def get_portfolio_summary(account_ids=None, member_id=None):
        """获取投资组合摘要"""
        query = CurrentHolding.query.filter(CurrentHolding.total_shares > 0)
        
        if account_ids:
            query = query.filter(CurrentHolding.account_id.in_(account_ids))
        
        if member_id:
            from app.models.account import AccountMember
            query = query.join(
                AccountMember, CurrentHolding.account_id == AccountMember.account_id
            ).filter(AccountMember.member_id == member_id)
        
        holdings = query.all()
        
        summary = {
            'total_cost_value': 0,
            'total_current_value': 0,
            'total_unrealized_gain': 0,
            'holding_count': len(holdings),
            'categories': {},
            'currencies': {'CAD': 0, 'USD': 0},
            'top_holdings': []
        }
        
        for holding in holdings:
            cost_value = holding.cost_value
            current_value = holding.current_value or cost_value
            
            summary['total_cost_value'] += cost_value
            summary['total_current_value'] += current_value
            summary['total_unrealized_gain'] += (current_value - cost_value)
            
            # 按分类统计
            if holding.stock and holding.stock.category:
                category_name = holding.stock.category.name
                if category_name not in summary['categories']:
                    summary['categories'][category_name] = {
                        'cost_value': 0,
                        'current_value': 0,
                        'count': 0
                    }
                summary['categories'][category_name]['cost_value'] += cost_value
                summary['categories'][category_name]['current_value'] += current_value
                summary['categories'][category_name]['count'] += 1
            
            # 按货币统计
            if holding.stock:
                currency = holding.stock.currency
                if currency in summary['currencies']:
                    summary['currencies'][currency] += current_value
            
            # 收集持仓数据用于排序
            summary['top_holdings'].append({
                'holding': holding.to_dict(),
                'current_value': current_value
            })
        
        # 按当前价值排序，取前10
        summary['top_holdings'].sort(key=lambda x: x['current_value'], reverse=True)
        summary['top_holdings'] = [h['holding'] for h in summary['top_holdings'][:10]]
        
        # 计算总收益率
        if summary['total_cost_value'] > 0:
            summary['total_unrealized_gain_percent'] = (
                summary['total_unrealized_gain'] / summary['total_cost_value'] * 100
            )
        else:
            summary['total_unrealized_gain_percent'] = 0
        
        return summary