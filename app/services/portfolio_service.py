"""
投资组合分析和计算服务
"""

from datetime import datetime, date, timedelta
from typing import Dict, List, Optional, Tuple
from decimal import Decimal
from sqlalchemy import func, and_, or_
from app import db
from app.models.family import Family
from app.models.member import Member
from app.models.account import Account, AccountType
from app.models.holding import CurrentHolding
from app.models.transaction import Transaction
from app.models.stock import Stock, StockCategory
from app.models.contribution import Contribution
from app.models.price_cache import StockPriceCache


class PortfolioService:
    """投资组合服务类"""
    
    def __init__(self):
        pass
    
    def calculate_portfolio_performance(self, family_id: int, period_days: int = 365) -> Dict:
        """计算投资组合表现"""
        family = Family.query.get_or_404(family_id)
        start_date = datetime.now().date() - timedelta(days=period_days)
        
        # 获取所有账户
        accounts = family.accounts
        
        performance_data = {
            'family_id': family_id,
            'period_days': period_days,
            'start_date': start_date.isoformat(),
            'end_date': datetime.now().date().isoformat(),
            'total_invested': Decimal('0'),
            'current_value': Decimal('0'),
            'total_gain_loss': Decimal('0'),
            'total_gain_loss_percent': Decimal('0'),
            'realized_gain': Decimal('0'),
            'unrealized_gain': Decimal('0'),
            'dividend_income': Decimal('0'),
            'total_fees': Decimal('0'),
            'account_performance': [],
            'top_performers': [],
            'worst_performers': [],
            'sector_allocation': {},
            'currency_allocation': {},
            'monthly_performance': []
        }
        
        account_performances = []
        all_holdings = []
        
        for account in accounts:
            account_perf = self._calculate_account_performance(account.id, start_date)
            account_performances.append(account_perf)
            
            performance_data['total_invested'] += account_perf['total_invested']
            performance_data['current_value'] += account_perf['current_value']
            performance_data['realized_gain'] += account_perf['realized_gain']
            performance_data['unrealized_gain'] += account_perf['unrealized_gain']
            performance_data['dividend_income'] += account_perf['dividend_income']
            performance_data['total_fees'] += account_perf['total_fees']
            
            # 收集所有持仓
            all_holdings.extend(account.holdings.filter(CurrentHolding.total_shares > 0).all())
        
        performance_data['account_performance'] = account_performances
        
        # 计算总收益率
        if performance_data['total_invested'] > 0:
            performance_data['total_gain_loss'] = performance_data['current_value'] - performance_data['total_invested']
            performance_data['total_gain_loss_percent'] = (
                performance_data['total_gain_loss'] / performance_data['total_invested']
            ) * 100
        
        # 分析表现最好和最差的股票
        performance_data['top_performers'] = self._get_top_performers(all_holdings, limit=10)
        performance_data['worst_performers'] = self._get_worst_performers(all_holdings, limit=10)
        
        # 行业配置分析
        performance_data['sector_allocation'] = self._calculate_sector_allocation(all_holdings)
        
        # 货币配置分析
        performance_data['currency_allocation'] = self._calculate_currency_allocation(all_holdings)
        
        # 月度表现分析
        performance_data['monthly_performance'] = self._calculate_monthly_performance(family_id, 12)
        
        return performance_data
    
    def _calculate_account_performance(self, account_id: int, start_date: date) -> Dict:
        """计算单个账户表现"""
        account = Account.query.get(account_id)
        
        # 获取期间内的交易
        transactions = Transaction.query.filter(
            and_(
                Transaction.account_id == account_id,
                Transaction.transaction_date >= start_date
            )
        ).all()
        
        # 计算投入资金（买入交易）
        buy_transactions = [t for t in transactions if t.transaction_type == 'BUY']
        total_invested = sum(t.total_amount + t.transaction_fee for t in buy_transactions)
        
        # 计算取出资金（卖出交易）
        sell_transactions = [t for t in transactions if t.transaction_type == 'SELL']
        total_divested = sum(t.total_amount - t.transaction_fee for t in sell_transactions)
        
        # 计算股息收入
        dividend_transactions = [t for t in transactions if t.transaction_type == 'DIVIDEND']
        dividend_income = sum(t.total_amount for t in dividend_transactions)
        
        # 计算总费用
        total_fees = sum(t.transaction_fee for t in transactions)
        
        # 计算已实现收益
        realized_gain = Transaction.calculate_realized_gain(account_id, start_date)
        
        # 获取当前持仓价值
        holdings = CurrentHolding.query.filter(
            and_(
                CurrentHolding.account_id == account_id,
                CurrentHolding.total_shares > 0
            )
        ).all()
        
        current_value = Decimal('0')
        unrealized_gain = Decimal('0')
        
        for holding in holdings:
            holding_value = holding.current_value or holding.cost_value
            current_value += holding_value
            unrealized_gain += holding.unrealized_gain or Decimal('0')
        
        # 净投入资金
        net_invested = total_invested - total_divested
        
        return {
            'account_id': account_id,
            'account_name': account.name,
            'account_type': account.account_type.name if account.account_type else 'Unknown',
            'total_invested': total_invested,
            'total_divested': total_divested,
            'net_invested': net_invested,
            'current_value': current_value,
            'realized_gain': realized_gain,
            'unrealized_gain': unrealized_gain,
            'dividend_income': dividend_income,
            'total_fees': total_fees,
            'total_return': realized_gain + unrealized_gain + dividend_income,
            'return_percent': (realized_gain + unrealized_gain + dividend_income) / net_invested * 100 if net_invested > 0 else Decimal('0'),
            'holdings_count': len(holdings)
        }
    
    def _get_top_performers(self, holdings: List[CurrentHolding], limit: int = 10) -> List[Dict]:
        """获取表现最好的股票"""
        performance_list = []
        
        for holding in holdings:
            if holding.total_shares > 0 and holding.cost_value > 0:
                unrealized_gain_percent = holding.unrealized_gain_percent or Decimal('0')
                performance_list.append({
                    'symbol': holding.stock.symbol,
                    'name': holding.stock.name,
                    'current_value': float(holding.current_value or holding.cost_value),
                    'cost_value': float(holding.cost_value),
                    'unrealized_gain': float(holding.unrealized_gain or Decimal('0')),
                    'unrealized_gain_percent': float(unrealized_gain_percent),
                    'shares': float(holding.total_shares),
                    'category': holding.stock.category.name if holding.stock.category else 'Uncategorized'
                })
        
        # 按收益率排序
        performance_list.sort(key=lambda x: x['unrealized_gain_percent'], reverse=True)
        
        return performance_list[:limit]
    
    def _get_worst_performers(self, holdings: List[CurrentHolding], limit: int = 10) -> List[Dict]:
        """获取表现最差的股票"""
        performance_list = []
        
        for holding in holdings:
            if holding.total_shares > 0 and holding.cost_value > 0:
                unrealized_gain_percent = holding.unrealized_gain_percent or Decimal('0')
                performance_list.append({
                    'symbol': holding.stock.symbol,
                    'name': holding.stock.name,
                    'current_value': float(holding.current_value or holding.cost_value),
                    'cost_value': float(holding.cost_value),
                    'unrealized_gain': float(holding.unrealized_gain or Decimal('0')),
                    'unrealized_gain_percent': float(unrealized_gain_percent),
                    'shares': float(holding.total_shares),
                    'category': holding.stock.category.name if holding.stock.category else 'Uncategorized'
                })
        
        # 按收益率排序（升序，最差的在前面）
        performance_list.sort(key=lambda x: x['unrealized_gain_percent'])
        
        return performance_list[:limit]
    
    def _calculate_sector_allocation(self, holdings: List[CurrentHolding]) -> Dict[str, Dict]:
        """计算行业配置"""
        sector_data = {}
        total_value = Decimal('0')
        
        # 先计算总价值
        for holding in holdings:
            if holding.total_shares > 0:
                holding_value = holding.current_value or holding.cost_value
                total_value += holding_value
        
        # 按行业分组
        for holding in holdings:
            if holding.total_shares > 0:
                category_name = holding.stock.category.name if holding.stock.category else 'Uncategorized'
                holding_value = holding.current_value or holding.cost_value
                
                if category_name not in sector_data:
                    sector_data[category_name] = {
                        'value': Decimal('0'),
                        'cost': Decimal('0'),
                        'count': 0,
                        'percent': Decimal('0')
                    }
                
                sector_data[category_name]['value'] += holding_value
                sector_data[category_name]['cost'] += holding.cost_value
                sector_data[category_name]['count'] += 1
        
        # 计算百分比
        for sector in sector_data.values():
            if total_value > 0:
                sector['percent'] = (sector['value'] / total_value) * 100
            sector['value'] = float(sector['value'])
            sector['cost'] = float(sector['cost'])
            sector['percent'] = float(sector['percent'])
        
        return sector_data
    
    def _calculate_currency_allocation(self, holdings: List[CurrentHolding]) -> Dict[str, Dict]:
        """计算货币配置"""
        currency_data = {}
        total_value = Decimal('0')
        
        # 先计算总价值
        for holding in holdings:
            if holding.total_shares > 0:
                holding_value = holding.current_value or holding.cost_value
                total_value += holding_value
        
        # 按货币分组
        for holding in holdings:
            if holding.total_shares > 0:
                currency = holding.stock.currency
                holding_value = holding.current_value or holding.cost_value
                
                if currency not in currency_data:
                    currency_data[currency] = {
                        'value': Decimal('0'),
                        'cost': Decimal('0'),
                        'count': 0,
                        'percent': Decimal('0')
                    }
                
                currency_data[currency]['value'] += holding_value
                currency_data[currency]['cost'] += holding.cost_value
                currency_data[currency]['count'] += 1
        
        # 计算百分比
        for currency in currency_data.values():
            if total_value > 0:
                currency['percent'] = (currency['value'] / total_value) * 100
            currency['value'] = float(currency['value'])
            currency['cost'] = float(currency['cost'])
            currency['percent'] = float(currency['percent'])
        
        return currency_data
    
    def _calculate_monthly_performance(self, family_id: int, months: int = 12) -> List[Dict]:
        """计算月度表现"""
        monthly_data = []
        family = Family.query.get(family_id)
        
        for i in range(months):
            # 计算每个月月底的时间点
            end_date = datetime.now().date().replace(day=1) - timedelta(days=i*30)
            start_date = end_date.replace(day=1)
            
            # 计算该月的投资组合价值和交易活动
            month_transactions = Transaction.query.join(Account).filter(
                and_(
                    Account.family_id == family_id,
                    Transaction.transaction_date >= start_date,
                    Transaction.transaction_date < end_date
                )
            ).all()
            
            # 计算月度交易统计
            buy_volume = sum(t.total_amount for t in month_transactions if t.transaction_type == 'BUY')
            sell_volume = sum(t.total_amount for t in month_transactions if t.transaction_type == 'SELL')
            dividend_income = sum(t.total_amount for t in month_transactions if t.transaction_type == 'DIVIDEND')
            
            monthly_data.append({
                'month': start_date.strftime('%Y-%m'),
                'buy_volume': float(buy_volume),
                'sell_volume': float(sell_volume),
                'dividend_income': float(dividend_income),
                'net_investment': float(buy_volume - sell_volume),
                'transaction_count': len(month_transactions)
            })
        
        # 按时间倒序排列
        monthly_data.reverse()
        return monthly_data
    
    def calculate_risk_metrics(self, family_id: int) -> Dict:
        """计算风险指标"""
        family = Family.query.get_or_404(family_id)
        
        # 获取所有持仓
        holdings = []
        for account in family.accounts:
            holdings.extend(account.holdings.filter(CurrentHolding.total_shares > 0).all())
        
        if not holdings:
            return {'error': 'No holdings found'}
        
        # 计算集中度风险
        concentration_risk = self._calculate_concentration_risk(holdings)
        
        # 计算行业多样性
        sector_diversity = self._calculate_sector_diversity(holdings)
        
        # 计算货币风险
        currency_risk = self._calculate_currency_risk(holdings)
        
        # 计算价格波动性（基于历史数据，简化版本）
        volatility_risk = self._calculate_volatility_risk(holdings)
        
        return {
            'concentration_risk': concentration_risk,
            'sector_diversity': sector_diversity,
            'currency_risk': currency_risk,
            'volatility_risk': volatility_risk,
            'risk_score': self._calculate_overall_risk_score(
                concentration_risk, sector_diversity, currency_risk, volatility_risk
            )
        }
    
    def _calculate_concentration_risk(self, holdings: List[CurrentHolding]) -> Dict:
        """计算集中度风险"""
        total_value = sum(holding.current_value or holding.cost_value for holding in holdings)
        
        if total_value == 0:
            return {'score': 0, 'description': 'No holdings'}
        
        # 计算前5大持仓的占比
        holding_percentages = []
        for holding in holdings:
            holding_value = holding.current_value or holding.cost_value
            percentage = (holding_value / total_value) * 100
            holding_percentages.append(percentage)
        
        holding_percentages.sort(reverse=True)
        top_5_concentration = sum(holding_percentages[:5])
        
        # 评分逻辑
        if top_5_concentration < 50:
            score = 'Low'
            description = 'Well diversified portfolio'
        elif top_5_concentration < 70:
            score = 'Medium'
            description = 'Moderately concentrated'
        else:
            score = 'High'
            description = 'Highly concentrated portfolio'
        
        return {
            'score': score,
            'top_5_concentration': float(top_5_concentration),
            'largest_holding_percent': float(holding_percentages[0]) if holding_percentages else 0,
            'description': description
        }
    
    def _calculate_sector_diversity(self, holdings: List[CurrentHolding]) -> Dict:
        """计算行业多样性"""
        sector_counts = {}
        total_value = sum(holding.current_value or holding.cost_value for holding in holdings)
        
        for holding in holdings:
            category_name = holding.stock.category.name if holding.stock.category else 'Uncategorized'
            holding_value = holding.current_value or holding.cost_value
            
            if category_name not in sector_counts:
                sector_counts[category_name] = Decimal('0')
            
            sector_counts[category_name] += holding_value
        
        # 计算行业数量和分布
        sector_count = len(sector_counts)
        sector_percentages = [(value / total_value) * 100 for value in sector_counts.values()] if total_value > 0 else []
        
        # 计算赫芬达尔指数（HHI）
        hhi = sum(p**2 for p in sector_percentages) / 100 if sector_percentages else 0
        
        # 评分逻辑
        if sector_count >= 8 and hhi < 2000:
            score = 'High'
            description = 'Excellent sector diversification'
        elif sector_count >= 5 and hhi < 3000:
            score = 'Medium'
            description = 'Good sector diversification'
        else:
            score = 'Low'
            description = 'Limited sector diversification'
        
        return {
            'score': score,
            'sector_count': sector_count,
            'herfindahl_index': float(hhi),
            'description': description,
            'largest_sector_percent': float(max(sector_percentages)) if sector_percentages else 0
        }
    
    def _calculate_currency_risk(self, holdings: List[CurrentHolding]) -> Dict:
        """计算货币风险"""
        currency_exposure = {}
        total_value = sum(holding.current_value or holding.cost_value for holding in holdings)
        
        for holding in holdings:
            currency = holding.stock.currency
            holding_value = holding.current_value or holding.cost_value
            
            if currency not in currency_exposure:
                currency_exposure[currency] = Decimal('0')
            
            currency_exposure[currency] += holding_value
        
        # 计算货币分布
        currency_percentages = {}
        for currency, value in currency_exposure.items():
            currency_percentages[currency] = (value / total_value) * 100 if total_value > 0 else 0
        
        # 对于加拿大投资者，CAD是本币
        cad_exposure = float(currency_percentages.get('CAD', 0))
        usd_exposure = float(currency_percentages.get('USD', 0))
        other_exposure = 100 - cad_exposure - usd_exposure
        
        # 评分逻辑
        if other_exposure > 30:
            score = 'High'
            description = 'Significant exposure to foreign currencies'
        elif usd_exposure > 50:
            score = 'Medium'
            description = 'Moderate US dollar exposure'
        else:
            score = 'Low'
            description = 'Low currency risk'
        
        return {
            'score': score,
            'cad_exposure': cad_exposure,
            'usd_exposure': usd_exposure,
            'other_exposure': other_exposure,
            'currency_count': len(currency_exposure),
            'description': description
        }
    
    def _calculate_volatility_risk(self, holdings: List[CurrentHolding]) -> Dict:
        """计算波动性风险（简化版本）"""
        # 这里简化处理，实际应该基于历史价格数据计算
        volatile_categories = ['Technology', 'Healthcare', 'Energy']
        stable_categories = ['Utilities', 'Consumer Goods', 'Banking']
        
        total_value = sum(holding.current_value or holding.cost_value for holding in holdings)
        volatile_exposure = Decimal('0')
        stable_exposure = Decimal('0')
        
        for holding in holdings:
            holding_value = holding.current_value or holding.cost_value
            category_name = holding.stock.category.name if holding.stock.category else 'Unknown'
            
            if category_name in volatile_categories:
                volatile_exposure += holding_value
            elif category_name in stable_categories:
                stable_exposure += holding_value
        
        volatile_percent = (volatile_exposure / total_value) * 100 if total_value > 0 else 0
        stable_percent = (stable_exposure / total_value) * 100 if total_value > 0 else 0
        
        # 评分逻辑
        if volatile_percent > 60:
            score = 'High'
            description = 'High exposure to volatile sectors'
        elif volatile_percent > 40:
            score = 'Medium'
            description = 'Moderate volatility exposure'
        else:
            score = 'Low'
            description = 'Conservative portfolio allocation'
        
        return {
            'score': score,
            'volatile_exposure_percent': float(volatile_percent),
            'stable_exposure_percent': float(stable_percent),
            'description': description
        }
    
    def _calculate_overall_risk_score(self, concentration, sector, currency, volatility) -> Dict:
        """计算总体风险评分"""
        risk_scores = {
            'High': 3,
            'Medium': 2,
            'Low': 1
        }
        
        total_score = (
            risk_scores.get(concentration['score'], 2) +
            (4 - risk_scores.get(sector['score'], 2)) +  # 多样性越高风险越低
            risk_scores.get(currency['score'], 2) +
            risk_scores.get(volatility['score'], 2)
        ) / 4
        
        if total_score >= 2.5:
            overall_score = 'High'
            description = 'Portfolio has significant risk factors'
            recommendation = 'Consider diversification and risk management'
        elif total_score >= 1.5:
            overall_score = 'Medium'
            description = 'Portfolio has moderate risk profile'
            recommendation = 'Monitor risk factors and consider gradual adjustments'
        else:
            overall_score = 'Low'
            description = 'Conservative portfolio with low risk'
            recommendation = 'Well-balanced portfolio for risk-averse investors'
        
        return {
            'score': overall_score,
            'numeric_score': float(total_score),
            'description': description,
            'recommendation': recommendation
        }
    
    def generate_rebalancing_suggestions(self, family_id: int, target_allocation: Dict[str, float] = None) -> Dict:
        """生成再平衡建议"""
        family = Family.query.get_or_404(family_id)
        
        # 默认目标配置（可以自定义）
        if target_allocation is None:
            target_allocation = {
                'Technology': 20.0,
                'Banking': 15.0,
                'Index Funds': 30.0,
                'Healthcare': 10.0,
                'Energy': 10.0,
                'Real Estate': 5.0,
                'Consumer Goods': 5.0,
                'Others': 5.0
            }
        
        # 获取当前配置
        all_holdings = []
        for account in family.accounts:
            all_holdings.extend(account.holdings.filter(CurrentHolding.total_shares > 0).all())
        
        current_allocation = self._calculate_sector_allocation(all_holdings)
        
        # 计算差异和建议
        suggestions = []
        total_value = sum(holding.current_value or holding.cost_value for holding in all_holdings)
        
        for sector, target_percent in target_allocation.items():
            current_percent = current_allocation.get(sector, {}).get('percent', 0)
            difference = target_percent - current_percent
            
            if abs(difference) > 2:  # 只对差异大于2%的情况提供建议
                target_value = (target_percent / 100) * total_value
                current_value = current_allocation.get(sector, {}).get('value', 0)
                adjustment_value = target_value - current_value
                
                action = 'Buy' if adjustment_value > 0 else 'Sell'
                
                suggestions.append({
                    'sector': sector,
                    'current_percent': float(current_percent),
                    'target_percent': target_percent,
                    'difference_percent': float(difference),
                    'adjustment_value': float(abs(adjustment_value)),
                    'action': action,
                    'priority': 'High' if abs(difference) > 5 else 'Medium'
                })
        
        # 按优先级和差异大小排序
        suggestions.sort(key=lambda x: (x['priority'] == 'High', abs(x['difference_percent'])), reverse=True)
        
        return {
            'family_id': family_id,
            'total_portfolio_value': float(total_value),
            'target_allocation': target_allocation,
            'current_allocation': {k: v['percent'] for k, v in current_allocation.items()},
            'suggestions': suggestions,
            'rebalancing_needed': len(suggestions) > 0
        }