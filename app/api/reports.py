"""
报告和分析API
"""

import hashlib
import json
from typing import List, Optional, Tuple
from flask import request, jsonify
from flask_babel import _
from datetime import datetime, timedelta, date
from decimal import Decimal
from sqlalchemy import func, tuple_
from app import db
from app.models.family import Family
from app.models.member import Member
from app.models.account import Account, AccountMember, AccountType
from app.services.account_service import AccountService
from app.services.portfolio_service import PortfolioService, TimePeriod
# from app.models.holding import CurrentHolding  # CurrentHolding model deleted
from app.models.transaction import Transaction
from app.models.contribution import Contribution
from app.models.cash import Cash
from app.models.stocks_cache import StocksCache
from app.models.stock_price_history import StockPriceHistory
from app.models.report_analysis_cache import ReportAnalysisCache
from app.services.currency_service import currency_service
from app.services.currency_service import ExchangeRate
from . import bp



def _resolve_account_ids(family_id, member_id=None, account_id=None, account_type=None):
    if account_type:
        account_type = account_type.strip().upper()
    if account_id:
        account = Account.query.get_or_404(account_id)
        if account.family_id != family_id:
            return [], None
        if account_type and (not account.account_type or account.account_type.name.upper() != account_type):
            return [], None
        return [account_id], None

    if member_id:
        member = Member.query.get_or_404(member_id)
        if member.family_id != family_id:
            return [], None
        query = Account.query.join(AccountMember).filter(
            AccountMember.member_id == member.id,
            Account.family_id == family_id
        )
        if account_type:
            query = query.join(AccountType).filter(func.upper(AccountType.name) == account_type)
        accounts = query.all()
        account_ids = [acc.id for acc in accounts]
        ownership_map = {am.account_id: Decimal(str(am.ownership_percentage or 0)) / Decimal('100')
                         for am in AccountMember.query.filter_by(member_id=member.id).all()}
        # prune ownership_map to filtered accounts
        if account_ids:
            ownership_map = {aid: ownership_map.get(aid, Decimal('0')) for aid in account_ids}
        return account_ids, ownership_map

    query = Account.query.filter(Account.family_id == family_id)
    if account_type:
        query = query.join(AccountType).filter(func.upper(AccountType.name) == account_type)
    accounts = query.all()
    return [acc.id for acc in accounts], None

def apply_member_ownership_proportions(analysis_data, member_id):
    """
    应用成员所有权比例到分析数据
    当汇总成员账户时，对于共享账户需要按照持股比例计算
    """
    account_memberships = AccountMember.query.filter_by(member_id=member_id).all()
    
    # 创建账户到所有权比例的映射
    ownership_map = {}
    for am in account_memberships:
        ownership_map[am.account_id] = float(am.ownership_percentage) / 100.0
    
    def apply_proportion_to_value(value, account_id):
        """对单个数值应用比例"""
        if account_id in ownership_map:
            proportion = ownership_map[account_id]
            return value * proportion if value is not None else None
        return value
    
    def apply_proportion_to_dict(data_dict, account_id_key='account_id'):
        """对字典中的数值应用比例"""
        if not isinstance(data_dict, dict):
            return data_dict
            
        result = data_dict.copy()
        account_id = result.get(account_id_key)
        
        if account_id in ownership_map:
            proportion = ownership_map[account_id]
            
            # 需要按比例计算的字段
            proportional_fields = [
                'net_deposits', 'net_withdrawals', 'dividends_received', 
                'interest_received', 'fees_paid', 'taxes_paid',
                'period_start_value', 'period_end_value', 'net_gain_loss',
                'total_return', 'current_value', 'total_cost', 'unrealized_gain',
                'realized_gain', 'total_gain', 'market_value', 'book_value'
            ]
            
            for field in proportional_fields:
                if field in result and result[field] is not None:
                    try:
                        result[field] = float(result[field]) * proportion
                    except (ValueError, TypeError):
                        pass
        
        return result
    
    # 应用比例到不同层级的数据
    if 'yearly_data' in analysis_data:
        for year_data in analysis_data['yearly_data']:
            if 'accounts' in year_data:
                year_data['accounts'] = [
                    apply_proportion_to_dict(acc_data) 
                    for acc_data in year_data['accounts']
                ]
            
            # 重新计算年度汇总数据
            if 'accounts' in year_data:
                accounts = year_data['accounts']
                year_data['summary'] = _recalculate_summary(accounts)
    
    if 'quarterly_data' in analysis_data:
        for quarter_data in analysis_data['quarterly_data']:
            if 'accounts' in quarter_data:
                quarter_data['accounts'] = [
                    apply_proportion_to_dict(acc_data) 
                    for acc_data in quarter_data['accounts']
                ]
            if 'accounts' in quarter_data:
                accounts = quarter_data['accounts']
                quarter_data['summary'] = _recalculate_summary(accounts)
    
    if 'monthly_data' in analysis_data:
        for month_data in analysis_data['monthly_data']:
            if 'accounts' in month_data:
                month_data['accounts'] = [
                    apply_proportion_to_dict(acc_data) 
                    for acc_data in month_data['accounts']
                ]
            if 'accounts' in month_data:
                accounts = month_data['accounts']
                month_data['summary'] = _recalculate_summary(accounts)
    
    if 'daily_data' in analysis_data:
        for daily_data_item in analysis_data['daily_data']:
            if 'accounts' in daily_data_item:
                daily_data_item['accounts'] = [
                    apply_proportion_to_dict(acc_data) 
                    for acc_data in daily_data_item['accounts']
                ]
            if 'accounts' in daily_data_item:
                accounts = daily_data_item['accounts']
                daily_data_item['summary'] = _recalculate_summary(accounts)
    
    # 对于持仓分布数据
    if 'holdings' in analysis_data:
        for holding in analysis_data['holdings']:
            account_id = holding.get('account_id')
            if account_id in ownership_map:
                proportion = ownership_map[account_id]
                for field in ['current_value', 'total_cost', 'unrealized_gain', 'quantity', 'average_cost']:
                    if field in holding and holding[field] is not None:
                        try:
                            holding[field] = float(holding[field]) * proportion
                        except (ValueError, TypeError):
                            pass
    
    return analysis_data


def _normalize_cache_params(params: dict) -> dict:
    normalized = {}
    for key, value in (params or {}).items():
        if isinstance(value, list):
            normalized[key] = sorted(value)
        else:
            normalized[key] = value
    return normalized


# 每次修改 portfolio_service 计算逻辑时手动递增，使所有旧缓存自动失效
_CACHE_SCHEMA_VERSION = 6


def _build_analysis_cache_key(cache_type: str, family_id: int, member_id: Optional[int], account_id: Optional[int],
                              account_ids: List[int], params: dict) -> Tuple[str, dict]:
    payload = {
        'v': _CACHE_SCHEMA_VERSION,
        'cache_type': cache_type,
        'family_id': family_id,
        'member_id': member_id,
        'account_id': account_id,
        'account_ids': sorted(set(account_ids or [])),
        'params': _normalize_cache_params(params or {})
    }
    raw = json.dumps(payload, sort_keys=True, separators=(',', ':'))
    return hashlib.sha1(raw.encode('utf-8')).hexdigest(), payload


def _get_symbol_pairs(account_ids: List[int]) -> List[Tuple[str, str]]:
    rows = db.session.query(Transaction.stock, Transaction.currency).filter(
        Transaction.account_id.in_(account_ids),
        Transaction.stock.isnot(None),
        Transaction.stock != ''
    ).distinct().all()
    pairs = []
    for symbol, currency in rows:
        if not symbol:
            continue
        pairs.append((symbol.upper(), (currency or 'USD').upper()))
    return pairs


def _get_latest_symbol_updates(symbol_pairs: List[Tuple[str, str]]):
    if not symbol_pairs:
        return None, None
    max_history = db.session.query(func.max(StockPriceHistory.updated_at)).filter(
        tuple_(StockPriceHistory.symbol, StockPriceHistory.currency).in_(symbol_pairs)
    ).scalar()
    max_price = db.session.query(func.max(StocksCache.price_updated_at)).filter(
        tuple_(StocksCache.symbol, StocksCache.currency).in_(symbol_pairs)
    ).scalar()
    return max_history, max_price


def _get_latest_exchange_rate_update(cache_type: str):
    query = ExchangeRate.query.filter(
        ExchangeRate.from_currency == 'USD',
        ExchangeRate.to_currency == 'CAD'
    )
    if cache_type == 'annual':
        query = query.filter(ExchangeRate.source == 'ANNUAL_AVERAGE')
    else:
        query = query.filter(ExchangeRate.source == 'API')
    return query.with_entities(func.max(ExchangeRate.created_at)).scalar()


def _is_analysis_cache_fresh(cache: ReportAnalysisCache, account_ids: List[int], cache_type: str) -> bool:
    if not cache:
        return False
    cache_time = cache.updated_at or cache.created_at
    if not cache_time:
        return False

    max_tx = db.session.query(func.max(Transaction.updated_at)).filter(
        Transaction.account_id.in_(account_ids)
    ).scalar()
    if max_tx and max_tx > cache_time:
        return False

    max_cash = db.session.query(func.max(Cash.updated_at)).filter(
        Cash.account_id.in_(account_ids)
    ).scalar()
    if max_cash and max_cash > cache_time:
        return False

    symbol_pairs = _get_symbol_pairs(account_ids)
    max_history, max_price = _get_latest_symbol_updates(symbol_pairs)
    if max_history and max_history > cache_time:
        return False
    if max_price and max_price > cache_time:
        return False

    max_rate = _get_latest_exchange_rate_update(cache_type)
    if max_rate and max_rate > cache_time:
        return False

    return True


def invalidate_report_cache_for_account(account_id: int):
    """删除所有包含指定账户的 ReportAnalysisCache 记录。
    用于交易记录被删除时（max updated_at 不会变化，无法靠时间戳自动失效）。
    account_ids_json 格式为排好序的整数列表，如 [1,3,7]，匹配方式：
    完整匹配 [id]、开头 [id,、结尾 ,id]、中间 ,id,
    """
    aid = str(account_id)
    ReportAnalysisCache.query.filter(
        db.or_(
            ReportAnalysisCache.account_id == account_id,
            ReportAnalysisCache.account_ids_json == f'[{aid}]',
            ReportAnalysisCache.account_ids_json.like(f'[{aid},%'),
            ReportAnalysisCache.account_ids_json.like(f'%,{aid}]'),
            ReportAnalysisCache.account_ids_json.like(f'%,{aid},%'),
        )
    ).delete(synchronize_session=False)
    db.session.commit()


def _dump_cache_payload(payload: dict) -> str:
    return json.dumps(payload, ensure_ascii=False, default=str)


def _get_cached_analysis(cache_type: str, family_id: int, member_id: Optional[int], account_id: Optional[int],
                         account_ids: List[int], params: dict):
    cache_key, payload = _build_analysis_cache_key(
        cache_type, family_id, member_id, account_id, account_ids, params
    )
    cache = ReportAnalysisCache.query.filter_by(
        cache_type=cache_type,
        cache_key=cache_key
    ).first()
    if cache and _is_analysis_cache_fresh(cache, account_ids, cache_type):
        try:
            return json.loads(cache.data_json), cache, cache_key, payload
        except Exception:
            pass
    return None, cache, cache_key, payload


def _store_analysis_cache(cache_type: str, family_id: int, member_id: Optional[int], account_id: Optional[int],
                          account_ids: List[int], params: dict, analysis_data: dict,
                          cache: ReportAnalysisCache, cache_key: str):
    account_ids_json = json.dumps(sorted(set(account_ids or [])))
    params_json = json.dumps(_normalize_cache_params(params or {}), ensure_ascii=False)
    data_json = _dump_cache_payload(analysis_data)
    now = datetime.utcnow()

    if cache is None:
        cache = ReportAnalysisCache(
            cache_type=cache_type,
            cache_key=cache_key,
            family_id=family_id,
            member_id=member_id,
            account_id=account_id,
            account_ids_json=account_ids_json,
            params_json=params_json,
            data_json=data_json,
            created_at=now,
            updated_at=now,
        )
        db.session.add(cache)
    else:
        cache.family_id = family_id
        cache.member_id = member_id
        cache.account_id = account_id
        cache.account_ids_json = account_ids_json
        cache.params_json = params_json
        cache.data_json = data_json
        cache.updated_at = now  # 显式设置，不依赖 SQLAlchemy onupdate
    try:
        db.session.commit()
    except Exception:
        db.session.rollback()


def _recalculate_summary(accounts):
    """重新计算汇总数据"""
    summary = {
        'net_deposits': 0,
        'net_withdrawals': 0,
        'dividends_received': 0,
        'interest_received': 0,
        'fees_paid': 0,
        'taxes_paid': 0,
        'period_start_value': 0,
        'period_end_value': 0,
        'net_gain_loss': 0,
        'total_return': 0
    }
    
    for acc in accounts:
        for field in summary.keys():
            if field in acc and acc[field] is not None:
                try:
                    summary[field] += float(acc[field])
                except (ValueError, TypeError):
                    pass
    
    # 计算收益率
    if summary['period_start_value'] > 0:
        summary['return_rate'] = (summary['net_gain_loss'] / summary['period_start_value']) * 100
    else:
        summary['return_rate'] = 0
    
    return summary

@bp.route('/families/<int:family_id>/reports/portfolio', methods=['GET'])
def get_family_portfolio_report(family_id):
    """获取家庭投资组合报告"""
    family = Family.query.get_or_404(family_id)
    
    # 获取时间范围参数
    start_date = request.args.get('start_date')
    end_date = request.args.get('end_date')
    group_by = request.args.get('group_by', 'account')  # account, member, category, currency
    
    if start_date:
        start_date = datetime.strptime(start_date, '%Y-%m-%d').date()
    if end_date:
        end_date = datetime.strptime(end_date, '%Y-%m-%d').date()
    
    # 获取基础数据
    portfolio_summary = family.get_portfolio_summary()
    
    # 按账户分组的持仓
    accounts = AccountService.get_accounts_display_list(family.id)
    accounts_data = []
    for account in accounts:
        holdings_summary = account.get_holdings_summary()
        accounts_data.append({
            'account': account.to_dict(),
            'holdings_summary': holdings_summary
        })
    
    # 按成员分组的持仓
    members_data = []
    for member in family.members:
        member_summary = member.get_portfolio_summary()
        # member_holdings = CurrentHolding.get_holdings_by_member(member.id)  # Temporarily disabled
        member_holdings = []  # TODO: Re-implement with new holding system
        members_data.append({
            'member': member.to_dict(),
            'portfolio_summary': member_summary,
            'holdings': member_holdings
        })
    
    # 按分类分组的持仓
    categories_data = {}
    for account in accounts:
        for holding in account.holdings:
            if holding.total_shares > 0 and holding.stock and holding.stock.category:
                category = holding.stock.category
                category_name = category.get_localized_name()
                
                if category_name not in categories_data:
                    categories_data[category_name] = {
                        'category': category.to_dict(),
                        'total_cost': 0,
                        'total_current_value': 0,
                        'holdings_count': 0,
                        'stocks': []
                    }
                
                current_value = holding.current_value or holding.cost_value
                categories_data[category_name]['total_cost'] += holding.cost_value
                categories_data[category_name]['total_current_value'] += current_value
                categories_data[category_name]['holdings_count'] += 1
                categories_data[category_name]['stocks'].append(holding.to_dict())
    
    # 计算分类收益率
    for category_data in categories_data.values():
        if category_data['total_cost'] > 0:
            category_data['unrealized_gain'] = category_data['total_current_value'] - category_data['total_cost']
            category_data['unrealized_gain_percent'] = (category_data['unrealized_gain'] / category_data['total_cost']) * 100
        else:
            category_data['unrealized_gain'] = 0
            category_data['unrealized_gain_percent'] = 0
    
    return jsonify({
        'family': family.to_dict(),
        'portfolio_summary': portfolio_summary,
        'by_account': accounts_data,
        'by_member': members_data,
        'by_category': list(categories_data.values()),
        'report_parameters': {
            'start_date': start_date.isoformat() if start_date else None,
            'end_date': end_date.isoformat() if end_date else None,
            'group_by': group_by
        }
    })

@bp.route('/accounts/<int:account_id>/reports/performance', methods=['GET'])
def get_account_performance_report(account_id):
    """获取账户表现报告"""
    account = Account.query.get_or_404(account_id)
    
    # 获取参数
    start_date = request.args.get('start_date')
    end_date = request.args.get('end_date')
    period = request.args.get('period', 'monthly')  # daily, weekly, monthly
    
    if not start_date:
        start_date = (datetime.now() - timedelta(days=365)).date()
    else:
        start_date = datetime.strptime(start_date, '%Y-%m-%d').date()
    
    if not end_date:
        end_date = datetime.now().date()
    else:
        end_date = datetime.strptime(end_date, '%Y-%m-%d').date()
    
    # 获取交易历史
    transactions = Transaction.get_transactions_by_account(
        account_id, start_date, end_date
    )
    
    # 获取当前持仓
    holdings_summary = account.get_holdings_summary()
    
    # 计算交易统计
    buy_transactions = [t for t in transactions if t.type == 'BUY']
    sell_transactions = [t for t in transactions if t.type == 'SELL']
    
    total_invested = sum(t.net_amount for t in buy_transactions)
    total_divested = sum(t.net_amount for t in sell_transactions)
    total_fees = sum(t.transaction_fee for t in transactions)
    
    # 已实现收益计算
    realized_gain = Transaction.calculate_realized_gain(account_id)
    
    performance_data = {
        'account': account.to_dict(),
        'period': {
            'start_date': start_date.isoformat(),
            'end_date': end_date.isoformat(),
            'period_type': period
        },
        'summary': {
            'current_value': float(account.current_value or 0),
            'total_cost': float(account.total_cost or 0),
            'unrealized_gain': float(account.unrealized_gain or 0),
            'unrealized_gain_percent': account.unrealized_gain_percent or 0,
            'realized_gain': float(realized_gain),
            'total_invested': float(total_invested),
            'total_divested': float(total_divested),
            'total_fees': float(total_fees),
            'net_cash_flow': float(total_invested - total_divested)
        },
        'transactions': {
            'total_count': len(transactions),
            'buy_count': len(buy_transactions),
            'sell_count': len(sell_transactions),
            'buy_volume': float(sum(t.total_amount for t in buy_transactions)),
            'sell_volume': float(sum(t.total_amount for t in sell_transactions))
        },
        'holdings': holdings_summary,
        'top_performers': [],  # 可以添加表现最好的股票
        'worst_performers': []  # 可以添加表现最差的股票
    }
    
    # 计算股票表现
    stock_performance = {}
    for holding in account.holdings:
        if holding.total_shares > 0:
            stock_performance[holding.stock.symbol] = {
                'symbol': holding.stock.symbol,
                'name': holding.stock.name,
                'current_value': holding.current_value or 0,
                'cost_value': holding.cost_value,
                'unrealized_gain': holding.unrealized_gain or 0,
                'unrealized_gain_percent': holding.unrealized_gain_percent or 0,
                'total_shares': float(holding.total_shares)
            }
    
    # 按收益率排序
    sorted_performance = sorted(
        stock_performance.values(),
        key=lambda x: x['unrealized_gain_percent'],
        reverse=True
    )
    
    performance_data['top_performers'] = sorted_performance[:5]
    performance_data['worst_performers'] = sorted_performance[-5:]
    
    return jsonify(performance_data)

@bp.route('/members/<int:member_id>/reports/contributions', methods=['GET'])
def get_member_contribution_report(member_id):
    """获取成员供款报告"""
    member = Member.query.get_or_404(member_id)
    
    year = request.args.get('year', datetime.now().year, type=int)
    
    # 获取供款摘要
    contribution_summary = Contribution.get_contribution_summary(member_id, year)
    
    # 获取家庭供款摘要（用于对比）
    family_summary = Contribution.get_family_contribution_summary(member.family_id, year)
    
    # 获取历史年度数据
    historical_years = request.args.get('include_history', 'false').lower() == 'true'
    historical_data = {}
    
    if historical_years:
        current_year = datetime.now().year
        for hist_year in range(current_year - 4, current_year + 1):
            historical_data[hist_year] = Contribution.get_contribution_summary(member_id, hist_year)
    
    return jsonify({
        'member': member.to_dict(),
        'year': year,
        'contribution_summary': contribution_summary,
        'family_summary': family_summary,
        'historical_data': historical_data
    })

@bp.route('/families/<int:family_id>/reports/tax-summary', methods=['GET'])
def get_family_tax_summary(family_id):
    """获取家庭税务摘要报告"""
    family = Family.query.get_or_404(family_id)
    
    year = request.args.get('year', datetime.now().year, type=int)
    
    # 获取家庭供款摘要
    family_contributions = Contribution.get_family_contribution_summary(family_id, year)
    
    # 获取已实现收益（用于税务计算）
    realized_gains_by_account = {}
    total_realized_gains = 0
    
    accounts = AccountService.get_accounts_display_list(family.id)
    for account in accounts:
        if account.account_type and account.account_type.tax_advantaged:
            # 税收优惠账户的收益通常不需要报税
            realized_gains_by_account[account.name] = {
                'account_type': account.account_type.name,
                'realized_gain': 0,
                'tax_implications': 'Tax-sheltered'
            }
        else:
            # 普通账户的已实现收益需要报税
            realized_gain = Transaction.calculate_realized_gain(account.id)
            realized_gains_by_account[account.name] = {
                'account_type': 'Regular',
                'realized_gain': float(realized_gain),
                'tax_implications': 'Taxable'
            }
            total_realized_gains += realized_gain
    
    # 股息收入（简化处理，实际应该从交易记录中提取）
    dividend_income = 0  # 这里需要实现股息记录功能
    
    tax_summary = {
        'family': family.to_dict(),
        'year': year,
        'contribution_summary': family_contributions,
        'capital_gains': {
            'total_realized_gains': float(total_realized_gains),
            'by_account': realized_gains_by_account,
            'estimated_tax': float(total_realized_gains * 0.5 * 0.25)  # 简化的税收估算
        },
        'dividend_income': {
            'total_dividends': float(dividend_income),
            'eligible_dividends': float(dividend_income * 0.8),  # 假设80%为合格股息
            'other_dividends': float(dividend_income * 0.2)
        },
        'tax_documents_needed': [
            'T5 - Investment Income',
            'T3 - Trust Income',
            'T5013 - Partnership Income',
            'Schedule 3 - Capital Gains'
        ] if total_realized_gains > 0 or dividend_income > 0 else []
    }
    
    return jsonify(tax_summary)

@bp.route('/reports/market-overview', methods=['GET'])
def get_market_overview():
    """获取市场概览报告"""
    # 这里应该集成外部市场数据API
    # 暂时返回模拟数据
    
    market_data = {
        'last_updated': datetime.now().isoformat(),
        'major_indices': {
            'TSX': {
                'value': 20500.45,
                'change': 125.30,
                'change_percent': 0.62
            },
            'S&P_500': {
                'value': 4456.78,
                'change': -23.45,
                'change_percent': -0.52
            },
            'NASDAQ': {
                'value': 13845.67,
                'change': -89.23,
                'change_percent': -0.64
            }
        },
        'currency_rates': {
            'USD_CAD': {
                'rate': 1.3456,
                'change': 0.0023,
                'change_percent': 0.17
            }
        },
        'sector_performance': {
            'Technology': 1.25,
            'Healthcare': 0.89,
            'Financials': -0.34,
            'Energy': 2.15,
            'Consumer_Discretionary': -0.67
        },
        'market_news': [
            {
                'headline': 'Bank of Canada holds interest rate steady',
                'source': 'Financial Post',
                'timestamp': (datetime.now() - timedelta(hours=2)).isoformat()
            },
            {
                'headline': 'Tech stocks rally on AI optimism',
                'source': 'Globe and Mail',
                'timestamp': (datetime.now() - timedelta(hours=4)).isoformat()
            }
        ]
    }
    
    return jsonify(market_data)

@bp.route('/accounts/<int:account_id>/reports/export', methods=['POST'])
def export_account_report(account_id):
    """导出账户报告"""
    account = Account.query.get_or_404(account_id)
    data = request.get_json()
    
    report_type = data.get('report_type', 'portfolio')  # portfolio, performance, transactions
    format_type = data.get('format', 'pdf')  # pdf, excel, csv
    start_date = data.get('start_date')
    end_date = data.get('end_date')
    
    try:
        # 这里应该调用报告生成服务
        # from app.services.report_service import ReportService
        # 
        # report_service = ReportService()
        # file_path = report_service.generate_account_report(
        #     account_id, report_type, format_type, start_date, end_date
        # )
        
        # 暂时返回成功响应
        filename = f'{account.name}_{report_type}_report.{format_type}'
        
        return jsonify({
            'success': True,
            'message': _('Report generated successfully'),
            'download_url': f'/static/exports/{filename}',
            'filename': filename
        })
        
    except Exception as e:
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500

@bp.route('/families/<int:family_id>/reports/annual-analysis', methods=['GET'])
def get_family_annual_analysis(family_id):
    """获取家庭年度分析报告"""
    family = Family.query.get_or_404(family_id)
    
    # 获取参数
    member_id = request.args.get('member_id', type=int)
    account_id = request.args.get('account_id', type=int)
    account_type = request.args.get('account_type')
    if member_id is not None and member_id <= 0:
        member_id = None
    if account_id is not None and account_id <= 0:
        account_id = None
    years_param = request.args.get('years')
    separate_accounts = request.args.get('separate_accounts', default=False, type=lambda v: str(v).lower() in ('1', 'true', 'yes', 'on'))
    
    # 确定账户范围
    account_ids, _ = _resolve_account_ids(family.id, member_id, account_id, account_type)

    years = None
    if years_param:
        try:
            years = [int(year.strip()) for year in years_param.split(',')]
        except ValueError:
            return jsonify({'error': 'Invalid years parameter format'}), 400
    
    try:
        params = {
            'account_type': account_type,
            'years': years,
            'separate_accounts': separate_accounts
        }
        analysis_data, cache, cache_key, _ = _get_cached_analysis(
            'annual', family_id, member_id, account_id, account_ids, params
        )
        if analysis_data is None:
            # 调用统一的投资组合服务
            from app.services.portfolio_service import portfolio_service
            analysis_data = portfolio_service.get_annual_analysis(
                account_ids,
                years,
                member_id=member_id,
                selected_account_id=account_id,
                include_account_breakdown=separate_accounts
            )
            _store_analysis_cache(
                'annual', family_id, member_id, account_id, account_ids, params, analysis_data, cache, cache_key
            )

        # 获取单账户的类型名称（用于前端判断是否显示T3按钮）
        account_type_name = None
        if account_id:
            _acct = Account.query.get(account_id)
            if _acct and _acct.account_type:
                account_type_name = _acct.account_type.name

        return jsonify({
            'family': family.to_dict(),
            'filter_info': {
                'member_id': member_id,
                'account_id': account_id,
                'account_count': len(account_ids),
                'account_type': account_type,
                'account_type_name': account_type_name,
            },
            'analysis': analysis_data
        })

    except Exception as e:
        return jsonify({
            'success': False,
            'error': f'Failed to generate annual analysis: {str(e)}'
        }), 500


def _compute_t5008_for_accounts(account_ids_list, year_start, year_end, account_name_map):
    """
    对给定账户列表计算T5008记录（合并平均成本法ACB）。
    返回 (records, t3_all_records)。
    """
    from app.models.t3_box42 import T3Box42

    sell_txs = Transaction.query.filter(
        Transaction.account_id.in_(account_ids_list),
        Transaction.type == 'SELL',
        Transaction.trade_date >= year_start,
        Transaction.trade_date <= year_end
    ).order_by(Transaction.trade_date.asc(), Transaction.id.asc()).all()

    if not sell_txs:
        return [], {}

    symbols = list({tx.stock for tx in sell_txs})
    records = []
    t3_all_records = {}

    for symbol in symbols:
        all_txs = Transaction.query.filter(
            Transaction.account_id.in_(account_ids_list),
            Transaction.stock == symbol,
            Transaction.trade_date <= year_end
        ).order_by(Transaction.trade_date.asc(), Transaction.id.asc()).all()

        # 确定该股票的货币，并为 USD 股票预取每日汇率
        symbol_currency = (all_txs[0].currency or 'CAD').upper() if all_txs else 'CAD'
        fx_by_date: dict = {}  # date -> float (USD->CAD), 仅 USD 股票使用
        if symbol_currency == 'USD':
            usd_dates = list({tx.trade_date for tx in all_txs if tx.type in ('BUY', 'SELL')})
            if usd_dates:
                raw_fx = currency_service.get_rates_for_dates(usd_dates, 'USD', 'CAD')
                fx_by_date = {d: float(r) for d, r in raw_fx.items()}

        roc_raw = T3Box42.query.filter(
            T3Box42.account_id.in_(account_ids_list),
            T3Box42.stock == symbol
        ).order_by(T3Box42.year.asc()).all()
        roc_by_year: dict = {}
        for r in roc_raw:
            if r.year not in roc_by_year:
                roc_by_year[r.year] = {'amount': 0.0, 'notes': [], 'account_ids': set(), 'per_account': {}}
            roc_by_year[r.year]['amount'] += float(r.box42_amount)
            roc_by_year[r.year]['account_ids'].add(r.account_id)
            roc_by_year[r.year]['per_account'][r.account_id] = float(r.box42_amount)
            if r.notes:
                roc_by_year[r.year]['notes'].append(r.notes)

        class _RocRecord:
            def __init__(self, yr, amt, notes_list, acct_ids):
                self.year = yr
                self.box42_amount = amt
                self.notes = '; '.join(notes_list) if notes_list else ''
                self.account_ids = acct_ids  # 贡献该年度ROC的账户集合

        roc_records = [
            _RocRecord(yr, d['amount'], d['notes'], d['account_ids'])
            for yr, d in sorted(roc_by_year.items())
        ]

        # per_account 金额明细（用于tooltip显示，key为account_id字符串）
        roc_per_account_by_year = {
            yr: d['per_account'] for yr, d in roc_by_year.items()
        }

        events = []
        for tx in all_txs:
            events.append((tx.trade_date, tx.id, 0, 'TX', tx, 0.0, 0))
        for roc in roc_records:
            key = _roc_insertion_sort_key(all_txs, roc.year)
            events.append((key[0], key[1], key[2], 'ROC', None, float(roc.box42_amount), roc.year))
        events.sort(key=lambda e: (e[0], e[1], e[2]))

        # total_cost 始终以 CAD 累计（USD 股票每笔按当日汇率转换）
        # T3 ROC 永远是 CAD，直接加减不需转换
        total_shares = 0.0
        total_cost = 0.0       # CAD
        total_cost_no_roc = 0.0  # CAD
        roc_applied = False
        roc_events_in_period = []
        buy_accounts_in_period: set = set()  # 当前持仓周期内有过买入的账户集合
        roc_joint_in_period = False           # 当前持仓周期内是否有多账户联合ROC
        # USD 股票额外跟踪（用于显示原始美元金额和汇率 tooltip）
        total_cost_usd = 0.0    # 仅 USD 股票
        buy_log: list = []      # [{date, qty, price_usd, fee_usd, fx_rate, cost_cad}]

        # 建立 roc_year -> account_ids 的快查表
        roc_year_accounts = {r.year: r.account_ids for r in roc_records}

        for ev_date, ev_id, ev_sub, etype, tx, roc_amount, roc_year in events:
            if etype == 'ROC':
                # T3 ROC 始终为 CAD，total_cost 也是 CAD，直接扣减
                if total_shares > 1e-9:
                    total_cost = max(0.0, total_cost - roc_amount)
                    roc_applied = True
                    roc_events_in_period.append({'year': roc_year, 'amount': roc_amount})
                    if len(roc_year_accounts.get(roc_year, set())) > 1:
                        roc_joint_in_period = True
            elif etype == 'TX':
                if tx.type == 'BUY':
                    qty = float(tx.quantity)
                    price_native = float(tx.price)
                    fee_native = float(tx.fee or 0)
                    cost_native = qty * price_native + fee_native
                    if symbol_currency == 'USD':
                        fx = fx_by_date.get(tx.trade_date, 1.35)
                        cost = cost_native * fx
                        total_cost_usd += cost_native
                        buy_log.append({
                            'date': tx.trade_date.isoformat(),
                            'qty': qty,
                            'price_usd': round(price_native, 4),
                            'fee_usd': round(fee_native, 2),
                            'fx_rate': round(fx, 4),
                            'cost_cad': round(cost, 2),
                        })
                    else:
                        cost = cost_native
                    total_shares += qty
                    total_cost += cost
                    total_cost_no_roc += cost
                    buy_accounts_in_period.add(tx.account_id)
                elif tx.type == 'SELL':
                    sell_qty = float(tx.quantity)
                    proceeds_native = float(tx.net_amount)
                    if symbol_currency == 'USD':
                        fx_sell = fx_by_date.get(tx.trade_date, 1.35)
                        proceeds = proceeds_native * fx_sell
                        acb_usd_val = (total_cost_usd / total_shares) * sell_qty if total_shares > 0 else 0.0
                        total_cost_usd -= acb_usd_val
                        acb_buy_snapshot = list(buy_log)
                    else:
                        fx_sell = None
                        proceeds = proceeds_native
                        acb_usd_val = None
                        acb_buy_snapshot = []
                    # ACB 和 proceeds 均为 CAD
                    avg_cost_per_share = (total_cost / total_shares) if total_shares > 0 else 0.0
                    acb = avg_cost_per_share * sell_qty
                    original_acb = ((total_cost_no_roc / total_shares) * sell_qty) if total_shares > 0 else 0.0
                    this_sell_roc_adjusted = roc_applied
                    this_sell_roc_years = list(roc_events_in_period)
                    # 本持仓周期内有多个账户贡献买入或ROC → ACB为跨账户联合计算
                    joint_acb = len(buy_accounts_in_period) > 1 or roc_joint_in_period
                    total_shares -= sell_qty
                    total_cost -= acb
                    total_cost_no_roc -= original_acb
                    if total_shares < 1e-9:
                        total_shares = 0.0
                        total_cost = 0.0
                        total_cost_no_roc = 0.0
                        total_cost_usd = 0.0
                        buy_log = []
                        roc_applied = False
                        roc_events_in_period = []
                        buy_accounts_in_period = set()
                        roc_joint_in_period = False
                    if tx.trade_date >= year_start:
                        gain = proceeds - acb
                        out_currency = 'CAD' if symbol_currency == 'USD' else (tx.currency or 'CAD')
                        records.append({
                            'date': tx.trade_date.isoformat(),
                            'symbol': symbol,
                            'quantity': sell_qty,
                            'proceeds': round(proceeds, 2),
                            'acb': round(acb, 2),
                            'original_acb': round(original_acb, 2),
                            'gain': round(gain, 2),
                            'currency': out_currency,
                            'original_currency': symbol_currency,
                            # USD 股票额外字段
                            'proceeds_usd': round(proceeds_native, 2) if symbol_currency == 'USD' else None,
                            'sell_fx_rate': round(fx_sell, 4) if fx_sell else None,
                            'acb_usd': round(acb_usd_val, 2) if acb_usd_val is not None else None,
                            'acb_buy_log': acb_buy_snapshot,
                            'roc_adjusted': this_sell_roc_adjusted,
                            'roc_applied_years': this_sell_roc_years,
                            'account_id': tx.account_id,
                            'account_name': account_name_map.get(tx.account_id, ''),
                            'joint_acb': joint_acb,
                        })

        t3_all_records[symbol] = [
            {
                'year': r.year,
                'amount': float(r.box42_amount),
                'notes': r.notes or '',
                # 多账户时附上各账户明细：[{account_name, amount}, ...]
                'breakdown': [
                    {'account_name': account_name_map.get(aid, str(aid)), 'amount': amt}
                    for aid, amt in sorted(
                        roc_per_account_by_year.get(r.year, {}).items(),
                        key=lambda x: account_name_map.get(x[0], str(x[0]))
                    )
                ] if len(roc_per_account_by_year.get(r.year, {})) > 1 else [],
            }
            for r in roc_records
        ]

    records.sort(key=lambda r: (r['symbol'], r['date']))
    return records, t3_all_records


def _roc_insertion_sort_key(all_txs_sorted, roc_year):
    """
    计算T3 Box42 ROC事件在ACB模拟中的插入排序键。

    - 若roc_year年末持仓=0（年内清仓）：
        - 找清仓前最后一笔BUY
        - 若该BUY在roc_year内 → 插在该BUY之后：(buy_date, buy_id, 1)
        - 否则（跨年持仓）→ 插在roc_year 1月1日最前：(Jan1, -1, 0)
    - 若年末持仓≠0 → 插在12月31日所有交易之后：(Dec31, MAX, 1)
    """
    year_end = date(roc_year, 12, 31)
    last_buy = None
    running_shares = 0.0
    final_clearance_last_buy = None

    for tx in all_txs_sorted:
        if tx.trade_date > year_end:
            break
        if tx.type == 'BUY':
            running_shares += float(tx.quantity)
            last_buy = tx
        elif tx.type == 'SELL':
            running_shares -= float(tx.quantity)
            if running_shares < 1e-9:
                running_shares = 0.0
                final_clearance_last_buy = last_buy
                last_buy = None

    if running_shares < 1e-9:
        # 年末已清仓
        if final_clearance_last_buy and final_clearance_last_buy.trade_date.year == roc_year:
            return (final_clearance_last_buy.trade_date, final_clearance_last_buy.id, 1)
        else:
            return (date(roc_year, 1, 1), -1, 0)
    else:
        # 年末仍有持仓，插在12月31日所有交易之后
        return (date(roc_year, 12, 31), 10 ** 9, 1)


@bp.route('/families/<int:family_id>/reports/annual-analysis/t5008', methods=['GET'])
def get_t5008_data(family_id):
    """
    获取T5008数据，响应统一返回 groups 列表，每个 group 对应一种账户类型。
    调用方式：
      - ?member_id=X&year=Y   → 按该成员的非税收优惠账户类型分组（每组一张表）
      - ?account_ids=1,2&year=Y / ?account_id=1&year=Y → 单组，兼容旧调用
    """
    from app.models.member import Member
    Family.query.get_or_404(family_id)
    year = request.args.get('year', type=int)
    if not year:
        return jsonify({'success': False, 'error': 'year is required'}), 400

    year_start = date(year, 1, 1)
    year_end   = date(year, 12, 31)

    from app.services.currency_service import currency_service
    annual_rate = currency_service.get_annual_average_rate(year, 'USD', 'CAD')
    annual_rate_float = float(annual_rate) if annual_rate else None

    member_id_param = request.args.get('member_id', type=int)

    if member_id_param:
        # ── 按成员分组模式 ──────────────────────────────────────────
        member = Member.query.get_or_404(member_id_param)
        memberships = AccountMember.query.filter_by(member_id=member_id_param).all()
        all_account_ids = [am.account_id for am in memberships]
        accounts_obj = {a.id: a for a in Account.query.filter(Account.id.in_(all_account_ids)).all()}

        # 按所有权人组合分组（跳过税收优惠账户）
        # 同一组合的账户合并ACB计算，不同组合各自独立
        from app.models.member import Member as MemberModel
        all_am = AccountMember.query.filter(AccountMember.account_id.in_(all_account_ids)).all()
        all_mid_set = {am.member_id for am in all_am}
        members_lookup = {m.id: m for m in MemberModel.query.filter(MemberModel.id.in_(all_mid_set)).all()}

        # account_id -> frozenset of member_ids
        acct_owners: dict = {}
        for am in all_am:
            acct_owners.setdefault(am.account_id, set()).add(am.member_id)

        groups_map: dict = {}   # frozenset(member_ids) -> [account_id, ...]
        for aid in all_account_ids:
            acct = accounts_obj.get(aid)
            if not acct:
                continue
            if acct.account_type and acct.account_type.tax_advantaged:
                continue
            owners_key = frozenset(acct_owners.get(aid, {member_id_param}))
            groups_map.setdefault(owners_key, []).append(aid)

        # 排序：独有账户（仅本人）在前，联名账户按共有人名字排序在后
        def _owners_sort_key(item):
            owners_key, _ = item
            is_joint = len(owners_key) > 1
            partner_names = sorted(
                members_lookup[mid].name for mid in owners_key
                if mid != member_id_param and mid in members_lookup
            )
            return (1 if is_joint else 0, partner_names)

        groups = []
        for owners_key, group_ids in sorted(groups_map.items(), key=_owners_sort_key):
            name_map = {aid: accounts_obj[aid].name for aid in group_ids if aid in accounts_obj}
            recs, t3 = _compute_t5008_for_accounts(group_ids, year_start, year_end, name_map)
            # 生成标签
            if len(owners_key) == 1:
                label = '个人账户'
            else:
                partner_names = sorted(
                    members_lookup[mid].name for mid in owners_key
                    if mid != member_id_param and mid in members_lookup
                )
                label = '联合 (' + ', '.join(partner_names) + ')'
            groups.append({'label': label, 'records': recs, 't3_all_records': t3})

        return jsonify({
            'success': True,
            'year': year,
            'title': member.name,
            'annual_usd_cad_rate': annual_rate_float,
            'groups': groups,
        })

    else:
        # ── 指定账户模式（兼容旧调用）──────────────────────────────
        account_ids_param = request.args.get('account_ids', '')
        account_id_param  = request.args.get('account_id', type=int)
        if account_ids_param:
            try:
                account_ids_list = [int(x.strip()) for x in account_ids_param.split(',') if x.strip()]
            except ValueError:
                return jsonify({'success': False, 'error': 'Invalid account_ids'}), 400
        elif account_id_param:
            account_ids_list = [account_id_param]
        else:
            return jsonify({'success': False, 'error': 'member_id or account_id(s) is required'}), 400

        accounts_obj = {a.id: a for a in Account.query.filter(Account.id.in_(account_ids_list)).all()}
        account_name_map = {aid: accounts_obj[aid].name for aid in account_ids_list if aid in accounts_obj}

        label_param = request.args.get('label', '')
        if label_param:
            group_label = label_param
        elif len(account_ids_list) == 1:
            group_label = account_name_map.get(account_ids_list[0], '')
        else:
            group_label = ' / '.join(account_name_map.get(aid, str(aid)) for aid in account_ids_list)

        recs, t3 = _compute_t5008_for_accounts(account_ids_list, year_start, year_end, account_name_map)

        return jsonify({
            'success': True,
            'year': year,
            'title': group_label,
            'annual_usd_cad_rate': annual_rate_float,
            'groups': [{'label': group_label, 'records': recs, 't3_all_records': t3}],
        })


@bp.route('/accounts/<int:account_id>/t3-box42', methods=['GET'])
def get_t3_box42(account_id):
    """获取账户某股票的T3 Box42记录（或所有记录）"""
    from app.models.t3_box42 import T3Box42
    Account.query.get_or_404(account_id)
    stock = request.args.get('stock')
    query = T3Box42.query.filter_by(account_id=account_id)
    if stock:
        query = query.filter_by(stock=stock.upper())
    records = query.order_by(T3Box42.year.desc()).all()
    return jsonify({'success': True, 'records': [r.to_dict() for r in records]})


@bp.route('/accounts/<int:account_id>/t3-box42', methods=['POST'])
def save_t3_box42(account_id):
    """新增或更新T3 Box42记录"""
    from app.models.t3_box42 import T3Box42
    Account.query.get_or_404(account_id)
    data = request.get_json() or {}
    stock = (data.get('stock') or '').upper()
    year = data.get('year')
    amount = data.get('box42_amount')
    if not stock or not year or amount is None:
        return jsonify({'success': False, 'error': 'stock, year, box42_amount are required'}), 400
    try:
        year = int(year)
        amount = float(amount)
    except (TypeError, ValueError):
        return jsonify({'success': False, 'error': 'Invalid year or amount'}), 400

    record = T3Box42.query.filter_by(account_id=account_id, stock=stock, year=year).first()
    if record:
        record.box42_amount = amount
        record.notes = data.get('notes', record.notes)
        record.updated_at = datetime.utcnow()
    else:
        record = T3Box42(
            account_id=account_id,
            stock=stock,
            year=year,
            box42_amount=amount,
            currency=data.get('currency', 'CAD'),
            notes=data.get('notes'),
        )
        db.session.add(record)
    db.session.commit()
    return jsonify({'success': True, 'record': record.to_dict()})


@bp.route('/accounts/<int:account_id>/t3-box42/<int:record_id>', methods=['DELETE'])
def delete_t3_box42(account_id, record_id):
    """删除T3 Box42记录"""
    from app.models.t3_box42 import T3Box42
    record = T3Box42.query.filter_by(id=record_id, account_id=account_id).first_or_404()
    db.session.delete(record)
    db.session.commit()
    return jsonify({'success': True})


@bp.route('/families/<int:family_id>/reports/annual-analysis/exchange-rates', methods=['POST'])
def refresh_annual_exchange_rates(family_id):
    """刷新指定年份的年度平均汇率（来源：加拿大银行）"""
    Family.query.get_or_404(family_id)

    payload = request.get_json() or {}
    years = payload.get('years')
    from_currency = (payload.get('from_currency') or 'USD').upper()
    to_currency = (payload.get('to_currency') or 'CAD').upper()

    if not isinstance(years, list) or not years:
        return jsonify({'success': False, 'error': _('Years list is required')}), 400

    try:
        year_values = sorted({int(year) for year in years})
    except (TypeError, ValueError):
        return jsonify({'success': False, 'error': _('Invalid year format provided')}), 400

    refreshed = currency_service.refresh_annual_rates_from_bank_of_canada(
        year_values,
        from_currency=from_currency,
        to_currency=to_currency
    )

    # 将Decimal转换为float以便JSON序列化
    formatted_rates = {
        year: (float(rate) if rate is not None else None)
        for year, rate in refreshed.items()
    }

    return jsonify({
        'success': True,
        'family_id': family_id,
        'from_currency': from_currency,
        'to_currency': to_currency,
        'rates': formatted_rates
    })

@bp.route('/families/<int:family_id>/reports/quarterly-analysis', methods=['GET'])
def get_family_quarterly_analysis(family_id):
    """获取家庭季度分析报告"""
    family = Family.query.get_or_404(family_id)
    
    # 获取参数
    member_id = request.args.get('member_id', type=int)
    account_id = request.args.get('account_id', type=int)
    account_type = request.args.get('account_type')
    years_param = request.args.get('years')
    
    # 确定账户范围
    account_ids, _ = _resolve_account_ids(family.id, member_id, account_id, account_type)

    years = None
    if years_param:
        try:
            years = [int(year.strip()) for year in years_param.split(',')]
        except ValueError:
            return jsonify({'error': 'Invalid years parameter format'}), 400
    
    try:
        params = {
            'years': years,
            'account_type': account_type
        }
        analysis_data, cache, cache_key, _ = _get_cached_analysis(
            'quarterly', family_id, member_id, account_id, account_ids, params
        )
        if analysis_data is None:
            # 调用统一的投资组合服务
            from app.services.portfolio_service import portfolio_service
            analysis_data = portfolio_service.get_quarterly_analysis(account_ids, years, member_id=member_id)
            _store_analysis_cache(
                'quarterly', family_id, member_id, account_id, account_ids, params, analysis_data, cache, cache_key
            )
        
        return jsonify({
            'family': family.to_dict(),
            'filter_info': {
                'member_id': member_id,
                'account_id': account_id,
                'account_count': len(account_ids),
            'account_type': account_type
            },
            'analysis': analysis_data
        })
        
    except Exception as e:
        return jsonify({
            'success': False,
            'error': f'Failed to generate quarterly analysis: {str(e)}'
        }), 500

@bp.route('/families/<int:family_id>/reports/monthly-analysis', methods=['GET'])
def get_family_monthly_analysis(family_id):
    """获取家庭月度分析报告"""
    family = Family.query.get_or_404(family_id)
    
    # 获取参数
    member_id = request.args.get('member_id', type=int)
    account_id = request.args.get('account_id', type=int)
    account_type = request.args.get('account_type')
    months = request.args.get('months', type=int)  # 不设置默认值，让服务层决定
    
    # ??????
    account_ids, _ = _resolve_account_ids(family.id, member_id, account_id, account_type)

    try:
        params = {
            'months': months,
            'account_type': account_type
        }
        analysis_data, cache, cache_key, _ = _get_cached_analysis(
            'monthly', family_id, member_id, account_id, account_ids, params
        )
        if analysis_data is None:
            # 调用统一的投资组合服务
            from app.services.portfolio_service import portfolio_service
            analysis_data = portfolio_service.get_monthly_analysis(account_ids, months, member_id=member_id)
            _store_analysis_cache(
                'monthly', family_id, member_id, account_id, account_ids, params, analysis_data, cache, cache_key
            )
        
        return jsonify({
            'family': family.to_dict(),
            'filter_info': {
                'member_id': member_id,
                'account_id': account_id,
                'account_count': len(account_ids),
            'account_type': account_type
            },
            'analysis': analysis_data
        })
        
    except Exception as e:
        return jsonify({
            'success': False,
            'error': f'Failed to generate monthly analysis: {str(e)}'
        }), 500


@bp.route('/families/<int:family_id>/reports/monthly-stock-pnl', methods=['GET'])
def get_family_monthly_stock_pnl(family_id):
    """Get per-stock monthly P&L (CAD) for the selected month."""
    family = Family.query.get_or_404(family_id)

    member_id = request.args.get('member_id', type=int)
    account_id = request.args.get('account_id', type=int)
    account_type = request.args.get('account_type')
    year = request.args.get('year', type=int)
    month = request.args.get('month', type=int)

    if not year or not month:
        return jsonify({'success': False, 'error': 'year and month are required'}), 400

    if not (1 <= month <= 12):
        return jsonify({'success': False, 'error': 'Invalid month'}), 400

    if year < 2000 or year > 2100:
        return jsonify({'success': False, 'error': 'Invalid year'}), 400

    account_ids, ownership_map = _resolve_account_ids(family.id, member_id, account_id, account_type)

    if not account_ids:
        return jsonify({'success': False, 'error': 'No accounts available'}), 400

    from calendar import monthrange
    start_date = date(year, month, 1)
    end_date = date(year, month, monthrange(year, month)[1])
    today = date.today()
    effective_end = end_date if end_date <= today else today
    prev_date = start_date - timedelta(days=1)

    portfolio_service = PortfolioService(auto_refresh_prices=False)

    def get_proportion(acc_id: int) -> Decimal:
        if not ownership_map:
            return Decimal('1')
        return ownership_map.get(acc_id, Decimal('0'))

    summary = portfolio_service.get_portfolio_summary(
        account_ids, TimePeriod.CUSTOM, end_date=effective_end
    )

    symbols = {}
    for holding in (summary.get('current_holdings', []) or []):
        symbol = holding.get('symbol')
        currency = (holding.get('currency') or 'USD').upper()
        if not symbol:
            continue
        key = f"{symbol}:{currency}"
        if key in symbols:
            continue
        symbols[key] = {
            'symbol': symbol,
            'currency': currency,
            'label': symbol
        }

    tx_symbols = db.session.query(Transaction.stock, Transaction.currency).filter(
        Transaction.account_id.in_(account_ids),
        Transaction.trade_date >= start_date,
        Transaction.trade_date <= effective_end,
        Transaction.stock.isnot(None),
        Transaction.stock != ''
    ).distinct().all()

    for symbol, currency in tx_symbols:
        if not symbol:
            continue
        currency = (currency or 'USD').upper()
        key = f"{symbol}:{currency}"
        if key in symbols:
            continue
        symbols[key] = {
            'symbol': symbol,
            'currency': currency,
            'label': symbol
        }

    if not symbols:
        return jsonify({'success': True, 'data': {'labels': [], 'values': [], 'symbols': []}})

    usd_to_cad = currency_service.get_current_rate('USD', 'CAD')
    if not usd_to_cad:
        usd_to_cad = currency_service.get_cad_usd_rates().get('usd_to_cad', 1)
    usd_to_cad_dec = Decimal(str(usd_to_cad or 1))

    results = []
    for key, info in symbols.items():
        total = Decimal('0')
        realized_total = Decimal('0')
        buy_qty = Decimal('0')
        sell_qty = Decimal('0')
        for acc_id in account_ids:
            proportion = get_proportion(acc_id)
            if proportion <= 0:
                continue
            current = portfolio_service.get_position_snapshot(info['symbol'], acc_id, effective_end)
            previous = portfolio_service.get_position_snapshot(info['symbol'], acc_id, prev_date)
            current_total = current.realized_gain + current.unrealized_gain
            prev_total = previous.realized_gain + previous.unrealized_gain
            total += (current_total - prev_total) * proportion

            realized_total += (current.realized_gain - previous.realized_gain) * proportion

            month_txs = Transaction.query.filter(
                Transaction.account_id == acc_id,
                Transaction.stock == info['symbol'],
                Transaction.trade_date >= start_date,
                Transaction.trade_date <= effective_end
            ).all()
            for tx in month_txs:
                if tx.type == 'BUY':
                    buy_qty += Decimal(str(tx.quantity)) * proportion
                elif tx.type == 'SELL':
                    sell_qty += Decimal(str(tx.quantity)) * proportion

        if info['currency'] == 'USD':
            total = total * usd_to_cad_dec
            realized_total = realized_total * usd_to_cad_dec

        results.append({
            'label': info['label'],
            'symbol': info['symbol'],
            'currency': info['currency'],
            'value': float(total),
            'realized_gain': float(realized_total),
            'buy_qty': float(buy_qty),
            'sell_qty': float(sell_qty)
        })

    # Sort by absolute value desc for readability
    results.sort(key=lambda x: abs(x['value']), reverse=True)

    return jsonify({
        'success': True,
        'data': {
            'labels': [r['label'] for r in results],
            'values': [r['value'] for r in results],
            'symbols': results
        }
    })


@bp.route('/families/<int:family_id>/reports/daily-analysis', methods=['GET'])
def get_family_daily_analysis(family_id):
    """获取家庭日度分析报告"""
    family = Family.query.get_or_404(family_id)
    
    # 获取参数
    member_id = request.args.get('member_id', type=int)
    account_id = request.args.get('account_id', type=int)
    account_type = request.args.get('account_type')
    days = request.args.get('days', type=int, default=30)
    
    # 确定账户范围
    account_ids = []
    if account_id:
        # 单个账户
        account = Account.query.get_or_404(account_id)
        if account.family_id != family_id:
            return jsonify({'error': 'Account does not belong to this family'}), 400
        account_ids = [account_id]
    elif member_id:
        # 成员的所有账户
        member = Member.query.get_or_404(member_id)
        if member.family_id != family_id:
            return jsonify({'error': 'Member does not belong to this family'}), 400
        account_memberships = AccountMember.query.filter_by(member_id=member.id).all()
        account_ids = [am.account_id for am in account_memberships]
    else:
        # 家庭的所有账户
        account_ids = AccountService.get_account_ids_display_list(family.id)
    
    try:
        # 调用统一的投资组合服务
        from app.services.portfolio_service import portfolio_service
        analysis_data = portfolio_service.get_daily_analysis(account_ids, days)
        
        # 如果是成员过滤，应用所有权比例计算
        if member_id:
            analysis_data = apply_member_ownership_proportions(analysis_data, member_id)
        
        return jsonify({
            'family': family.to_dict(),
            'filter_info': {
                'member_id': member_id,
                'account_id': account_id,
                'account_count': len(account_ids),
            'account_type': account_type
            },
            'analysis': analysis_data
        })
        
    except Exception as e:
        return jsonify({
            'success': False,
            'error': f'Failed to generate daily analysis: {str(e)}'
        }), 500

@bp.route('/families/<int:family_id>/reports/comparison', methods=['GET'])
def get_family_performance_comparison(family_id):
    """获取家庭收益对比分析"""
    family = Family.query.get_or_404(family_id)
    
    # 获取参数
    member_id = request.args.get('member_id', type=int)
    account_id = request.args.get('account_id', type=int)
    account_type = request.args.get('account_type')
    
    # 确定账户范围
    account_ids = []
    if account_id:
        # 单个账户
        account = Account.query.get_or_404(account_id)
        if account.family_id != family_id:
            return jsonify({'error': 'Account does not belong to this family'}), 400
        account_ids = [account_id]
    elif member_id:
        # 成员的所有账户
        member = Member.query.get_or_404(member_id)
        if member.family_id != family_id:
            return jsonify({'error': 'Member does not belong to this family'}), 400
        account_memberships = AccountMember.query.filter_by(member_id=member.id).all()
        account_ids = [am.account_id for am in account_memberships]
    else:
        # 家庭的所有账户
        account_ids = AccountService.get_account_ids_display_list(family.id)
    range_param = request.args.get('range', '1m')

    try:
        # 调用统一的投资组合服务
        from app.services.portfolio_service import portfolio_service
        analysis_data = portfolio_service.get_performance_comparison(
            account_ids, range_param, member_id=member_id,
            return_type=request.args.get('return_type', 'mwr'),
            include_breakdown=len(account_ids) > 1
        )

        return jsonify({
            'family': family.to_dict(),
            'filter_info': {
                'member_id': member_id,
                'account_id': account_id,
                'account_count': len(account_ids),
            'account_type': account_type
            },
            'analysis': analysis_data
        })
        
    except Exception as e:
        return jsonify({
            'success': False,
            'error': f'Failed to generate recent 30 days analysis: {str(e)}'
        }), 500

@bp.route('/families/<int:family_id>/reports/holdings-distribution', methods=['GET'])
def get_family_holdings_distribution(family_id):
    """获取家庭持仓分布数据 - 为四个饼状图提供数据"""
    family = Family.query.get_or_404(family_id)
    
    # 获取参数
    member_id = request.args.get('member_id', type=int)
    account_id = request.args.get('account_id', type=int)
    account_type = request.args.get('account_type')
    
    # 确定账户范围
    account_ids = []
    if account_id:
        # 单个账户
        account = Account.query.get_or_404(account_id)
        if account.family_id != family_id:
            return jsonify({'error': 'Account does not belong to this family'}), 400
        account_ids = [account_id]
    elif member_id:
        # 成员的所有账户
        member = Member.query.get_or_404(member_id)
        if member.family_id != family_id:
            return jsonify({'error': 'Member does not belong to this family'}), 400
        account_memberships = AccountMember.query.filter_by(member_id=member.id).all()
        account_ids = [am.account_id for am in account_memberships]
    else:
        # 家庭的所有账户
        account_ids = AccountService.get_account_ids_display_list(family.id)
    
    try:
        # 调用统一的投资组合服务
        from app.services.portfolio_service import portfolio_service
        distribution_data = portfolio_service.get_holdings_distribution(account_ids)
        
        # 如果是成员过滤，应用所有权比例计算
        if member_id:
            distribution_data = apply_member_ownership_proportions(distribution_data, member_id)
        
        return jsonify({
            'family': family.to_dict(),
            'filter_info': {
                'member_id': member_id,
                'account_id': account_id,
                'account_count': len(account_ids),
            'account_type': account_type
            },
            'distribution': distribution_data
        })
        
    except Exception as e:
        return jsonify({
            'success': False,
            'error': f'Failed to generate holdings distribution: {str(e)}'
        }), 500
