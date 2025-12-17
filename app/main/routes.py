"""
主要路由 - 页面视图
"""

from flask import render_template, request, jsonify, redirect, url_for, flash, session, current_app
from flask_babel import _
from decimal import Decimal, ROUND_HALF_UP
from datetime import datetime, date, timedelta
from bisect import bisect_left
from app.main import bp
from app import db
from app.models.family import Family
from app.models.member import Member
from app.models.account import Account, AccountType, AccountMember
from app.models.transaction import Transaction
# from app.models.stock import Stock, StockCategory  # Stock models deleted
from app.models.stocks_cache import StocksCache
from app.models.import_task import ImportTask, OCRTask
from app.models.market_holiday import MarketHoliday, StockHolidayAttempt
# from app.services.analytics_service import analytics_service, TimePeriod  # 旧架构已废弃
from app.services.currency_service import currency_service
from app.services.holdings_service import holdings_service
from app.services.asset_valuation_service import AssetValuationService
from app.services.report_service import ReportService
from app.services.account_service import AccountService


def _build_ownership_map(member_id):
    if not member_id:
        return None

    ownership_map = {}
    memberships = AccountMember.query.filter_by(member_id=member_id).all()
    for membership in memberships:
        try:
            ownership_map[membership.account_id] = Decimal(str(membership.ownership_percentage or 0)) / Decimal('100')
        except Exception:
            ownership_map[membership.account_id] = Decimal('0')
    return ownership_map


def _build_portfolio_view_data(
    *,
    account_ids,
    accounts,
    portfolio_summary,
    asset_service,
    ownership_map=None,
    target_date=None,
    exchange_rates=None,
    account_id=None
):
    target_date = target_date or date.today()
    exchange_rates = exchange_rates or currency_service.get_cad_usd_rates()

    raw_holdings = portfolio_summary.get('current_holdings', [])
    raw_cleared_holdings = portfolio_summary.get('cleared_holdings', [])
    # 使用account_id作为key，更准确
    account_lookup = {acc.id: acc for acc in accounts} if accounts else {}

    def safe_float(value, default=0.0):
        if value in (None, ""):
            return default
        try:
            return float(value)
        except (TypeError, ValueError):
            return default

    def extract_shares(holding_dict):
        return safe_float(holding_dict.get('current_shares', 0))

    def format_account_name(holding_dict):
        """根据holding数据格式化账户名称，使用account_id查找账户"""
        account_id = holding_dict.get('account_id')
        if not account_id or not account_lookup:
            return holding_dict.get('account_name', 'Unknown')
        
        account_obj = account_lookup.get(account_id)
        if account_obj:
            # 使用asset_service的方法来获取带成员信息的账户名称
            return asset_service._get_account_name_with_members(account_obj)
        
        # 如果找不到账户对象，返回原始名称
        return holding_dict.get('account_name', 'Unknown')

    def merge_holdings_by_stock(holdings_list):
        # 如果选择了单一账户（account_id不为None），不创建account_details
        # 如果未选择单一账户（account_id为None），即使只有一个账户也要创建account_details
        if account_id is not None or len(account_ids) <= 1:
            # 未选择单一账户时，为每个holding创建account_details
            if account_id is None and len(account_ids) <= 1:
                for holding in holdings_list:
                    total_shares = extract_shares(holding)
                    holding['current_shares'] = total_shares
                    holding['shares'] = total_shares
                    # 创建account_details，即使只有一个账户
                    account_detail = {
                        'account_name': format_account_name(holding),
                        'shares': total_shares,
                        'cost': safe_float(holding.get('total_cost')),
                        'realized_gain': safe_float(holding.get('realized_gain')),
                        'unrealized_gain': safe_float(holding.get('unrealized_gain'))
                    }
                    # 如果是清仓持仓，添加bought_shares和sold_shares字段
                    if 'total_bought_shares' in holding:
                        account_detail['bought_shares'] = safe_float(holding.get('total_bought_shares', 0))
                        account_detail['sold_shares'] = safe_float(holding.get('total_sold_shares', 0))
                        account_detail['sold_value'] = safe_float(holding.get('total_sold_value', 0))
                    holding['account_details'] = [account_detail]
            else:
                # 选择了单一账户，不创建account_details
                for holding in holdings_list:
                    total_shares = extract_shares(holding)
                    holding['current_shares'] = total_shares
                    holding['shares'] = total_shares
            return holdings_list

        merged = {}
        for holding in holdings_list:
            key = f"{holding.get('symbol', '')}_{holding.get('currency', 'USD')}"
            incoming_shares = extract_shares(holding)

            if key not in merged:
                merged_holding = holding.copy()
                merged_holding['current_shares'] = incoming_shares
                merged_holding['shares'] = incoming_shares
                total_cost = safe_float(holding.get('total_cost'))
                merged_holding['total_cost'] = total_cost
                merged_holding['average_cost'] = safe_float(holding.get('average_cost'))
                merged_holding['current_value'] = safe_float(holding.get('current_value'))
                unrealized_gain = safe_float(holding.get('unrealized_gain'))
                merged_holding['unrealized_gain'] = unrealized_gain
                merged_holding['total_bought_shares'] = safe_float(holding.get('total_bought_shares'))
                merged_holding['total_sold_shares'] = safe_float(holding.get('total_sold_shares'))
                merged_holding['total_bought_value'] = safe_float(holding.get('total_bought_value'))
                merged_holding['total_sold_value'] = safe_float(holding.get('total_sold_value'))
                # 确保unrealized_gain_percent基于当前的数据计算
                if total_cost > 0:
                    merged_holding['unrealized_gain_percent'] = (unrealized_gain / total_cost) * 100
                else:
                    merged_holding['unrealized_gain_percent'] = 0
                merged_holding['realized_gain'] = safe_float(holding.get('realized_gain'))
                merged_holding['total_dividends'] = safe_float(holding.get('total_dividends'))
                merged_holding['total_interest'] = safe_float(holding.get('total_interest'))
                merged_holding['merged_accounts'] = [holding.get('account_name', '')]

                account_detail = {
                    'account_name': format_account_name(holding),
                    'shares': incoming_shares,
                    'cost': total_cost,
                    'realized_gain': safe_float(holding.get('realized_gain')),
                    'unrealized_gain': unrealized_gain
                }
                # 如果是清仓持仓，添加bought_shares和sold_shares字段
                if 'total_bought_shares' in holding:
                    account_detail['bought_shares'] = safe_float(holding.get('total_bought_shares', 0))
                    account_detail['sold_shares'] = safe_float(holding.get('total_sold_shares', 0))
                    account_detail['sold_value'] = safe_float(holding.get('total_sold_value', 0))
                merged_holding['account_details'] = [account_detail]
                merged[key] = merged_holding
            else:
                existing = merged[key]
                existing['current_shares'] = safe_float(existing.get('current_shares')) + incoming_shares
                existing['shares'] = existing['current_shares']
                existing['total_cost'] = safe_float(existing.get('total_cost')) + safe_float(holding.get('total_cost'))
                existing['current_value'] = safe_float(existing.get('current_value')) + safe_float(holding.get('current_value'))
                existing['unrealized_gain'] = safe_float(existing.get('unrealized_gain')) + safe_float(holding.get('unrealized_gain'))
                existing['realized_gain'] = safe_float(existing.get('realized_gain')) + safe_float(holding.get('realized_gain'))
                existing['total_bought_shares'] = safe_float(existing.get('total_bought_shares')) + safe_float(holding.get('total_bought_shares'))
                existing['total_sold_shares'] = safe_float(existing.get('total_sold_shares')) + safe_float(holding.get('total_sold_shares'))
                existing['total_bought_value'] = safe_float(existing.get('total_bought_value')) + safe_float(holding.get('total_bought_value'))
                existing['total_sold_value'] = safe_float(existing.get('total_sold_value')) + safe_float(holding.get('total_sold_value'))
                existing['total_dividends'] = safe_float(existing.get('total_dividends')) + safe_float(holding.get('total_dividends'))
                existing['total_interest'] = safe_float(existing.get('total_interest')) + safe_float(holding.get('total_interest'))
                existing.setdefault('merged_accounts', []).append(holding.get('account_name', ''))

                account_detail = {
                    'account_name': format_account_name(holding),
                    'shares': incoming_shares,
                    'cost': safe_float(holding.get('total_cost')),
                    'realized_gain': safe_float(holding.get('realized_gain')),
                    'unrealized_gain': safe_float(holding.get('unrealized_gain'))
                }
                # 如果是清仓持仓，添加bought_shares和sold_shares字段
                if 'total_bought_shares' in holding:
                    account_detail['bought_shares'] = safe_float(holding.get('total_bought_shares', 0))
                    account_detail['sold_shares'] = safe_float(holding.get('total_sold_shares', 0))
                    account_detail['sold_value'] = safe_float(holding.get('total_sold_value', 0))
                existing.setdefault('account_details', []).append(account_detail)

        for merged_holding in merged.values():
            if merged_holding['current_shares'] > 0:
                merged_holding['average_cost'] = merged_holding['total_cost'] / merged_holding['current_shares']
                merged_holding['average_cost_display'] = merged_holding['average_cost']
                # 重新计算未实现收益率（基于合并后的总成本和未实现收益）
                total_cost = safe_float(merged_holding.get('total_cost', 0))
                unrealized_gain = safe_float(merged_holding.get('unrealized_gain', 0))
                if total_cost > 0:
                    merged_holding['unrealized_gain_percent'] = (unrealized_gain / total_cost) * 100
                else:
                    merged_holding['unrealized_gain_percent'] = 0
            else:
                merged_holding['average_cost'] = 0
                merged_holding['average_cost_display'] = 0
                merged_holding['unrealized_gain_percent'] = 0

            daily_change_value = safe_float(merged_holding.get('daily_change_value'))
            previous_value = safe_float(merged_holding.get('previous_value'))

            if previous_value:
                merged_holding['daily_change_percent'] = (daily_change_value / previous_value) * 100
            else:
                current_value = safe_float(merged_holding.get('current_value'))
                base_value = current_value - daily_change_value
                merged_holding['daily_change_percent'] = ((daily_change_value / base_value) * 100
                                                          if base_value else 0.0)

        return list(merged.values())

    def merge_holdings_and_cleared_cross_accounts(raw_holdings_list, raw_cleared_list):
        all_stocks = {}
        for holding in raw_holdings_list:
            symbol = holding.get('symbol')
            if not symbol:
                continue
            all_stocks.setdefault(symbol, {'holdings': [], 'cleared': []})['holdings'].append(holding)

        for cleared in raw_cleared_list:
            symbol = cleared.get('symbol')
            if not symbol:
                continue
            all_stocks.setdefault(symbol, {'holdings': [], 'cleared': []})['cleared'].append(cleared)

        final_holdings = []
        final_cleared = []

        for symbol, grouped in all_stocks.items():
            holdings_list = grouped['holdings']
            cleared_list = grouped['cleared']

            total_current_shares = sum(h.get('current_shares', 0) for h in holdings_list)
            total_cleared_shares = sum(c.get('total_sold_shares', 0) for c in cleared_list)

            if total_current_shares > 0:
                if len(holdings_list) == 1:
                    final_holdings.append(holdings_list[0])
                else:
                    merged_entries = merge_holdings_by_stock(holdings_list)
                    if merged_entries:
                        final_holdings.append(merged_entries[0])

                if cleared_list:
                    total_cleared_realized_gain = sum(c.get('realized_gain', 0) for c in cleared_list)
                    if total_cleared_realized_gain and final_holdings:
                        final_holdings[-1]['realized_gain'] = final_holdings[-1].get('realized_gain', 0) + total_cleared_realized_gain
            elif total_cleared_shares > 0:
                if len(cleared_list) == 1:
                    final_cleared.append(cleared_list[0])
                else:
                    merged_cleared_entries = merge_holdings_by_stock(cleared_list)
                    if merged_cleared_entries:
                        final_cleared.append(merged_cleared_entries[0])

        return final_holdings, final_cleared

    holdings, cleared_holdings = merge_holdings_and_cleared_cross_accounts(raw_holdings, raw_cleared_holdings)

    def consolidate_by_symbol_currency(items, drop_zero_holdings=True):
        """合并相同股票（含币种）的行，适用于多账户场景。
        drop_zero_holdings=True 时过滤零股持仓，避免清仓仍显示浮盈。
        """
        merged = {}

        def safe_num(val):
            try:
                return float(val or 0)
            except (TypeError, ValueError):
                return 0.0

        for item in items:
            key = f"{item.get('symbol')}__{item.get('currency', 'USD')}"
            if key not in merged:
                merged[key] = item.copy()
                merged[key]['account_details'] = item.get('account_details', [])[:] if item.get('account_details') else []
            else:
                target = merged[key]
                # 累加关键数值
                for field in [
                    'current_shares', 'shares', 'total_cost', 'current_value', 'unrealized_gain',
                    'realized_gain', 'total_dividends', 'total_interest', 'dividends', 'interest',
                    'daily_change_value', 'previous_value', 'total_bought_shares', 'total_sold_shares',
                    'total_bought_value', 'total_sold_value'
                ]:
                    target[field] = safe_num(target.get(field)) + safe_num(item.get(field))

                # 合并账户明细
                if item.get('account_details'):
                    target.setdefault('account_details', []).extend(item['account_details'])

            merged[key].setdefault('currency', item.get('currency'))
            merged[key].setdefault('symbol', item.get('symbol'))
            merged[key].setdefault('company_name', item.get('company_name'))
            merged[key].setdefault('sector', item.get('sector'))

        # 补充派生字段
        consolidated = []
        for value in merged.values():
            shares_val = safe_num(value.get('current_shares')) or safe_num(value.get('shares'))
            total_cost_val = safe_num(value.get('total_cost'))
            current_value_val = safe_num(value.get('current_value'))
            daily_change_val = safe_num(value.get('daily_change_value'))
            prev_value_val = safe_num(value.get('previous_value'))

            if drop_zero_holdings and abs(shares_val) < 1e-9:
                # 清仓后不在当前持仓表显示
                continue

            # 清仓行：强制未实现/当日变化为0，防止遗留显示
            if shares_val <= 0:
                value['unrealized_gain'] = 0
                value['daily_change_value'] = 0
                value['daily_change_percent'] = 0

            if shares_val > 0 and total_cost_val:
                value['average_cost'] = total_cost_val / shares_val
                value['average_cost_display'] = value['average_cost']
            else:
                value['average_cost'] = 0
                value['average_cost_display'] = 0

            if total_cost_val > 0:
                value['unrealized_gain_percent'] = (safe_num(value.get('unrealized_gain')) / total_cost_val) * 100
            else:
                value['unrealized_gain_percent'] = 0

            if prev_value_val:
                value['daily_change_percent'] = (daily_change_val / prev_value_val) * 100
            elif current_value_val:
                base_val = current_value_val - daily_change_val
                value['daily_change_percent'] = (daily_change_val / base_val * 100) if base_val else 0
            else:
                value['daily_change_percent'] = 0

            consolidated.append(value)

        return consolidated

    # 按股票+币种合并，避免跨账户重复行（单账户也安全）
    holdings = consolidate_by_symbol_currency(holdings, drop_zero_holdings=True)
    cleared_holdings = consolidate_by_symbol_currency(cleared_holdings, drop_zero_holdings=False)

    ibit_holdings = [h for h in holdings if h.get('symbol') == 'IBIT']
    ibit_cleared = [h for h in cleared_holdings if h.get('symbol') == 'IBIT']
    if ibit_holdings and ibit_cleared:
        total_realized_gain = ibit_holdings[0].get('realized_gain', 0) + ibit_cleared[0].get('realized_gain', 0)
        ibit_holdings[0]['realized_gain'] = total_realized_gain

    daily_change_metrics = {'cad': 0.0, 'cad_only': 0.0, 'usd_only': 0.0}
    usd_to_cad_rate = Decimal(str(exchange_rates.get('usd_to_cad', 1)))
    if holdings:
        daily_change_cad_only = Decimal('0')
        daily_change_usd_only = Decimal('0')

        for holding in holdings:
            raw_change = holding.get('daily_change_value')
            if raw_change in (None, ''):
                continue

            change_value = Decimal(str(raw_change))
            currency = (holding.get('currency') or 'USD').upper()

            if currency == 'USD':
                daily_change_usd_only += change_value
            else:
                daily_change_cad_only += change_value

        total_daily_change_cad = daily_change_cad_only + (daily_change_usd_only * usd_to_cad_rate)

        daily_change_metrics = {
            'cad': float(total_daily_change_cad.quantize(Decimal('0.01'), rounding=ROUND_HALF_UP)),
            'cad_only': float(daily_change_cad_only.quantize(Decimal('0.01'), rounding=ROUND_HALF_UP)),
            'usd_only': float(daily_change_usd_only.quantize(Decimal('0.01'), rounding=ROUND_HALF_UP))
        }

    comprehensive_metrics = asset_service.get_comprehensive_portfolio_metrics(
        account_ids,
        target_date=target_date,
        ownership_map=ownership_map
    )

    summary_data = portfolio_summary.get('summary', {})
    total_cost = summary_data.get('total_cost', 0)
    total_realized = summary_data.get('total_realized_gain', 0)
    total_unrealized = summary_data.get('total_unrealized_gain', 0)
    total_return = total_realized + total_unrealized
    comprehensive_metrics['total_return_rate'] = (total_return / total_cost * 100) if total_cost else 0

    return {
        'holdings': holdings,
        'cleared_holdings': cleared_holdings,
        'daily_change': daily_change_metrics,
        'metrics': comprehensive_metrics,
        'portfolio_summary': portfolio_summary
    }


@bp.route('/')
@bp.route('/index')
def index():
    """首页 - 直接重定向到仪表板"""
    return redirect(url_for('main.overview'))


@bp.route('/overview')
def overview():
    """仪表板 - 投资组合总览 - 真正的异步版本，立即显示页面"""
    import logging
    
    # 获取默认家庭（假设只有一个家庭，或者使用第一个家庭）
    family = Family.query.first()
    if not family:
        # 如果没有家庭，创建一个默认家庭
        family = Family(name="我的家庭")
        from app import db
        db.session.add(family)
        db.session.commit()
    
    # 获取过滤参数
    member_id = request.args.get('member_id', type=int)
    account_id = request.args.get('account_id', type=int)
    time_period = request.args.get('period', 'all_time')
    
    try:
        # 根据过滤条件获取账户（不涉及价格计算）
        ownership_map = None

        if account_id:
            accounts = Account.query.filter_by(id=account_id, family_id=family.id).all()
            filter_description = f"账户: {accounts[0].name}" if accounts else "未找到账户"
        elif member_id:
            from app.models.account import AccountMember
            member_accounts = AccountMember.query.filter_by(member_id=member_id).all()
            account_ids = [am.account_id for am in member_accounts]
            accounts = Account.query.filter(Account.id.in_(account_ids), Account.family_id == family.id).all()

            from app.models.member import Member
            member = Member.query.get(member_id)
            filter_description = f"成员: {member.name}" if member else "未找到成员"

            ownership_map = _build_ownership_map(member_id)
        else:
            accounts = Account.query.filter_by(family_id=family.id).all()
            filter_description = "All Members"

        # 获取投资组合数据（使用缓存的价格，不强制更新）
        account_ids = [acc.id for acc in accounts]
        
        # 获取汇率信息
        exchange_rates = currency_service.get_cad_usd_rates()
        
        # 使用Portfolio Service统一计算架构
        from app.services.portfolio_service import PortfolioService, TimePeriod
        portfolio_service = PortfolioService(auto_refresh_prices=False)
        
        # 使用Portfolio Service获取投资组合数据（使用缓存价格）
        portfolio_summary = portfolio_service.get_portfolio_summary(account_ids, TimePeriod.ALL_TIME)

        view_data = _build_portfolio_view_data(
            account_ids=account_ids,
            accounts=accounts,
            portfolio_summary=portfolio_summary,
            asset_service=AssetValuationService(auto_refresh_prices=False),
            ownership_map=ownership_map,
            target_date=date.today(),
            exchange_rates=exchange_rates,
            account_id=account_id
        )

        holdings = view_data['holdings']
        cleared_holdings = view_data['cleared_holdings']
        daily_change_metrics = view_data['daily_change']
        comprehensive_metrics = view_data['metrics']

        # 从综合指标中提取数据 - 使用统一计算架构的正确数据
        if comprehensive_metrics:
            total_assets = comprehensive_metrics['total_assets']['cad']
            # 使用统一计算架构的现金数据
            total_cash_cad = comprehensive_metrics['cash_balance']['cad']
            total_cash_usd = comprehensive_metrics['cash_balance']['usd']
            total_stock_value = comprehensive_metrics['total_assets']['cad'] - comprehensive_metrics['cash_balance']['total_cad']
        else:
            # 当没有数据时，使用默认值
            total_assets = 0
            total_stock_value = 0
            total_cash_cad = 0
            total_cash_usd = 0
        
        # 创建包含完整财务指标的metrics对象 - 总是创建
        if comprehensive_metrics:
            class ComprehensiveMetrics:
                def __init__(self, metrics_data, daily_change_data=None):
                    self.total_assets = type('obj', (object,), {
                        'cad': metrics_data['total_assets']['cad'],
                        'cad_only': metrics_data['total_assets']['cad_only'], 
                        'usd_only': metrics_data['total_assets']['usd_only']
                    })
                    # 计算股票市值：总资产 - 现金余额
                    stock_value_cad = metrics_data['total_assets']['cad'] - metrics_data['cash_balance']['total_cad']
                    self.stock_market_value = type('obj', (object,), {
                        'cad': stock_value_cad,
                        'cad_only': stock_value_cad, 
                        'usd_only': 0
                    })
                    self.cash_balance_total = metrics_data['cash_balance']['total_cad']
                    
                    # 完整的财务指标 - 使用新架构的准确计算
                    self.total_return = type('obj', (object,), {
                        'cad': metrics_data['total_return']['cad'], 
                        'cad_only': metrics_data['total_return']['cad_only'], 
                        'usd_only': metrics_data['total_return']['usd_only']
                    })
                    # 使用统一计算后的总回报率
                    self.total_return_rate = metrics_data.get('total_return_rate', 0)
                    self.realized_gain = type('obj', (object,), {
                        'cad': metrics_data['realized_gain']['cad'], 
                        'cad_only': metrics_data['realized_gain']['cad_only'], 
                        'usd_only': metrics_data['realized_gain']['usd_only']
                    })
                    self.unrealized_gain = type('obj', (object,), {
                        'cad': metrics_data['unrealized_gain']['cad'], 
                        'cad_only': metrics_data['unrealized_gain']['cad_only'], 
                        'usd_only': metrics_data['unrealized_gain']['usd_only']
                    })
                    daily_data = daily_change_data or {'cad': 0.0, 'cad_only': 0.0, 'usd_only': 0.0}
                    self.daily_change = type('obj', (object,), {
                        'cad': daily_data.get('cad', 0.0),
                        'cad_only': daily_data.get('cad_only', 0.0),
                        'usd_only': daily_data.get('usd_only', 0.0)
                    })
                    self.total_dividends = type('obj', (object,), {
                        'cad': metrics_data['dividends']['cad'], 
                        'cad_only': metrics_data['dividends']['cad_only'], 
                        'usd_only': metrics_data['dividends']['usd_only']
                    })
                    self.total_interest = type('obj', (object,), {
                        'cad': metrics_data['interest']['cad'], 
                        'cad_only': metrics_data['interest']['cad_only'], 
                        'usd_only': metrics_data['interest']['usd_only']
                    })
                    self.total_deposits = type('obj', (object,), {
                        'cad': metrics_data['total_deposits']['cad'], 
                        'cad_only': metrics_data['total_deposits']['cad_only'], 
                        'usd_only': metrics_data['total_deposits']['usd_only']
                    })
                    self.total_withdrawals = type('obj', (object,), {
                        'cad': metrics_data['total_withdrawals']['cad'], 
                        'cad_only': metrics_data['total_withdrawals']['cad_only'], 
                        'usd_only': metrics_data['total_withdrawals']['usd_only']
                    })
            
            metrics = ComprehensiveMetrics(comprehensive_metrics, daily_change_metrics)
        else:
            # 当没有数据时，创建空的metrics对象
            class EmptyMetrics:
                def __init__(self):
                    self.total_assets = type('obj', (object,), {'cad': 0, 'cad_only': 0, 'usd_only': 0})
                    self.stock_market_value = type('obj', (object,), {'cad': 0, 'cad_only': 0, 'usd_only': 0})
                    self.cash_balance_total = 0
                    self.total_return = type('obj', (object,), {'cad': 0, 'cad_only': 0, 'usd_only': 0})
                    self.total_return_rate = 0
                    self.realized_gain = type('obj', (object,), {'cad': 0, 'cad_only': 0, 'usd_only': 0})
                    self.unrealized_gain = type('obj', (object,), {'cad': 0, 'cad_only': 0, 'usd_only': 0})
                    self.daily_change = type('obj', (object,), {'cad': 0, 'cad_only': 0, 'usd_only': 0})
                    self.total_dividends = type('obj', (object,), {'cad': 0, 'cad_only': 0, 'usd_only': 0})
                    self.total_interest = type('obj', (object,), {'cad': 0, 'cad_only': 0, 'usd_only': 0})
                    self.total_deposits = type('obj', (object,), {'cad': 0, 'cad_only': 0, 'usd_only': 0})
                    self.total_withdrawals = type('obj', (object,), {'cad': 0, 'cad_only': 0, 'usd_only': 0})
            
            metrics = EmptyMetrics()
        
        # 准备现金数据
        cash_data = {
            'cad': total_cash_cad,
            'usd': total_cash_usd, 
            'total_cad': Decimal(str(total_cash_cad)) + Decimal(str(total_cash_usd)) * Decimal(str(exchange_rates.get('usd_to_cad', 1.35) if exchange_rates else 1.35))
        }
        
        # 获取统计数据
        from app.models.member import Member
        members_count = Member.query.filter_by(family_id=family.id).count()
        
        account_ids = [acc.id for acc in accounts] if accounts else []
        transactions_count = Transaction.query.filter(Transaction.account_id.in_(account_ids)).count() if account_ids else 0
        
        # 获取最近的交易
        if account_ids:
            recent_transactions = Transaction.query.filter(
                Transaction.account_id.in_(account_ids)
            ).order_by(Transaction.trade_date.desc()).limit(8).all()
        else:
            recent_transactions = []
        
        # 获取待处理任务
        pending_imports = ImportTask.query.filter_by(status='pending').count()
        pending_ocr = OCRTask.query.filter_by(status='pending').count()
        
        stats = {
            'members_count': members_count,
            'accounts_count': len(accounts),
            'transactions_count': transactions_count,
            'stocks_count': StocksCache.query.count(),
            'pending_imports': pending_imports,
            'pending_ocr': pending_ocr
        }
        
        # Get current member info for smart account selection
        current_member = None
        if member_id:
            current_member = Member.query.get(member_id)
        
        return render_template('investment/overview.html',
                             title=_('Overview'),
                             family=family,
                             stats=stats,
                             metrics=metrics,
                             holdings=holdings,
                             cleared_holdings=cleared_holdings,
                             exchange_rates=exchange_rates,
                             recent_transactions=recent_transactions,
                             filter_description=filter_description,
                             cash_data=cash_data,
                             current_period=time_period,
                             member_id=member_id,
                             account_id=account_id,
                             account_ids=account_ids,
                             accounts=accounts,
                             current_member=current_member,
                             current_view='overview')
        
    except Exception as e:
        # 如果新服务失败，记录错误并显示基本信息
        logging.error(f"AssetValuationService 出错: {e}", exc_info=True)
        
        # 显示基本信息（不再回退到旧服务）
        from app.models.member import Member
        stats = {
            'members_count': Member.query.filter_by(family_id=family.id).count(),
            'accounts_count': len(accounts),
            'transactions_count': 0,
            'stocks_count': 0,
            'pending_imports': 0,
            'pending_ocr': 0
        }
        
        return render_template('investment/overview.html',
                             title=_('Overview'),
                             family=family,
                             stats=stats,
                             metrics=None,
                             holdings=[],
                             cleared_holdings=[],
                             exchange_rates=None,
                             recent_transactions=[],
                             filter_description="All Members",
                             cash_data={'usd': 0, 'cad': 0, 'total_cad': 0},
                             current_period='all_time',
                             member_id=None,
                             account_id=None,
                             account_ids=[],
                             accounts=accounts,
                             current_member=None,
                             current_view='overview')


@bp.route('/api/overview/update-prices', methods=['POST'])
def update_overview_prices():
    """异步更新overview页面的股票价格"""
    try:
        # 获取请求参数
        data = request.get_json() or {}
        member_id = data.get('member_id')
        account_id = data.get('account_id')
        force_refresh = data.get('force_refresh', False)
        
        # 获取默认家庭
        family = Family.query.first()
        if not family:
            return jsonify({'success': False, 'error': 'No family found'}), 400
        
        # 根据过滤条件获取账户
        if account_id:
            accounts = Account.query.filter_by(id=account_id, family_id=family.id).all()
        elif member_id:
            from app.models.account import AccountMember
            member_accounts = AccountMember.query.filter_by(member_id=member_id).all()
            account_ids = [am.account_id for am in member_accounts]
            accounts = Account.query.filter(Account.id.in_(account_ids), Account.family_id == family.id).all()
        else:
            accounts = Account.query.filter_by(family_id=family.id).all()
        
        if not accounts:
            return jsonify({'success': False, 'error': 'No accounts found'}), 400
        
        # 获取这些账户中所有股票的symbol和currency组合
        from app.models.stocks_cache import StocksCache
        from app.models.transaction import Transaction
        
        account_ids = [acc.id for acc in accounts]
        symbols_currencies = db.session.query(StocksCache.symbol, StocksCache.currency).join(
            Transaction, Transaction.stock == StocksCache.symbol
        ).filter(
            Transaction.account_id.in_(account_ids),
            StocksCache.symbol.isnot(None),
            StocksCache.symbol != ''
        ).distinct().all()
        
        if not symbols_currencies:
            return jsonify({'success': True, 'message': 'No stocks to update'})
        
        # 更新股票价格
        from app.services.stock_price_service import StockPriceService
        stock_service = StockPriceService()
        
        symbol_currency_pairs = [(sc.symbol, sc.currency) for sc in symbols_currencies]
        update_result = stock_service.update_prices_for_symbols(symbol_currency_pairs, force_refresh=force_refresh)
        
        # 重新计算投资组合数据
        from app.services.portfolio_service import PortfolioService, TimePeriod
        from app.services.currency_service import CurrencyService
        
        portfolio_service = PortfolioService(auto_refresh_prices=False)
        currency_service = CurrencyService()
        
        # 获取汇率信息
        exchange_rates = currency_service.get_cad_usd_rates()
        
        # 使用Portfolio Service获取投资组合数据
        portfolio_summary = portfolio_service.get_portfolio_summary(account_ids, TimePeriod.ALL_TIME)
        
        # 构建视图数据
        ownership_map = None
        if member_id:
            ownership_map = _build_ownership_map(member_id)
        
        view_data = _build_portfolio_view_data(
            account_ids=account_ids,
            accounts=accounts,
            portfolio_summary=portfolio_summary,
            asset_service=AssetValuationService(auto_refresh_prices=False),
            ownership_map=ownership_map,
            target_date=date.today(),
            exchange_rates=exchange_rates,
            account_id=account_id
        )
        
        holdings = view_data['holdings']
        cleared_holdings = view_data['cleared_holdings']
        daily_change_metrics = view_data['daily_change']
        comprehensive_metrics = view_data['metrics']
        
        # 构建返回数据 - 清仓数据与当前股价无关，不返回
        response_data = {
            'success': True,
            'update_result': update_result,
            'holdings': holdings,
            'daily_change': daily_change_metrics,
            'metrics': comprehensive_metrics
        }
        
        return jsonify(response_data)
        
    except Exception as e:
        current_app.logger.error(f"异步更新股票价格失败: {str(e)}", exc_info=True)
        return jsonify({
            'success': False,
            'error': f'Failed to update stock prices: {str(e)}'
        }), 500


@bp.route('/api/stocks/refresh-price', methods=['POST'])
def refresh_single_stock_price():
    """直接通过Yahoo Finance刷新单只股票价格"""
    try:
        data = request.get_json() or {}
        symbol = (data.get('symbol') or '').strip().upper()
        currency = (data.get('currency') or '').strip().upper() or 'USD'
        force_refresh_raw = data.get('force_refresh', False)
        force_refresh = force_refresh_raw if isinstance(force_refresh_raw, bool) else str(force_refresh_raw).lower() == 'true'

        if not symbol:
            return jsonify({'success': False, 'error': _('Stock symbol is required')}), 400

        from app.services.stock_price_service import StockPriceService
        stock_service = StockPriceService()

        updated = stock_service.update_stock_price(symbol, currency, force_refresh=force_refresh)
        if not updated:
            return jsonify({'success': False, 'error': _('Failed to fetch price from Yahoo Finance')})

        stock_info = StocksCache.query.filter_by(symbol=symbol, currency=currency).first()
        current_price = float(stock_info.current_price) if stock_info and stock_info.current_price else None

        return jsonify({'success': True, 'symbol': symbol, 'currency': currency, 'current_price': current_price})
    except Exception as exc:
        current_app.logger.error('刷新股票价格失败: %s', exc, exc_info=True)
        return jsonify({'success': False, 'error': _('Failed to refresh stock price')}), 500


@bp.route('/api/stocks/clear-price-cache', methods=['POST'])
def clear_stock_price_cache():
    """清除所有股票的当前价格缓存"""
    try:
        cleared = StocksCache.query.update(
            {StocksCache.current_price: None, StocksCache.price_updated_at: None}
        )
        db.session.commit()
        return jsonify({'success': True, 'cleared': cleared})
    except Exception as exc:
        db.session.rollback()
        current_app.logger.error('清除股票价格缓存失败: %s', exc, exc_info=True)
        return jsonify({'success': False, 'error': _('Failed to clear price cache')}), 500


@bp.route('/api/accounts/cash-data', methods=['GET'])
def get_accounts_cash_data():
    """获取账户现金数据API"""
    try:
        # 获取默认家庭
        family = Family.query.first()
        if not family:
            return jsonify({'success': False, 'error': _('No family found')}), 404
        
        # 总是显示所有账户，不受过滤参数限制
        accounts = Account.query.filter_by(family_id=family.id).all()
        
        # 获取每个账户的现金数据
        from app.models.cash import Cash
        accounts_data = []
        
        for account in accounts:
            cash_record = Cash.get_account_cash(account.id)
            
            # 获取账户拥有者信息
            owners = []
            for account_member in account.account_members:
                owners.append(account_member.member.name)
            owner_names = ', '.join(owners) if owners else 'Unknown'
            
            accounts_data.append({
                'id': account.id,
                'name': account.name,
                'type': account.account_type.name if account.account_type else 'Unknown',
                'owners': owner_names,
                'cash': {
                    'cad': float(cash_record.cad) if cash_record else 0,
                    'usd': float(cash_record.usd) if cash_record else 0
                }
            })
        
        return jsonify({
            'success': True,
            'accounts': accounts_data
        })
        
    except Exception as e:
        return jsonify({'success': False, 'error': _('Failed to load accounts cash data')}), 500

@bp.route('/api/cash/batch-update', methods=['POST'])
def batch_update_cash():
    """批量更新账户现金API"""
    try:
        data = request.get_json()
        if not data or 'updates' not in data:
            return jsonify({'success': False, 'error': _('No update data provided')}), 400
        
        updates = data['updates']
        from app.models.cash import Cash
        
        # 批量更新每个账户的现金
        updated_count = 0
        for update in updates:
            account_id = update.get('account_id')
            cad_amount = update.get('cad', 0)
            usd_amount = update.get('usd', 0)
            
            if account_id:
                # 验证账户存在
                account = Account.query.get(account_id)
                if account:
                    Cash.update_cash(account_id, usd=usd_amount, cad=cad_amount)
                    updated_count += 1
        
        return jsonify({
            'success': True,
            'message': _('Cash balances updated successfully'),
            'updated_accounts': updated_count
        })
        
    except Exception as e:
        print(f"Error updating cash: {str(e)}")
        return jsonify({'success': False, 'error': _('Failed to update cash balances')}), 500

@bp.route('/family-members')
def family_members():
    """家庭成员管理页面"""
    family = Family.query.first()
    if not family:
        family = Family(name="我的家庭")
        from app import db
        db.session.add(family)
        db.session.commit()
    
    from app.models.member import Member
    members = Member.query.filter_by(family_id=family.id).order_by(Member.created_at.desc()).all()
    
    return render_template('members/list.html',
                         title=_('Family Members'),
                         family=family,
                         members=members)


@bp.route('/families/create')
def create_family():
    """创建家庭页面"""
    return render_template('families/create.html',
                         title=_('Create Family'))


@bp.route('/families/<int:family_id>')
def family_detail(family_id):
    """家庭详情页面"""
    family = Family.query.get_or_404(family_id)
    return render_template('families/detail.html',
                         title=family.name,
                         family=family)


@bp.route('/families/<int:family_id>/dashboard')
def family_dashboard(family_id):
    """家庭仪表板"""
    family = Family.query.get_or_404(family_id)
    return render_template('families/dashboard.html',
                         title=f"{family.name} - {_('Dashboard')}",
                         family=family)


@bp.route('/families/<int:family_id>/edit', methods=['GET', 'POST'])
def edit_family(family_id):
    """编辑家庭"""
    family = Family.query.get_or_404(family_id)
    
    if request.method == 'POST':
        new_name = request.form.get('name', '').strip()
        if new_name:
            family.name = new_name
            db.session.commit()
            flash(_('Family name updated successfully'), 'success')
        else:
            flash(_('Family name cannot be empty'), 'error')
        return redirect(url_for('main.family_members'))
    
    # For GET requests, redirect to family members page
    return redirect(url_for('main.family_members'))


@bp.route('/accounts')
def accounts():
    """账户列表页面"""
    # 获取默认家庭
    family = Family.query.first()
    if not family:
        family = Family(name="My Family")
        db.session.add(family)
        db.session.commit()
    
    # 获取账户列表（按ID排序）
    accounts = Account.query.filter_by(family_id=family.id).order_by(Account.id.asc()).all()
    
    # 获取账户类型和成员数据供模态框使用
    account_types = AccountType.query.all()
    members = Member.query.filter_by(family_id=family.id).all()
    
    return render_template('accounts/list.html',
                         title=_('Account Management'),
                         accounts=accounts,
                         account_types=account_types,
                         members=members,
                         family=family)


@bp.route('/accounts/create', methods=['GET', 'POST'])
def create_account():
    """创建账户"""
    if request.method == 'POST':
        try:
            # 获取基本信息
            name = request.form.get('name')
            account_type_id = request.form.get('account_type_id')
            is_joint = 'is_joint' in request.form
            
            # 验证账户类型是否支持联名账户
            if is_joint and account_type_id:
                account_type = AccountType.query.get(account_type_id)
                if account_type and account_type.name not in ['Regular', 'Margin']:
                    flash(_('Only Regular and Margin accounts can be joint accounts. Tax-advantaged accounts (TFSA, RRSP, RESP, FHSA) must have a single owner.'), 'error')
                    return redirect(url_for('main.accounts'))
            
            # 获取家庭ID（假设只有一个家庭）
            family = Family.query.first()
            if not family:
                flash(_('No family found'), 'error')
                return redirect(url_for('main.accounts'))
            
            # 创建账户
            account = Account(
                name=name,
                family_id=family.id,
                account_type_id=account_type_id,
                is_joint=is_joint
            )
            db.session.add(account)
            db.session.flush()  # 获取account.id
            
            if is_joint:
                # 处理联合账户成员
                members = request.form.getlist('members')
                total_percentage = 0
                
                for member_id in members:
                    percentage = float(request.form.get(f'percentage_{member_id}', 0))
                    total_percentage += percentage
                    
                    account_member = AccountMember(
                        account_id=account.id,
                        member_id=int(member_id),
                        ownership_percentage=percentage,
                        is_primary=(member_id == members[0])
                    )
                    db.session.add(account_member)
                
                if abs(total_percentage - 100.0) > 0.01:
                    flash(_('Total percentage must equal 100%%'), 'error')
                    db.session.rollback()
                    return redirect(url_for('main.accounts'))
            else:
                # 处理单一所有者账户
                single_owner_id = request.form.get('single_owner')
                if not single_owner_id:
                    flash(_('Please select an account owner'), 'error')
                    db.session.rollback()
                    return redirect(url_for('main.accounts'))
                
                account_member = AccountMember(
                    account_id=account.id,
                    member_id=int(single_owner_id),
                    ownership_percentage=100.0,
                    is_primary=True
                )
                db.session.add(account_member)
            
            db.session.commit()
            flash(_('Account created successfully'), 'success')
            
        except Exception as e:
            db.session.rollback()
            flash(_('Error creating account: {}').format(str(e)), 'error')
    
    return redirect(url_for('main.accounts'))


@bp.route('/account-types')
def account_types():
    """账户类型管理页面"""
    account_types = AccountType.query.all()
    return render_template('accounts/types.html',
                         title=_('Account Types'),
                         account_types=account_types)


@bp.route('/holdings-board')
def holdings_board():
    """Holdings Board - 持仓板块页面"""
    try:
        # 获取默认家庭
        family = Family.query.first()
        if not family:
            family = Family(name="我的家庭")
            db.session.add(family)
            db.session.commit()

        # 使用统一的账户服务获取排序后的账户列表
        from app.services.account_service import AccountService
        accounts = AccountService.get_accounts_display_list(family.id)

        # 获取所有成员供显示
        members = Member.query.filter_by(family_id=family.id).all()

        # 获取汇率信息
        from app.services.currency_service import CurrencyService
        currency_service = CurrencyService()
        exchange_rates = currency_service.get_cad_usd_rates()

        # 获取持仓服务
        from app.services.holdings_service import holdings_service

        return render_template('investment/holdings_board.html',
                             title=_('Holdings Board'),
                             accounts=accounts,
                             members=members,
                             exchange_rates=exchange_rates,
                             current_view='holdings_board')

    except Exception as e:
        current_app.logger.error(f"Holdings board error: {e}")
        flash(_('Error loading holdings board'), 'error')
        return redirect(url_for('main.overview'))


@bp.route('/api/holdings-board')
def api_holdings_board():
    """Holdings Board API - 获取持仓数据"""
    try:
        account_ids = request.args.getlist('account_ids')
        separate = request.args.get('separate', 'false').lower() == 'true'
        force_refresh = request.args.get('force_refresh', 'false').lower() == 'true'
        period = (request.args.get('period') or '1M').upper()

        if not account_ids:
            return jsonify({'success': False, 'error': 'No accounts selected'})

        # 获取账户信息
        accounts = Account.query.filter(Account.id.in_(account_ids)).all()
        if not accounts:
            return jsonify({'success': False, 'error': 'Invalid account IDs'})

        # 转换为整数列表
        account_ids = [int(id) for id in account_ids]

        # 计算时间范围
        def get_period_range(period_key: str):
            today = date.today()
            if period_key == '1M':
                return today - timedelta(days=30), today
            if period_key == '3M':
                return today - timedelta(days=90), today
            if period_key == '6M':
                return today - timedelta(days=180), today
            if period_key == 'YTD':
                return date(today.year, 1, 1), today
            if period_key == '1Y':
                return today - timedelta(days=365), today
            return None, today  # ALL

        start_date, end_date = get_period_range(period)

        # 如果需要强制刷新价格，先更新所有相关股票的价格
        if force_refresh:
            from app.services.stock_price_service import StockPriceService
            from app.models.stocks_cache import StocksCache

            # 获取这些账户中所有股票的symbol和currency组合
            symbols_currencies = db.session.query(
                StocksCache.symbol,
                StocksCache.currency
            ).join(
                Transaction, StocksCache.symbol == Transaction.stock
            ).filter(
                Transaction.account_id.in_(account_ids)
            ).distinct().all()

            if symbols_currencies:
                stock_service = StockPriceService()
                symbol_currency_pairs = [(sc.symbol, sc.currency) for sc in symbols_currencies]
                update_result = stock_service.update_prices_for_symbols(symbol_currency_pairs, force_refresh=True)
                current_app.logger.info(f"Force refresh result: {update_result['updated']} updated, {update_result['failed']} failed")

        # 使用与overview完全相同的服务
        from app.services.asset_valuation_service import AssetValuationService
        asset_service = AssetValuationService(auto_refresh_prices=False)

        # 获取详细的投资组合数据 - 与overview使用相同方法
        portfolio_data = asset_service.get_detailed_portfolio_data(account_ids)
        raw_holdings = portfolio_data.get('current_holdings', [])

        # 获取选定时间段内操作过的股票symbol集合
        symbols_in_period = None  # union of symbols across all selected accounts
        symbols_in_period_by_account = {}  # account_id -> set(symbols) for separate view
        tx_type_filter = ['BUY', 'SELL']
        if start_date:
            symbols_query = db.session.query(Transaction.account_id, Transaction.stock).filter(
                Transaction.account_id.in_(account_ids),
                Transaction.stock.isnot(None),
                Transaction.stock != '',
                Transaction.type.in_(tx_type_filter),
                Transaction.trade_date >= start_date,
                Transaction.trade_date <= end_date
            ).distinct()
        elif end_date:
            symbols_query = db.session.query(Transaction.account_id, Transaction.stock).filter(
                Transaction.account_id.in_(account_ids),
                Transaction.stock.isnot(None),
                Transaction.stock != '',
                Transaction.type.in_(tx_type_filter),
                Transaction.trade_date <= end_date
            ).distinct()
        else:
            symbols_query = None

        if symbols_query:
            symbols_in_period = set()
            for acc_id, sym in symbols_query.all():
                if not sym:
                    continue
                symbols_in_period.add(sym)
                symbols_in_period_by_account.setdefault(acc_id, set()).add(sym)

        # 获取指定账户在时间范围内的交易（仅买卖）
        def fetch_account_transactions(acc_id, symbols_filter=None, account_name=''):
            if not symbols_filter:
                return {}
            tx_q = Transaction.query.filter(
                Transaction.account_id == acc_id,
                Transaction.type.in_(tx_type_filter),
                Transaction.stock.in_(symbols_filter)
            )
            if start_date:
                tx_q = tx_q.filter(Transaction.trade_date >= start_date)
            if end_date:
                tx_q = tx_q.filter(Transaction.trade_date <= end_date)
            tx_q = tx_q.order_by(Transaction.trade_date.asc())
            tx_map = {}
            for tx in tx_q.all():
                tx_map.setdefault(tx.stock, []).append({
                    'date': tx.trade_date.isoformat(),
                    'type': tx.type,
                    'quantity': float(tx.quantity or 0),
                    'price': float(tx.price or 0),
                    'amount': float(tx.amount or 0),
                    'fee': float(tx.fee or 0),
                    'account_name': account_name
                })
            return tx_map

        def filter_holdings(holdings_list, symbols_filter=None, tx_map=None, *, strict=False):
            if symbols_filter is None:
                return holdings_list
            if not symbols_filter:
                return [] if strict else holdings_list
            filtered = []
            for h in holdings_list:
                symbol = h.get('symbol')
                if not symbol or symbol not in symbols_filter:
                    continue
                if tx_map and symbol in tx_map:
                    h = h.copy()
                    h['period_transactions'] = tx_map[symbol]
                filtered.append(h)
            return filtered

        def convert_cleared_to_holding(cleared, account_id, account_name):
            """构造显示用的清仓持仓行，确保表格字段完整"""
            def sf(val):
                try:
                    return float(val or 0)
                except (TypeError, ValueError):
                    return 0.0

            bought_shares = sf(cleared.get('total_bought_shares'))
            sold_shares = sf(cleared.get('total_sold_shares'))
            bought_value = sf(cleared.get('total_bought_value'))
            avg_cost = bought_value / bought_shares if bought_shares else 0.0
            realized_gain = sf(cleared.get('realized_gain'))

            return {
                'symbol': cleared.get('symbol'),
                'currency': cleared.get('currency', 'USD'),
                'account_id': account_id,
                'account_name': account_name,
                'company_name': cleared.get('company_name') or cleared.get('symbol'),
                'sector': cleared.get('sector') or 'Unknown',
                'shares': 0.0,
                'current_shares': 0.0,
                'average_cost': avg_cost,
                'average_cost_display': avg_cost,
                'total_cost': 0.0,
                'current_price': 0.0,
                'current_value': 0.0,
                'daily_change_value': 0.0,
                'daily_change_percent': 0.0,
                'unrealized_gain': 0.0,
                'unrealized_gain_percent': 0.0,
                'realized_gain': realized_gain,
                'dividends': sf(cleared.get('dividends')),
                'interest': sf(cleared.get('interest')),
                'total_bought_shares': bought_shares,
                'total_sold_shares': sold_shares,
                'total_bought_value': bought_value,
                'total_sold_value': sf(cleared.get('total_sold_value')),
                'period_transactions': cleared.get('period_transactions', []),
            }

        # 按账户分组数据，按选择顺序返回，使用带成员信息的账户名
        account_name_map = {acc.id: AccountService.get_account_name_with_members(acc) for acc in accounts}
        result_data = []

        for account_id in account_ids:
            # 为每个账户单独获取数据
            account_portfolio_data = asset_service.get_detailed_portfolio_data([account_id])
            account_symbols_filter = (
                symbols_in_period_by_account.get(account_id, set()) if separate else symbols_in_period
            )
            tx_map = fetch_account_transactions(account_id, account_symbols_filter, account_name_map.get(account_id, '')) if account_symbols_filter else {}
            current_holdings = filter_holdings(
                account_portfolio_data.get('current_holdings', []),
                account_symbols_filter,
                tx_map,
                strict=separate
            )
            cleared_holdings = filter_holdings(
                account_portfolio_data.get('cleared_holdings', []),
                account_symbols_filter,
                tx_map,
                strict=separate
            )

            # 获取当前现金余额
            cash_balance = asset_service.get_cash_balance(account_id)

            # 将符合时间段的清仓股票也加入显示（避免被过滤掉）
            current_symbols = {h.get('symbol') for h in current_holdings if h.get('symbol')}
            for cleared in cleared_holdings:
                sym = cleared.get('symbol')
                if not sym or sym in current_symbols:
                    continue
                current_holdings.append(convert_cleared_to_holding(cleared, account_id, account_name_map.get(account_id, '')))

            result_data.append({
                'account_id': account_id,
                'account_name': account_name_map[account_id],
                'holdings': current_holdings,
                'cash': {
                    'cad': float(cash_balance.get('cad', 0)),
                    'usd': float(cash_balance.get('usd', 0)),
                    'total_cad': float(cash_balance.get('total_cad', 0))
                }
            })

        return jsonify({'success': True, 'data': result_data})

    except Exception as e:
        current_app.logger.error(f"Holdings board API error: {e}")
        return jsonify({'success': False, 'error': str(e)})


@bp.route('/transactions')
def transactions():
    """交易记录列表页面"""
    try:
        page = request.args.get('page', 1, type=int)
        account_id = request.args.get('account_id', type=int)
        member_id = request.args.get('member_id', type=int)  # 新增成员筛选
        type_filter = request.args.get('type')
        stock_symbol = request.args.get('stock')  # 股票筛选
        
        # 构建查询
        query = Transaction.query
        
        # 如果指定了成员ID，获取该成员的所有账户
        if member_id:
            member_accounts = db.session.query(Account.id).join(AccountMember).filter(
                AccountMember.member_id == member_id
            ).all()
            account_ids = [acc.id for acc in member_accounts]
            if account_ids:
                query = query.filter(Transaction.account_id.in_(account_ids))
            else:
                # 如果成员没有账户，返回空结果
                query = query.filter(Transaction.id == -1)
        elif account_id:
            query = query.filter(Transaction.account_id == account_id)
            
        if type_filter:
            query = query.filter(Transaction.type == type_filter)
        if stock_symbol:
            query = query.filter(Transaction.stock.contains(stock_symbol.upper()))
        
        # 执行分页查询
        transactions = query.order_by(Transaction.trade_date.desc()).paginate(
            page=page, per_page=50, error_out=False
        )
        
        # 获取所有账户（预加载account_members关系）
        from app.services.account_service import AccountService
        accounts = AccountService.get_accounts_display_list()
        
        return render_template('transactions/list.html',
                             title=_('Transactions'),
                             transactions=transactions,
                             accounts=accounts,
                             current_view='transactions')
    except Exception as e:
        current_app.logger.error(f"Error in transactions route: {str(e)}")
        flash(_('Error loading transactions: ') + str(e), 'error')
        return redirect(url_for('main.overview'))


def save_transaction_record(transaction_id=None, account_id=None, transaction_type=None, quantity=None, price=None, currency=None, stock=None, fee=0.0, trade_date=None, notes='', amount=None):
    """
    统一的交易记录数据库操作函数
    - 如果 transaction_id 为 None，则创建新记录
    - 如果 transaction_id 不为 None，则修改现有记录
    """
    print(f"DEBUG: 📝 save_transaction_record called with: stock={stock}, currency={currency}, transaction_type={transaction_type}")
    print(f"DEBUG: 📝 Full params: transaction_id={transaction_id}, account_id={account_id}")
    print(f"DEBUG: 📝 quantity={quantity}, price={price}, fee={fee}, trade_date={trade_date}")
    print(f"DEBUG: 📝 notes='{notes}', amount={amount}")
    
    try:
        if transaction_id is None:
            # 创建新记录 - 如果没有提供交易日期，使用今天
            if trade_date is None:
                from datetime import date
                trade_date = date.today()
                
            # 验证必需字段
            if not account_id:
                raise ValueError("account_id is required")
            if not transaction_type:
                raise ValueError("transaction_type is required")
            if not currency:
                raise ValueError("currency is required")
            
            # 对于股票交易，验证quantity和price是必需的
            if transaction_type in ['BUY', 'SELL']:
                if not quantity:
                    raise ValueError("quantity is required for stock transactions")
                if not price:
                    raise ValueError("price is required for stock transactions")
            
            # 对于有股票代码的交易，验证币种一致性
            if stock:
                print(f"DEBUG: 🔍 Validating currency for stock {stock} with currency {currency}")
                existing_currency = Transaction.get_currency_by_stock_symbol(stock)
                print(f"DEBUG: 🔍 Existing currency for {stock}: {existing_currency}")
                if existing_currency and existing_currency != currency:
                    print(f"DEBUG: ❌ Currency conflict detected! Stock {stock} exists with {existing_currency}, trying to use {currency}")
                    raise ValueError(f"股票 {stock} 已存在使用 {existing_currency} 币种的交易记录，不允许使用 {currency} 币种。同一股票代码只能使用一种货币。")
                else:
                    print(f"DEBUG: ✅ Currency validation passed for {stock}")
            else:
                print(f"DEBUG: ⏭️ No stock symbol provided, skipping currency validation")
            
            transaction = Transaction(
                account_id=account_id,
                stock=stock,  # 直接使用stock，可能为None
                type=transaction_type,
                quantity=quantity,
                price=price,
                amount=amount,  # 新增amount字段
                fee=fee,
                trade_date=trade_date,
                currency=currency,
                notes=notes
            )
            db.session.add(transaction)
        else:
            # 修改现有记录
            transaction = Transaction.query.get(transaction_id)
            if not transaction:
                raise ValueError(f"Transaction with ID {transaction_id} not found")
            
            # 检查货币和股票代码的变化
            updated_stock = stock if stock is not None else transaction.stock
            updated_currency = currency if currency is not None else transaction.currency
            
            # 不允许修改币种
            if currency is not None and currency != transaction.currency:
                raise ValueError("不允许修改交易记录的币种。如需修改币种，请删除原记录并重新创建。")
            
            # 如果修改了股票代码，验证币种一致性
            if stock is not None and stock != transaction.stock:
                if updated_stock:
                    existing_currency = Transaction.get_currency_by_stock_symbol(updated_stock)
                    if existing_currency and existing_currency != updated_currency:
                        # 排除当前交易记录
                        other_transactions = Transaction.query.filter(
                            Transaction.stock == updated_stock,
                            Transaction.currency == existing_currency,
                            Transaction.id != transaction_id
                        ).first()
                        if other_transactions:
                            raise ValueError(f"股票 {updated_stock} 已存在使用 {existing_currency} 币种的交易记录，不允许修改为 {updated_currency} 币种。同一股票代码只能使用一种货币。")
            
            # 只更新提供的字段
            if account_id is not None:
                transaction.account_id = account_id
            if transaction_type is not None:
                transaction.type = transaction_type
            if quantity is not None:
                transaction.quantity = quantity
            if price is not None:
                transaction.price = price
            # currency is not allowed to be modified - validation above prevents this
            if stock is not None:
                transaction.stock = stock
            if fee is not None:
                transaction.fee = fee
            if trade_date is not None:
                transaction.trade_date = trade_date
            if notes is not None:
                transaction.notes = notes
        
        db.session.commit()
        
        flash(f"成功保存交易记录! ID: {transaction.id}", 'success')
        return transaction
        
    except Exception as e:
        print(f"DEBUG: ❌ Exception occurred in save_transaction_record: {str(e)}")
        print(f"DEBUG: Exception type: {type(e)}")
        import traceback
        print(f"DEBUG: Full traceback:")
        traceback.print_exc()
        
        db.session.rollback()
        print(f"DEBUG: Database session rolled back")
        
        error_msg = f"数据库保存失败: {str(e)}"
        flash(error_msg, 'error')
        raise Exception(error_msg)

@bp.route('/transactions/create', methods=['GET', 'POST'])
def create_transaction():
    """创建交易记录"""
    print("****** TRANSACTION CREATE FUNCTION CALLED ******")
    print(f"DEBUG: 🚀 create_transaction called with method: {request.method}")
    if request.method == 'POST':
        print(f"DEBUG: 🚀 POST request received, processing form data")
        # 获取表单数据
        account_id = request.form.get('account_id')
        type = request.form.get('type')
        quantity = request.form.get('quantity')
        price = request.form.get('price')
        fee = request.form.get('fee', 0)
        trade_date = request.form.get('trade_date')
        currency = request.form.get('currency')
        notes = request.form.get('notes', '').strip()
        amount = request.form.get('amount')  # 新增amount参数
        
        # 根据交易类型处理股票代码
        stock_symbol = None
        if type in ['BUY', 'SELL', 'DIVIDEND', 'INTEREST']:
            stock_symbol = request.form.get('stock_symbol', '').strip().upper()
            if not stock_symbol:
                transaction_type_name = {
                    'BUY': '买入',
                    'SELL': '卖出', 
                    'DIVIDEND': '分红',
                    'INTEREST': '利息'
                }.get(type, type)
                flash(f'{transaction_type_name}交易需要股票代码', 'error')
                return redirect(url_for('main.transactions', account_id=account_id))
        # DEPOSIT, WITHDRAWAL, FEE 等现金交易不需要股票代码
        
        # 验证必填字段
        if not all([account_id, type, trade_date, currency]):
            flash(_('Please fill in all required fields'), 'error')
            return redirect(url_for('main.transactions', account_id=account_id))
        
        # 对于现金交易，验证amount字段
        if type in ['DEPOSIT', 'WITHDRAWAL', 'DIVIDEND', 'INTEREST']:
            if not amount:
                flash(_('Amount is required for this transaction type'), 'error')
                return redirect(url_for('main.transactions', account_id=account_id))
        
        # 对于股票交易，验证quantity和price字段
        if type in ['BUY', 'SELL']:
            if not quantity or not price:
                flash(_('Quantity and price are required for stock transactions'), 'error')
                return redirect(url_for('main.transactions', account_id=account_id))
        
       
        try:
            # 转换数据类型
            from datetime import datetime
            trade_date = datetime.strptime(trade_date, '%Y-%m-%d').date()
            fee = float(fee) if fee else 0
            
            # 根据交易类型处理数据
            if type in ['DEPOSIT', 'WITHDRAWAL', 'DIVIDEND', 'INTEREST']:
                # 现金交易使用amount字段
                amount_value = float(amount) if amount else 0
                # quantity和price保持原值，如果为空则设为0（因为数据库不允许NULL）
                quantity = float(quantity) if quantity else 0
                price = float(price) if price else 0
            else:
                # 股票交易使用quantity和price
                quantity = float(quantity) if quantity else 0
                price = float(price) if price else 0
                amount_value = None  # 股票交易不使用amount字段
            
            print(f"DEBUG: Creating {type} transaction for stock={stock_symbol or 'None'}, amount={amount_value}")
            
            # 使用统一函数创建交易记录
            try:
                transaction = save_transaction_record(
                    account_id=int(account_id),
                    transaction_type=type,
                    quantity=quantity,
                    price=price,
                    amount=amount_value,
                    currency=currency,
                    stock=stock_symbol,
                    fee=fee,
                    trade_date=trade_date,
                    notes=notes
                )
                print(f"DEBUG: ✅ Transaction saved with ID: {transaction.id}")
                    
            except Exception as save_error:
                print(f"DEBUG: ERROR in save_transaction_record: {save_error}")
                raise save_error
            
            # Check if this is an AJAX request
            if request.headers.get('X-Requested-With') == 'XMLHttpRequest':
                return jsonify({
                    'success': True,
                    'message': '交易记录创建成功',
                    'redirect_url': url_for('main.transactions', account_id=account_id)
                })
            else:
                flash(_('Transaction created successfully'), 'success')
                return redirect(url_for('main.transactions', account_id=account_id))
            
        except Exception as e:
            db.session.rollback()
            # 直接使用异常消息，不添加英文前缀
            error_message = str(e)
            
            # Check if this is an AJAX request
            if request.headers.get('X-Requested-With') == 'XMLHttpRequest':
                return jsonify({
                    'success': False,
                    'error': error_message
                }), 400
            else:
                flash(error_message, 'error')
                return redirect(url_for('main.transactions', account_id=account_id))
    
    # GET request - show form
    from app.services.account_service import AccountService
    accounts = AccountService.get_accounts_display_list()
    # stocks = StocksCache.query.all()  # 暂时不需要预加载股票
    family_members = Member.query.all()
    
    from datetime import date
    return render_template('transactions/create.html',
                         title=_('Create Transaction'),
                         accounts=accounts,
                         # stocks=stocks,  # 暂时不需要股票列表
                         family_members=family_members,
                         today=date.today())


@bp.route('/stocks')
def stocks():
    """股票缓存管理页面"""
    page = request.args.get('page', 1, type=int)
    # category_id = request.args.get('category_id', type=int)  # 暂时移除分类功能
    search = request.args.get('search', '')
    
    query = StocksCache.query
    # if category_id:  # 暂时移除分类功能
    #     query = query.filter_by(category_id=category_id)
    if search:
        query = query.filter(StocksCache.symbol.contains(search) | StocksCache.name.contains(search))
    
    stocks = query.order_by(StocksCache.symbol).paginate(
        page=page, per_page=50, error_out=False
    )
    
    # categories = StockCategory.query.all()  # 暂时移除分类功能
    
    return render_template('stocks/list.html',
                         title=_('Stocks Cache'),
                         stocks=stocks,
                         # categories=categories,  # 暂时移除分类功能
                         search=search)


@bp.route('/stocks/<symbol>')
def stock_info(symbol):
    """股票信息页面"""
    stock = StocksCache.query.filter_by(symbol=symbol).first_or_404()
    return render_template('stocks/detail.html',
                         title=f"{stock.symbol} - {stock.name}",
                         stock=stock)


@bp.route('/categories')
def categories():
    """股票分类管理页面 - 暂时禁用"""
    # categories = StockCategory.query.order_by(StockCategory.sort_order).all()  # 分类功能暂时移除
    return render_template('categories/list.html',
                         title=_('Stock Categories - Disabled'),
                         # categories=categories  # 分类功能暂时移除
                         )


@bp.route('/import-transactions')
def import_transactions():
    """数据导入页面"""
    from app.services.account_service import AccountService
    accounts = AccountService.get_accounts_display_list()
    
    # 获取预选账户ID
    preselected_account_id = request.args.get('account_id', type=int)
    
    # 获取最近的导入任务
    recent_imports = ImportTask.query.order_by(ImportTask.created_at.desc()).limit(10).all()
    recent_ocr = OCRTask.query.order_by(OCRTask.created_at.desc()).limit(10).all()
    
    return render_template('imports/index.html',
                         title=_('Import Data'),
                         accounts=accounts,
                         preselected_account_id=preselected_account_id,
                         recent_imports=recent_imports,
                         recent_ocr=recent_ocr)


@bp.route('/ocr-tasks/<int:task_id>/review')
def review_ocr_task(task_id):
    """OCR任务审核页面"""
    task = OCRTask.query.get_or_404(task_id)
    return render_template('imports/ocr_review.html',
                         title=f"{_('Review OCR Task')} #{task.id}",
                         task=task)


@bp.route('/portfolio-reports')
def portfolio_reports():
    """投资组合报告页面"""
    families = Family.query.all()
    return render_template('reports/portfolio.html',
                         title=_('Portfolio Reports'),
                         families=families)


@bp.route('/performance-reports')
def performance_reports():
    """表现报告页面"""
    from app.services.account_service import AccountService
    accounts = AccountService.get_accounts_display_list()
    return render_template('reports/performance.html',
                         title=_('Performance Reports'),
                         accounts=accounts)


@bp.route('/tax-reports')
def tax_reports():
    """税务报告页面"""
    families = Family.query.all()
    return render_template('reports/tax.html',
                         title=_('Tax Reports'),
                         families=families)


@bp.route('/settings')
def settings():
    """设置页面"""
    return render_template('main/settings.html',
                         title=_('Settings'))


@bp.route('/about')
def about():
    """关于页面"""
    return render_template('main/about.html',
                         title=_('About'))


@bp.route('/privacy')
def privacy():
    """隐私政策页面"""
    return render_template('main/privacy.html',
                         title=_('Privacy Policy'))


@bp.route('/set-language', methods=['POST'])
def set_language():
    """设置语言"""
    language = request.json.get('language')
    if language and language in ['en', 'zh_CN']:
        session['language'] = language
        return jsonify({'success': True, 'language': language})
    return jsonify({'success': False, 'error': 'Invalid language'}), 400



@bp.route('/api/translations')
def get_translations():
    """获取前端翻译"""
    translations = {
        'Language switched successfully': _('Language switched successfully'),
        'Failed to switch language': _('Failed to switch language'),
        'Select Language': _('Select Language'),
        'Loading...': _('Loading...'),
        'Save': _('Save'),
        'Cancel': _('Cancel'),
        'Delete': _('Delete'),
        'Edit': _('Edit'),
        'Add': _('Add'),
        'Success': _('Success'),
        'Error': _('Error'),
        'Warning': _('Warning'),
        'Confirmation': _('Confirmation'),
        'Are you sure?': _('Are you sure?'),
        'Total Value': _('Total Value'),
        'Unrealized Gain': _('Unrealized Gain'),
        'Realized Gain': _('Realized Gain'),
        'Buy': _('Buy'),
        'Sell': _('Sell'),
        'Quantity': _('Quantity'),
        'Price': _('Price'),
        'Date': _('Date'),
        'Notes': _('Notes'),
        'Categories': _('Categories'),
        'Import CSV': _('Import CSV'),
        'Import Screenshot': _('Import Screenshot'),
        'Export Data': _('Export Data')
    }
    
    return jsonify(translations)

@bp.route('/api/stock-lookup')
def stock_lookup():
    """股票信息查找API"""
    symbol = request.args.get('symbol', '').strip().upper()
    currency = request.args.get('currency', '').upper()
    
    if not symbol or not currency:
        return jsonify({'success': False, 'error': 'Symbol and currency are required'})
    
    # 在股票缓存中查找股票
    stock_cache = StocksCache.query.filter_by(symbol=symbol).first()
    
    if stock_cache:
        return jsonify({
            'success': True,
            'stock': {
                'id': stock_cache.id,
                'symbol': stock_cache.symbol,
                'name': stock_cache.name,
                'exchange': stock_cache.exchange,
                # 'currency': currency  # currency不在StocksCache中存储
            }
        })
    else:
        return jsonify({
            'success': False,
            'message': 'Stock not found in cache'
        })


# 占位符路由 - 用于模板链接，避免 BuildError
@bp.route('/accounts/<int:account_id>/edit', methods=['POST'])
def edit_account(account_id):
    """编辑账户"""
    try:
        account = Account.query.get_or_404(account_id)
        old_account_type_id = account.account_type_id
        new_account_type_id = int(request.form.get('account_type_id'))
        
        # 获取新账户类型信息
        new_account_type = AccountType.query.get(new_account_type_id)
        if not new_account_type:
            flash(_('Invalid account type'), 'error')
            return redirect(url_for('main.accounts'))
        
        # 检查账户类型变更的合法性 - 加强验证
        if account.is_joint and new_account_type.name not in ['Regular', 'Margin']:
            flash(_('Joint accounts can only be Regular or Margin types. Tax-advantaged accounts (TFSA, RRSP, RESP, FHSA) can only have single owners.'), 'error')
            return redirect(url_for('main.accounts'))
        
        # 检查多成员账户变更为税收优惠账户的情况
        account_members_count = AccountMember.query.filter_by(account_id=account.id).count()
        if account_members_count > 1 and new_account_type.name not in ['Regular', 'Margin']:
            flash(_('Accounts with multiple members can only be Regular or Margin types. Tax-advantaged accounts can only have single owners.'), 'error')
            return redirect(url_for('main.accounts'))
        
        # 更新基本信息
        account.name = request.form.get('name')
        account.account_type_id = new_account_type_id
        
        # 如果从Taxable改为税收优惠账户，需要确保只有一个成员
        type_changed_to_single_owner = (
            old_account_type_id != new_account_type_id and 
            new_account_type.name in ['TFSA', 'RRSP', 'RESP', 'FHSA']
        )
        
        if type_changed_to_single_owner and account.is_joint:
            flash(_('Cannot change joint account to tax-advantaged type'), 'error')
            db.session.rollback()
            return redirect(url_for('main.accounts'))
        elif type_changed_to_single_owner:
            # 如果原来不是联名账户但有多个成员，重置为单一成员100%所有权
            account_members = AccountMember.query.filter_by(account_id=account.id).all()
            if len(account_members) > 1:
                # 删除现有成员关系
                AccountMember.query.filter_by(account_id=account.id).delete()
                # 只保留第一个成员，100%所有权
                primary_member = account_members[0]
                account_member = AccountMember(
                    account_id=account.id,
                    member_id=primary_member.member_id,
                    ownership_percentage=100.0,
                    is_primary=True
                )
                db.session.add(account_member)
        else:
            # 正常的成员更新逻辑
            if account.is_joint:
                # 删除现有成员关系
                AccountMember.query.filter_by(account_id=account.id).delete()
                
                # 添加新的成员关系
                total_percentage = 0
                member_ids = []
                
                for key in request.form.keys():
                    if key.startswith('member_'):
                        member_id = int(key.split('_')[1])
                        member_ids.append(member_id)
                        percentage = float(request.form.get(f'percentage_{member_id}', 0))
                        total_percentage += percentage
                        
                        account_member = AccountMember(
                            account_id=account.id,
                            member_id=member_id,
                            ownership_percentage=percentage,
                            is_primary=(member_id == member_ids[0] if member_ids else False)
                        )
                        db.session.add(account_member)
                
                if abs(total_percentage - 100.0) > 0.01:
                    flash(_('Total percentage must equal 100%%'), 'error')
                    db.session.rollback()
                    return redirect(url_for('main.accounts'))
        
        db.session.commit()
        flash(_('Account updated successfully'), 'success')
        
    except Exception as e:
        db.session.rollback()
        flash(_('Error updating account: {}').format(str(e)), 'error')
    
    return redirect(url_for('main.accounts'))


@bp.route('/accounts/<int:account_id>/delete', methods=['POST'])
def delete_account(account_id):
    """删除账户"""
    try:
        account = Account.query.get_or_404(account_id)
        account_name = account.name
        
        # 删除相关的账户成员关系
        AccountMember.query.filter_by(account_id=account.id).delete()
        
        # 删除账户
        db.session.delete(account)
        db.session.commit()
        
        flash(_('Account "{}" has been deleted successfully').format(account_name), 'success')
        
    except Exception as e:
        db.session.rollback()
        flash(_('Error deleting account: {}').format(str(e)), 'error')
    
    return redirect(url_for('main.accounts'))


@bp.route('/api/v1/accounts/<int:account_id>/edit')
def api_get_account_for_edit(account_id):
    """获取账户编辑信息 API"""
    try:
        account = Account.query.get_or_404(account_id)
        
        account_data = {
            'id': account.id,
            'name': account.name,
            'account_type_id': account.account_type_id,
            'is_joint': account.is_joint,
            'account_members': []
        }
        
        # 添加账户成员信息
        for am in account.account_members:
            account_data['account_members'].append({
                'member_id': am.member_id,
                'member_name': am.member.name,
                'ownership_percentage': float(am.ownership_percentage)
            })
        
        return jsonify({'success': True, 'account': account_data})
    
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500

@bp.route('/api/v1/accounts/<int:account_id>')
def api_get_account(account_id):
    """获取账户详细信息 API"""
    try:
        account = Account.query.get_or_404(account_id)
        return jsonify(account.to_dict(include_summary=True))
    
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@bp.route('/accounts/<int:account_id>')
def account_detail(account_id):
    """账户详情页面"""
    account = Account.query.get_or_404(account_id)
    return render_template('accounts/detail.html',
                         title=account.name,
                         account=account)

@bp.route('/transactions/<int:transaction_id>')
def transaction_detail(transaction_id):
    """交易记录详情页面"""
    transaction = Transaction.query.get_or_404(transaction_id)
    return render_template('transactions/detail.html',
                         title=f"{_('Transaction')} #{transaction.id}",
                         transaction=transaction)

@bp.route('/transactions/<int:transaction_id>/edit')
def edit_transaction(transaction_id):
    """编辑交易 - 占位符"""
    return f"<h1>Edit Transaction {transaction_id}</h1><p>This feature is under development.</p>"

@bp.route('/stocks/<int:stock_id>')
def stock_detail_id(stock_id):
    """股票详情 - 占位符"""
    return f"<h1>Stock {stock_id} Detail</h1><p>This feature is under development.</p>"

@bp.route('/stocks/<int:stock_id>/edit')
def edit_stock(stock_id):
    """编辑股票 - 占位符"""
    return f"<h1>Edit Stock {stock_id}</h1><p>This feature is under development.</p>"

@bp.route('/stocks/create')
def create_stock():
    """创建股票 - 占位符"""
    return "<h1>Create Stock</h1><p>This feature is under development.</p>"

@bp.route('/stocks/categories')
def stock_categories():
    """股票分类管理页面"""
    try:
        from app.models.stock_category import StockCategory
        
        # 获取所有分类及其股票数量
        categories = StockCategory.get_all_with_counts()
        
        # 获取未分类的股票
        uncategorized_stocks = StocksCache.query.filter(
            StocksCache.category_id.is_(None)
        ).all()
        
        # 获取所有分类的详细信息（用于编辑）
        all_categories = StockCategory.query.all()
        
        # 获取所有股票
        all_stocks = StocksCache.query.all()
        
        return render_template('stocks/categories.html',
                             title=_('Manage Stock Category'),
                             categories=categories,
                             uncategorized=uncategorized_stocks,
                             all_categories=all_categories,
                             all_stocks=all_stocks,
                             current_view='stock_categories')
    except Exception as e:
        flash(_('Error loading stock categories: ') + str(e), 'error')
        return redirect(url_for('main.overview'))

# 股票分类CRUD路由
@bp.route('/api/stock-categories', methods=['POST'])
def create_stock_category():
    """创建股票分类"""
    try:
        from app.models.stock_category import StockCategory
        data = request.get_json()
        
        if not data or not data.get('name'):
            return jsonify({'success': False, 'error': _('Category name is required')}), 400
        
        # 检查是否已存在同名分类
        existing = StockCategory.query.filter_by(name=data['name']).first()
        if existing:
            return jsonify({'success': False, 'error': _('Category name already exists')}), 400
        
        category = StockCategory(
            name=data['name'],
            name_en=data.get('name_en', ''),
            description=data.get('description', ''),
            color=data.get('color', '#007bff')
        )
        
        db.session.add(category)
        db.session.commit()
        
        return jsonify({
            'success': True,
            'message': _('Category created successfully'),
            'category': category.to_dict()
        })
        
    except Exception as e:
        db.session.rollback()
        return jsonify({'success': False, 'error': str(e)}), 500

@bp.route('/api/stock-categories/<int:category_id>', methods=['PUT'])
def update_stock_category(category_id):
    """更新股票分类"""
    try:
        from app.models.stock_category import StockCategory
        category = StockCategory.query.get_or_404(category_id)
        data = request.get_json()
        
        if not data or not data.get('name'):
            return jsonify({'success': False, 'error': _('Category name is required')}), 400
        
        # 检查是否已存在同名分类（除了当前分类）
        existing = StockCategory.query.filter(
            StockCategory.name == data['name'],
            StockCategory.id != category_id
        ).first()
        if existing:
            return jsonify({'success': False, 'error': _('Category name already exists')}), 400
        
        category.name = data['name']
        category.name_en = data.get('name_en', '')
        category.description = data.get('description', '')
        category.color = data.get('color', category.color)
        
        db.session.commit()
        
        return jsonify({
            'success': True,
            'message': _('Category updated successfully'),
            'category': category.to_dict()
        })
        
    except Exception as e:
        db.session.rollback()
        return jsonify({'success': False, 'error': str(e)}), 500

@bp.route('/api/stock-categories/<int:category_id>', methods=['DELETE'])
def delete_stock_category(category_id):
    """删除股票分类"""
    try:
        from app.models.stock_category import StockCategory
        category = StockCategory.query.get_or_404(category_id)
        
        # 检查是否有股票使用这个分类
        stock_count = StocksCache.query.filter_by(category_id=category_id).count()
        if stock_count > 0:
            return jsonify({
                'success': False, 
                'error': _('Cannot delete category with %(count)d stocks. Please reassign stocks first.', count=stock_count)
            }), 400
        
        db.session.delete(category)
        db.session.commit()
        
        return jsonify({
            'success': True,
            'message': _('Category deleted successfully')
        })
        
    except Exception as e:
        db.session.rollback()
        return jsonify({'success': False, 'error': str(e)}), 500

@bp.route('/api/stocks/<int:stock_id>/category', methods=['PUT'])
def assign_stock_category(stock_id):
    """分配股票分类"""
    try:
        stock = StocksCache.query.get_or_404(stock_id)
        data = request.get_json()
        
        category_id = data.get('category_id') if data else None
        stock.category_id = category_id
        
        db.session.commit()
        
        return jsonify({
            'success': True,
            'message': _('Stock category updated successfully')
        })
        
    except Exception as e:
        db.session.rollback()
        return jsonify({'success': False, 'error': str(e)}), 500

@bp.route('/members/create', methods=['GET', 'POST'])
def create_member():
    """创建家庭成员"""
    if request.method == 'POST':
        name = request.form.get('name', '').strip()
        if name:
            family = Family.query.first()
            if family:
                new_member = Member(
                    family_id=family.id,
                    name=name
                )
                db.session.add(new_member)
                db.session.commit()
                flash(_('Member added successfully'), 'success')
            else:
                flash(_('No family found'), 'error')
        else:
            flash(_('Member name cannot be empty'), 'error')
        return redirect(url_for('main.family_members'))
    
    # For GET requests, redirect to family members page
    return redirect(url_for('main.family_members'))

@bp.route('/members/<int:member_id>')
def member_detail(member_id):
    """家庭成员详情 - 占位符"""
    return f"<h1>Member {member_id} Detail</h1><p>This feature is under development.</p>"

@bp.route('/members/<int:member_id>/edit', methods=['GET', 'POST'])
def edit_member(member_id):
    """编辑家庭成员"""
    member = Member.query.get_or_404(member_id)
    
    if request.method == 'POST':
        new_name = request.form.get('name', '').strip()
        if new_name:
            member.name = new_name
            db.session.commit()
            flash(_('Member updated successfully'), 'success')
        else:
            flash(_('Member name cannot be empty'), 'error')
        return redirect(url_for('main.family_members'))
    
    # For GET requests, redirect to family members page
    return redirect(url_for('main.family_members'))

@bp.route('/members/<int:member_id>/delete', methods=['POST'])
def delete_member(member_id):
    """删除家庭成员"""
    member = Member.query.get_or_404(member_id)
    member_name = member.name
    
    try:
        db.session.delete(member)
        db.session.commit()
        flash(_('Member "{}" has been deleted successfully').format(member_name), 'success')
    except Exception as e:
        db.session.rollback()
        flash(_('Error deleting member: {}').format(str(e)), 'error')
    
    return redirect(url_for('main.family_members'))


# 统计视图路由
@bp.route('/annual-stats')
def annual_stats():
    """年度统计视图"""
    family = Family.query.first()
    if not family:
        family = Family(name="我的家庭")
        from app import db
        db.session.add(family)
        db.session.commit()

    # 获取汇率信息
    from app.services.currency_service import CurrencyService
    currency_service = CurrencyService()
    exchange_rates = currency_service.get_cad_usd_rates()

    # 获取年度数据（简化版）
    from datetime import datetime, timedelta
    from sqlalchemy import func, extract
    
    current_year = datetime.now().year
    years = range(current_year - 4, current_year + 1)  # 最近5年
    
    annual_data = []
    for year in years:
        year_transactions = Transaction.query.join(Account).filter(
            Account.family_id == family.id,
            extract('year', Transaction.trade_date) == year
        ).all()
        
        buy_amount = sum(t.quantity * t.price for t in year_transactions if t.type == 'buy')
        sell_amount = sum(t.quantity * t.price for t in year_transactions if t.type == 'sell')
        net_investment = buy_amount - sell_amount
        
        annual_data.append({
            'year': year,
            'buy_amount': buy_amount,
            'sell_amount': sell_amount,
            'net_investment': net_investment,
            'transaction_count': len(year_transactions)
        })
    
    return render_template('investment/annual_stats.html',
                         title=_('Annual Statistics'),
                         annual_data=annual_data,
                         current_year=current_year,
                         exchange_rates=exchange_rates)



@bp.route('/monthly-stats')
def monthly_stats():
    """月度统计视图"""
    from flask_babel import _
    family = Family.query.first()
    if not family:
        family = Family(name="我的家庭")
        from app import db
        db.session.add(family)
        db.session.commit()

    # 获取汇率信息
    from app.services.currency_service import CurrencyService
    currency_service = CurrencyService()
    exchange_rates = currency_service.get_cad_usd_rates()

    return render_template('investment/monthly_stats.html',
                         title=_('Monthly Statistics'),
                         exchange_rates=exchange_rates)


@bp.route('/quarterly-stats')
def quarterly_stats():
    """季度统计视图"""
    family = Family.query.first()
    if not family:
        family = Family(name="我的家庭")
        from app import db
        db.session.add(family)
        db.session.commit()
    
    from datetime import datetime
    from sqlalchemy import extract
    
    current_year = datetime.now().year
    current_quarter = (datetime.now().month - 1) // 3 + 1
    
    quarterly_data = []
    
    # 获取最近8个季度的数据
    for year_offset in range(2):
        year = current_year - year_offset
        for quarter in [4, 3, 2, 1]:
            if year == current_year and quarter > current_quarter:
                continue
                
            # 计算季度月份范围
            start_month = (quarter - 1) * 3 + 1
            end_month = quarter * 3
            
            quarter_transactions = Transaction.query.join(Account).filter(
                Account.family_id == family.id,
                extract('year', Transaction.trade_date) == year,
                extract('month', Transaction.trade_date) >= start_month,
                extract('month', Transaction.trade_date) <= end_month
            ).all()
            
            buy_amount = sum(t.quantity * t.price for t in quarter_transactions if t.type == 'buy')
            sell_amount = sum(t.quantity * t.price for t in quarter_transactions if t.type == 'sell')
            
            quarterly_data.append({
                'year': year,
                'quarter': quarter,
                'quarter_name': f'{year}Q{quarter}',
                'buy_amount': buy_amount,
                'sell_amount': sell_amount,
                'net_investment': buy_amount - sell_amount,
                'transaction_count': len(quarter_transactions)
            })
    
    quarterly_data.reverse()  # 按时间顺序排列

    # 获取汇率信息
    from app.services.currency_service import CurrencyService
    currency_service = CurrencyService()
    exchange_rates = currency_service.get_cad_usd_rates()

    return render_template('investment/quarterly_stats.html',
                         title=_('Quarterly Statistics'),
                         quarterly_data=quarterly_data,
                         exchange_rates=exchange_rates)


@bp.route('/daily-stats')
def daily_stats():
    """每日统计视图"""
    from flask_babel import _
    from app.services.currency_service import CurrencyService

    family = Family.query.first()
    if not family:
        family = Family(name="我的家庭")
        from app import db
        db.session.add(family)
        db.session.commit()

    currency_service = CurrencyService()
    exchange_rates = currency_service.get_cad_usd_rates()

    return render_template('investment/daily_stats.html',
                         title=_('Daily Statistics'),
                         family=family,
                         exchange_rates=exchange_rates)


@bp.route('/comparison')
def performance_comparison():
    """收益对比视图"""
    from flask_babel import _
    family = Family.query.first()
    if not family:
        family = Family(name="我的家庭")
        from app import db
        db.session.add(family)
        db.session.commit()
    
    return render_template('investment/performance_comparison.html',
                         title=_('Performance Comparison'),
                         family=family)


@bp.route('/holdings-analysis')
def holdings_analysis():
    """持仓分析视图"""
    from app.services.currency_service import CurrencyService

    family = Family.query.first()
    if not family:
        family = Family(name="我的家庭")
        from app import db
        db.session.add(family)
        db.session.commit()

    currency_service = CurrencyService()
    exchange_rates = currency_service.get_cad_usd_rates()

    return render_template('investment/holdings_analysis.html',
                         title=_('Holdings Analysis'),
                         family=family,
                         exchange_rates=exchange_rates)


@bp.route('/transactions/delete-all', methods=['POST'])
def delete_all_transactions():
    """删除所有交易记录"""
    try:
        from app.models.transaction import Transaction
        # from app.models.holding import CurrentHolding  # CurrentHolding model deleted
        from app import db
        
        # 删除所有持仓记录 - temporarily disabled
        # CurrentHolding.query.delete()
        
        # 删除所有交易记录
        deleted_count = Transaction.query.delete()
        
        db.session.commit()
        
        return jsonify({
            'success': True, 
            'message': f'Successfully deleted {deleted_count} transactions',
            'deleted_count': deleted_count
        })
        
    except Exception as e:
        db.session.rollback()
        return jsonify({
            'success': False, 
            'error': str(e)
        }), 500


@bp.route('/transactions/export')
def export_transactions():
    """导出交易记录为CSV文件 - 统一使用CSVTransactionService"""
    try:
        from app.services.csv_service import CSVTransactionService
        from flask import Response
        import os

        # 获取参数
        account_id = request.args.get('account_id', type=int)
        start_date = request.args.get('start_date')
        end_date = request.args.get('end_date')

        csv_service = CSVTransactionService()

        if account_id:
            # 导出单个账户
            file_path = csv_service.export_transactions_to_csv(
                account_id=account_id,
                start_date=start_date,
                end_date=end_date
            )
        else:
            # 导出所有账户的交易 - 创建临时合并文件
            from app.models.family import Family
            from app.models.account import Account
            family = Family.query.first()
            if not family:
                raise ValueError("No family found")

            accounts = Account.query.filter_by(family_id=family.id).all()
            from app.models.transaction import Transaction
            from datetime import datetime
            import tempfile
            import csv

            # 构建查询
            query = Transaction.query
            if start_date:
                start_date_obj = datetime.strptime(start_date, '%Y-%m-%d').date()
                query = query.filter(Transaction.trade_date >= start_date_obj)
            if end_date:
                end_date_obj = datetime.strptime(end_date, '%Y-%m-%d').date()
                query = query.filter(Transaction.trade_date <= end_date_obj)

            transactions = query.order_by(Transaction.trade_date.desc()).all()

            # 创建临时文件
            export_dir = current_app.config.get('EXPORT_FOLDER', tempfile.gettempdir())
            os.makedirs(export_dir, exist_ok=True)

            filename = f"all_accounts_transactions_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
            file_path = os.path.join(export_dir, filename)

            # 使用统一的CSV格式（与CSVTransactionService一致）
            with open(file_path, 'w', newline='', encoding='utf-8') as csvfile:
                fieldnames = [
                    'Date', 'Symbol', 'Name', 'Type', 'Quantity', 'Price', 'Fee', 'Total', 'Amount', 'Currency', 'Notes'
                ]
                writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

                writer.writeheader()
                for txn in transactions:
                    writer.writerow({
                        'Date': txn.trade_date.isoformat(),
                        'Symbol': txn.stock or '',
                        'Name': txn.stock or '',
                        'Type': txn.type,
                        'Quantity': float(txn.quantity) if txn.quantity else 0,
                        'Price': float(txn.price) if txn.price else 0,
                        'Fee': float(txn.fee) if txn.fee else 0,
                        'Total': float(txn.quantity * txn.price + txn.fee) if txn.quantity and txn.price else 0,
                        'Amount': float(txn.amount) if txn.amount else 0,
                        'Currency': txn.currency or 'CAD',
                        'Notes': txn.notes or ''
                    })

        # 读取文件并返回
        with open(file_path, 'r', encoding='utf-8') as f:
            csv_content = f.read()

        filename = os.path.basename(file_path)

        return Response(
            csv_content,
            mimetype='text/csv',
            headers={"Content-disposition": f"attachment; filename={filename}"}
        )

    except Exception as e:
        from flask import flash, redirect, url_for
        flash(f'导出失败: {str(e)}', 'error')
        return redirect(url_for('main.transactions'))


@bp.route('/api/v1/transactions/<int:transaction_id>', methods=['DELETE'])
def delete_transaction(transaction_id):
    """删除单个交易记录"""
    try:
        # 查找交易记录
        transaction = Transaction.query.get_or_404(transaction_id)
        
        # 删除交易
        db.session.delete(transaction)
        db.session.commit()
        
        return jsonify({
            'success': True,
            'message': '交易记录删除成功'
        })
        
    except Exception as e:
        db.session.rollback()
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500

@bp.route('/api/v1/transactions/<int:transaction_id>', methods=['GET'])
def get_transaction(transaction_id):
    """获取单个交易记录"""
    try:
        transaction = Transaction.query.get_or_404(transaction_id)
        return jsonify({
            'success': True,
            'transaction': transaction.to_dict()
        })
    except Exception as e:
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500

@bp.route('/api/v1/transactions/<int:transaction_id>', methods=['PUT'])
def update_transaction(transaction_id):
    """更新单个交易记录"""
    from app.models.transaction import Transaction
    try:
        transaction = Transaction.query.get_or_404(transaction_id)
        data = request.get_json()
        
        # 不允许修改币种
        if 'currency' in data and data['currency'] != transaction.currency:
            return jsonify({
                'success': False,
                'error': "不允许修改交易记录的币种。如需修改币种，请删除原记录并重新创建。"
            }), 400
        
        # 检查股票代码的变化
        updated_stock = data.get('stock', transaction.stock)
        
        # 如果修改了股票代码，验证币种一致性
        if 'stock' in data and data['stock'] != transaction.stock:
            from app.models.transaction import Transaction
            existing_currency = Transaction.get_currency_by_stock_symbol(updated_stock)
            if existing_currency and existing_currency != transaction.currency:
                # 排除当前交易记录
                other_transactions = Transaction.query.filter(
                    Transaction.stock == updated_stock,
                    Transaction.currency == existing_currency,
                    Transaction.id != transaction_id
                ).first()
                if other_transactions:
                    return jsonify({
                        'success': False,
                        'error': f"股票 {updated_stock} 已存在使用 {existing_currency} 币种的交易记录，不允许修改为该股票代码。同一股票代码只能使用一种货币。"
                    }), 400

        # 更新字段
        if 'trade_date' in data:
            from datetime import datetime
            transaction.trade_date = datetime.strptime(data['trade_date'], '%Y-%m-%d').date()
        if 'type' in data:
            transaction.type = data['type']
        if 'stock' in data:
            transaction.stock = data['stock']
        if 'quantity' in data:
            transaction.quantity = float(data['quantity']) if data['quantity'] else 0
        if 'price' in data:
            transaction.price = float(data['price']) if data['price'] else 0
        if 'fee' in data:
            transaction.fee = float(data['fee']) if data['fee'] else 0
        if 'account_id' in data:
            transaction.account_id = data['account_id']
        if 'notes' in data:
            transaction.notes = data['notes']
        if 'amount' in data:
            transaction.amount = float(data['amount']) if data['amount'] else None
        
        db.session.commit()
        
        return jsonify({
            'success': True,
            'message': '交易记录更新成功',
            'transaction': transaction.to_dict()
        })
        
    except Exception as e:
        db.session.rollback()
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500


@bp.route('/transactions/update-notes', methods=['POST'])
def update_transaction_notes():
    """仅更新交易备注（供股票详情页使用）"""
    try:
        payload = request.get_json(silent=True) or {}
        transaction_id = request.form.get('transaction_id') or payload.get('transaction_id')
        notes = request.form.get('notes')
        if notes is None:
            notes = payload.get('notes', '')

        if not transaction_id:
            return jsonify({'success': False, 'error': _('Missing transaction id')}), 400

        try:
            transaction_id = int(transaction_id)
        except (TypeError, ValueError):
            return jsonify({'success': False, 'error': _('Invalid transaction id')}), 400

        notes = notes or ''

        transaction = Transaction.query.get(transaction_id)
        if not transaction:
            return jsonify({'success': False, 'error': _('Transaction not found')}), 404

        transaction.notes = notes
        db.session.commit()

        return jsonify({'success': True, 'message': _('Notes updated successfully')})
    except Exception as e:
        db.session.rollback()
        return jsonify({'success': False, 'error': str(e)}), 500

@bp.route('/api/accounts/<int:account_id>/transactions', methods=['DELETE'])
def delete_account_transactions(account_id):
    """删除指定账户的所有交易记录"""
    try:
        # 验证账户是否存在
        account = Account.query.get_or_404(account_id)
        
        # 删除该账户的所有交易记录
        deleted_count = Transaction.query.filter_by(account_id=account_id).delete()
        
        db.session.commit()
        
        return jsonify({
            'success': True,
            'message': f'Successfully deleted {deleted_count} transactions from account "{account.name}"',
            'deleted_count': deleted_count
        })
        
    except Exception as e:
        db.session.rollback()
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500




@bp.route('/api/v1/holdings')
def api_get_holdings():
    """新的持仓API - 支持灵活的查询参数"""
    try:
        # 获取查询参数
        target = request.args.get('target', 'all')  # 'all', account_id, member_id
        target_type = request.args.get('target_type', 'account')  # 'account' or 'member'
        as_of_date_str = request.args.get('as_of_date')  # YYYY-MM-DD format
        family_id = request.args.get('family_id', type=int)
        
        # 解析日期参数
        as_of_date = None
        if as_of_date_str:
            try:
                from datetime import datetime
                as_of_date = datetime.strptime(as_of_date_str, '%Y-%m-%d').date()
            except ValueError:
                return jsonify({'error': 'Invalid date format. Use YYYY-MM-DD'}), 400
        
        # 解析target参数
        if target != 'all':
            try:
                target = int(target)
            except ValueError:
                return jsonify({'error': 'Invalid target parameter'}), 400
        
        # 获取持仓信息
        portfolio_summary = holdings_service.get_portfolio_summary(
            target=target,
            target_type=target_type,
            as_of_date=as_of_date,
            family_id=family_id
        )
        
        return jsonify({
            'success': True,
            'data': portfolio_summary
        })
        
    except Exception as e:
        logger.error(f"Error in holdings API: {str(e)}")
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500


@bp.route('/api/v1/holdings/snapshot/<int:account_id>')
def api_get_account_holdings_snapshot(account_id):
    """获取特定账户的持仓快照"""
    try:
        as_of_date_str = request.args.get('as_of_date')
        as_of_date = None
        
        if as_of_date_str:
            try:
                from datetime import datetime
                as_of_date = datetime.strptime(as_of_date_str, '%Y-%m-%d').date()
            except ValueError:
                return jsonify({'error': 'Invalid date format. Use YYYY-MM-DD'}), 400
        
        # 获取持仓快照
        snapshot = holdings_service.get_holdings_snapshot(
            target=account_id,
            target_type='account',
            as_of_date=as_of_date
        )
        
        # 获取该账户的所有持仓
        account_holdings = snapshot.get_account_holdings(account_id)
        
        # 转换为API响应格式
        holdings_data = []
        for holding in account_holdings:
            holdings_data.append(holding.to_dict())
        
        return jsonify({
            'success': True,
            'data': {
                'account_id': account_id,
                'as_of_date': snapshot.as_of_date.isoformat(),
                'holdings': holdings_data,
                'total_holdings': len(holdings_data)
            }
        })
        
    except Exception as e:
        logger.error(f"Error in account holdings snapshot API: {str(e)}")
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500


@bp.route('/api/v1/holdings/member/<int:member_id>')
def api_get_member_holdings(member_id):
    """获取特定成员的持仓信息"""
    try:
        as_of_date_str = request.args.get('as_of_date')
        as_of_date = None
        
        if as_of_date_str:
            try:
                from datetime import datetime
                as_of_date = datetime.strptime(as_of_date_str, '%Y-%m-%d').date()
            except ValueError:
                return jsonify({'error': 'Invalid date format. Use YYYY-MM-DD'}), 400
        
        # 获取成员的持仓汇总
        portfolio_summary = holdings_service.get_portfolio_summary(
            target=member_id,
            target_type='member',
            as_of_date=as_of_date
        )
        
        return jsonify({
            'success': True,
            'data': portfolio_summary
        })
        
    except Exception as e:
        logger.error(f"Error in member holdings API: {str(e)}")
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500


@bp.route('/stock/<stock_symbol>')
def stock_detail(stock_symbol):
    """股票详情页面 - 显示价格图表和交易记录"""
    print(f"DEBUG: ============ STOCK DETAIL ROUTE HIT FOR {stock_symbol} ============")
    
    # 获取默认家庭
    from app.models.family import Family
    family = Family.query.first()
    if not family:
        print(f"DEBUG: No family found, redirecting to overview")
        flash(_('No family found'), 'error')
        return redirect(url_for('main.overview'))
    
    print(f"DEBUG: Family found: {family.name if family else 'None'}")
    
    # Get query parameters - do this early to avoid issues
    member_id = request.args.get('member_id', type=int)
    account_id = request.args.get('account_id', type=int)
    
    print(f"DEBUG: Query params - member_id: {member_id}, account_id: {account_id}")
    
    try:
        # 获取所有该股票的交易记录
        from app.models.transaction import Transaction
        all_transactions = Transaction.query.filter_by(stock=stock_symbol.upper()).all()
        print(f"DEBUG: Found {len(all_transactions)} transactions for {stock_symbol.upper()}")
        
        # 如果有过滤条件，应用过滤
        transactions = []
        if member_id:
            from app.models.account import AccountMember
            account_ids = [am.account_id for am in AccountMember.query.filter_by(member_id=member_id).all()]
            transactions = [t for t in all_transactions if t.account_id in account_ids]
        elif account_id:
            transactions = [t for t in all_transactions if t.account_id == account_id]
        else:
            transactions = all_transactions

        # 为每个交易记录添加格式化的账户名称
        if transactions:
            from app.services.asset_valuation_service import AssetValuationService
            asset_service = AssetValuationService(auto_refresh_prices=False)

            for transaction in transactions:
                # 使用资产服务的方法来获取格式化的账户名称
                transaction.formatted_account_name = asset_service._get_account_name_with_members(transaction.account)

        if not transactions:
            # 如果有过滤条件但没找到记录，显示提示信息但不重定向
            if member_id or account_id:
                filter_name = ""
                if member_id:
                    from app.models.member import Member
                    member = Member.query.get(member_id)
                    filter_name = f"成员 {member.name}" if member else f"成员 ID {member_id}"
                elif account_id:
                    from app.models.account import Account
                    account = Account.query.get(account_id)
                    filter_name = f"账户 {account.name}" if account else f"账户 ID {account_id}"
                
                flash(_('No transactions found for stock {} in {}').format(stock_symbol.upper(), filter_name), 'info')
            else:
                flash(_('No transactions found for stock {}').format(stock_symbol.upper()), 'warning')
                # 不重定向，继续显示股票详情页面，让用户可以使用股票修正功能
        
        # 获取股票信息
        from app.models.stocks_cache import StocksCache
        stock_info = StocksCache.query.filter_by(symbol=stock_symbol.upper()).first()
        
        # 如果股票缓存记录不存在但有交易记录，创建一个基础的股票缓存记录用于修正功能
        if not stock_info and all_transactions:
            # 从第一笔交易中推测货币
            first_transaction = all_transactions[0]
            inferred_currency = first_transaction.currency or 'CAD'
            
            # 创建基础股票缓存记录
            stock_info = StocksCache(
                symbol=stock_symbol.upper(),
                name='',  # 空名称，等待用户修正
                exchange='',  # 空交易所，等待用户修正
                currency=inferred_currency
            )
            db.session.add(stock_info)
            db.session.commit()
            print(f"Created basic stock cache record for {stock_symbol.upper()} with currency {inferred_currency}")
        
        # 初始化数据获取失败标志
        price_data_fetch_failed = False
        
        # 1. 智能获取股票历史价格数据（基于实际交易历史）
        price_data = []
        try:
            from app.services.smart_history_manager import SmartHistoryManager
            history_manager = SmartHistoryManager()
            
            # 智能获取历史数据：根据所有交易历史动态确定日期范围
            cached_history = history_manager.get_historical_data_for_stock(
                stock_symbol, all_transactions, family.id, None, None
            )
            
            if cached_history:
                print(f"缓存原始数据样本: {cached_history[:2] if cached_history else 'Empty'}")
                for data in cached_history:
                    price_data.append({
                        'date': data['date'],
                        'price': round(data['close'], 2)
                    })
                print(f"智能历史管理器: 成功获取 {stock_symbol} 的 {len(price_data)} 条价格数据")
                print(f"处理后的价格数据样本: {price_data[:2] if price_data else 'Empty'}")
                
                # 输出日期范围优化摘要
                range_summary = history_manager.get_date_range_summary(
                    stock_symbol, transactions, family_id=family.id, member_id=member_id, account_id=account_id
                )
                print(f"日期范围优化: {range_summary['optimization']}")
            else:
                print(f"智能历史管理器: 未获取到历史数据")
                price_data_fetch_failed = True
            
        except Exception as e:
            print(f"智能历史数据获取失败: {e}")
            price_data_fetch_failed = True
            
        # 如果智能获取失败或数据不足，回退到使用交易价格
        if len(price_data) < 10:
            print(f"缓存数据不足({len(price_data)}条)，回退到交易价格数据")
            from collections import defaultdict
            daily_prices = defaultdict(list)
            
            for transaction in reversed(transactions):
                if transaction.type in ['BUY', 'SELL'] and transaction.price:
                    date_str = transaction.trade_date.strftime('%Y-%m-%d')
                    daily_prices[date_str].append(float(transaction.price))
            
            # 清空缓存数据，使用交易数据
            price_data = []
            for date_str, prices in sorted(daily_prices.items()):
                avg_price = sum(prices) / len(prices)
                price_data.append({
                    'date': date_str,
                    'price': round(avg_price, 2)
                })
        
        # 2. 准备持有数量随时间变化的数据（柱状图）
        quantity_data = []
        from collections import defaultdict
        from decimal import Decimal
        
        # 按日期排序交易并计算累计持有量
        sorted_transactions = sorted(transactions, key=lambda t: t.trade_date)
        cumulative_quantity = Decimal('0')
        daily_quantities = {}
        
        for transaction in sorted_transactions:
            if transaction.type in ['BUY', 'SELL'] and transaction.quantity:
                date_str = transaction.trade_date.strftime('%Y-%m-%d')
                
                if transaction.type == 'BUY':
                    cumulative_quantity += Decimal(str(transaction.quantity))
                elif transaction.type == 'SELL':
                    cumulative_quantity -= Decimal(str(transaction.quantity))
                
                daily_quantities[date_str] = float(cumulative_quantity)
        
        # 转换为图表数据格式
        for date_str, quantity in sorted(daily_quantities.items()):
            quantity_data.append({
                'date': date_str,
                'quantity': round(quantity, 2)
            })
        
        # 使用Portfolio Service统一计算架构计算股票统计信息
        from app.services.portfolio_service import PortfolioService, TimePeriod
        from datetime import date
        
        portfolio_service = PortfolioService(auto_refresh_prices=False)
        stock_stats = {
            'current_shares': 0,
            'avg_cost': 0,
            'total_cost': 0,
            'total_invested': 0,
            'total_received': 0,
            'current_price': 0,
            'market_value': 0,
            'unrealized_pnl': 0,
            'realized_pnl': 0,
            'total_dividends': 0,
            'total_interest': 0,
            'currency': 'CAD'
        }
        
        # 计算当前持仓
        current_holding = None
        
        # 获取涉及的账户ID列表
        account_ids = []
        if member_id:
            from app.models.account import AccountMember
            account_ids = [am.account_id for am in AccountMember.query.filter_by(member_id=member_id).all()]
        elif account_id:
            account_ids = [account_id]
        else:
            # 如果没有过滤条件，获取所有涉及该股票的账户
            account_ids = list(set(t.account_id for t in all_transactions))
        
        if account_ids:
            # 汇总所有相关账户的持仓数据
            total_current_shares = Decimal('0')
            total_cost = Decimal('0')
            total_market_value = Decimal('0')
            total_realized_gain = Decimal('0')
            total_dividends = Decimal('0')
            total_interest = Decimal('0')
            total_bought_value = Decimal('0')
            total_sold_value = Decimal('0')
            currency = 'USD'  # 默认货币，会被实际货币覆盖
            
            # 获取所有账户的持仓快照并汇总
            positions = []
            for account_id in account_ids:
                try:
                    position = portfolio_service.get_position_snapshot(stock_symbol.upper(), account_id, date.today())
                    if position.current_shares > 0 or position.total_sold_shares > 0:
                        positions.append(position)
                        # 汇总数据
                        total_current_shares += position.current_shares
                        total_cost += position.total_cost
                        total_market_value += position.current_value
                        total_realized_gain += position.realized_gain
                        total_dividends += position.total_dividends
                        total_interest += position.total_interest
                        total_bought_value += position.total_bought_value
                        total_sold_value += position.total_sold_value
                        currency = position.currency  # 使用实际货币
                except Exception as e:
                    print(f"获取账户{account_id}的{stock_symbol}持仓快照失败: {e}")
                    continue
            
            if positions:
                # 计算平均成本
                avg_cost = total_cost / total_current_shares if total_current_shares > 0 else Decimal('0')
                
                # 使用汇总后的数据更新stock_stats
                stock_stats.update({
                    'current_shares': float(total_current_shares),
                    'avg_cost': float(avg_cost),
                    'total_cost': float(total_cost),
                    'current_price': float(positions[0].current_price),  # 使用第一个位置的价格（所有位置价格相同）
                    'market_value': float(total_market_value),
                    'unrealized_pnl': float(total_market_value - total_cost),
                    'realized_pnl': float(total_realized_gain),
                    'total_dividends': float(total_dividends),
                    'total_interest': float(total_interest),
                    'currency': currency
                })
                
                # 计算总投资和总收入
                stock_stats['total_invested'] = float(total_bought_value)
                stock_stats['total_received'] = float(total_sold_value)
                
                # 计算收益率
                if total_current_shares > 0:
                    # 当前持仓情况
                    total_return_rate = 0
                    if total_cost > 0:
                        total_return = (total_market_value + total_realized_gain + 
                                      total_dividends + total_interest - total_cost)
                        total_return_rate = float((total_return / total_cost) * 100)
                    stock_stats['total_return_rate'] = total_return_rate
                else:
                    # 零持仓情况
                    zero_holding_return_rate = 0
                    if total_bought_value > 0:
                        total_returns = (total_sold_value + total_dividends + total_interest)
                        zero_holding_return_rate = float(((total_returns - total_bought_value) / 
                                                        total_bought_value) * 100)
                    stock_stats['zero_holding_return_rate'] = zero_holding_return_rate
                
                # 创建current_holding对象用于模板显示
                if total_current_shares > 0:
                    current_holding = {
                        'shares': float(total_current_shares),
                        'avg_cost': float(avg_cost),
                        'total_cost': float(total_cost),
                        'currency': currency
                    }
        
        # 准备交易标记数据（买卖点）- 支持多账户
        transaction_markers = []
        account_id_map = {}  # 映射账户ID到编号
        account_counter = 0
        
        # 计算涉及的账户数量
        unique_accounts = set(t.account_id for t in transactions if t.type in ['BUY', 'SELL'])
        show_account_numbers = len(unique_accounts) > 1
        
        for transaction in transactions:
            if transaction.type in ['BUY', 'SELL'] and transaction.price and transaction.trade_date:
                # 为每个账户分配一个连续编号
                if transaction.account_id not in account_id_map:
                    account_counter += 1
                    account_id_map[transaction.account_id] = account_counter
                
                transaction_markers.append({
                    'date': transaction.trade_date.strftime('%Y-%m-%d'),
                    'price': float(transaction.price),
                    'type': transaction.type,
                    'quantity': float(transaction.quantity) if transaction.quantity else 0,
                    'account_id': transaction.account_id,
                    'account_number': account_id_map[transaction.account_id],
                    'account_name': transaction.account.name if transaction.account else f'Account {transaction.account_id}'
                })
        
        print(f"DEBUG: Passing to template - price_data count: {len(price_data)}, quantity_data count: {len(quantity_data)}")
        print(f"DEBUG: Price data sample: {price_data[:2] if price_data else 'Empty'}")
        print(f"DEBUG: Quantity data sample: {quantity_data[:2] if quantity_data else 'Empty'}")
        print(f"DEBUG: Transaction markers count: {len(transaction_markers)}")
        
        # Get account members for display
        from app.models.account import AccountMember, Account
        from app.models.member import Member
        from app.services.account_service import AccountService
        account_members = AccountMember.query.all()
        accounts = AccountService.get_accounts_display_list()
        members = Member.query.all()
        
        # Convert stock_info to dictionary to avoid JSON serialization issues
        if stock_info:
            stock_info_dict = {
                'id': stock_info.id,
                'symbol': stock_info.symbol,
                'name': stock_info.name,
                'exchange': stock_info.exchange,
                'currency': stock_info.currency,
                'current_price': float(stock_info.current_price) if stock_info.current_price else None,
                'price_updated_at': stock_info.price_updated_at
            }
        else:
            stock_info_dict = None
        
        return render_template('investment/stock_detail.html',
                             title=f'{stock_symbol.upper()} - Stock Detail',
                             stock_symbol=stock_symbol.upper(),
                             stock_info=stock_info_dict,
                             transactions=transactions,
                             price_data=price_data,
                             quantity_data=quantity_data,
                             transaction_markers=transaction_markers,
                             current_holding=current_holding,
                             stock_stats=stock_stats,
                             member_id=member_id,
                             account_id=account_id,
                             family=family,
                             accounts=accounts,
                             members=members,
                             account_members=account_members,
                             show_account_numbers=show_account_numbers,
                             price_data_fetch_failed=price_data_fetch_failed,
                             needs_symbol_correction=stock_info_dict and (not stock_info_dict.get('name') or not stock_info_dict.get('exchange')))
    
        
        print(f"DEBUG: After filtering: {len(transactions)} transactions")
        
        if not transactions:
            flash(_('No transactions found for this stock symbol'), 'info')
            
            # 获取股票信息以正确判断是否需要修正按钮
            from app.models.stocks_cache import StocksCache
            stock_info = StocksCache.query.filter_by(symbol=stock_symbol.upper()).first()
            
            # 转换为字典格式
            if stock_info:
                stock_info_dict = {
                    'id': stock_info.id,
                    'symbol': stock_info.symbol,
                    'name': stock_info.name,
                    'exchange': stock_info.exchange,
                    'currency': stock_info.currency,
                    'current_price': float(stock_info.current_price) if stock_info.current_price else None,
                    'price_updated_at': stock_info.price_updated_at
                }
            else:
                stock_info_dict = None
            
            return render_template('investment/stock_detail.html',
                                 title=f"{stock_symbol.upper()} - No Data",
                                 stock_symbol=stock_symbol.upper(),
                                 stock_info=stock_info_dict,
                                 price_data=[],
                                 quantity_data=[],
                                 transactions=[],
                                 all_transactions=[],
                                 transaction_markers=[],
                                 current_holding=None,
                                 stock_stats={},
                                 accounts=[],
                                 members=[],
                                 account_members=[],
                                 show_account_numbers=False,
                                 price_data_fetch_failed=True,
                                 needs_symbol_correction=stock_info_dict and (not stock_info_dict.get('name') or not stock_info_dict.get('exchange')),
                                 error_message="No transactions found")
        
        # 使用完整的原始实现
        # 获取股票信息
        from app.models.stocks_cache import StocksCache
        stock_info = StocksCache.query.filter_by(symbol=stock_symbol.upper()).first()
        
        # 获取账户和成员信息
        from app.models.account import Account, AccountMember
        from app.models.member import Member
        
        accounts = []
        members = []
        account_members = []
        if current_user.current_family:
            accounts = Account.query.filter_by(family_id=current_user.current_family.id).all()
            members = Member.query.filter_by(family_id=current_user.current_family.id).all()
            account_members = AccountMember.query.join(Account).filter(Account.family_id == current_user.current_family.id).all()
        
        # 获取历史数据和计算持仓等完整逻辑
        try:
            from app.services.smart_history_manager import SmartHistoryManager
            history_manager = SmartHistoryManager()
            
            # 获取股票历史数据
            price_data, _ = history_manager.get_stock_history(
                stock_symbol.upper(), 
                transactions, 
                currency=stock_info.currency if stock_info else 'USD'
            )
            
            price_data_fetch_failed = False
            if not price_data or len(price_data) < 10:
                price_data_fetch_failed = True
                print(f"Price data insufficient or missing for {stock_symbol}")
            
        except Exception as e:
            print(f"智能历史数据获取失败: {e}")
            price_data_fetch_failed = True
            price_data = []
        
        # 如果历史数据获取失败，使用交易价格生成图表数据
        if price_data_fetch_failed or not price_data:
            price_data = []
            for transaction in transactions:
                price_data.append({
                    'date': transaction.trade_date.strftime('%Y-%m-%d'),
                    'price': float(transaction.unit_price)
                })
        
        # 计算累计持仓量数据
        quantity_data = []
        cumulative_quantity = 0.0
        for transaction in sorted(transactions, key=lambda x: x.trade_date):
            if transaction.type == 'BUY':
                cumulative_quantity += float(transaction.quantity)
            elif transaction.type == 'SELL':
                cumulative_quantity -= float(transaction.quantity)
            
            quantity_data.append({
                'date': transaction.trade_date.strftime('%Y-%m-%d'),
                'quantity': cumulative_quantity
            })
        
        # 生成交易标记数据
        account_counter = {}
        transaction_markers = []
        for transaction in transactions:
            if transaction.account_id not in account_counter:
                account_counter[transaction.account_id] = len(account_counter) + 1
            
            transaction_markers.append({
                'date': transaction.trade_date.strftime('%Y-%m-%d'),
                'type': transaction.type,
                'price': float(transaction.unit_price),
                'quantity': float(transaction.quantity),
                'account_id': transaction.account_id,
                'account_number': account_counter[transaction.account_id],
                'account_name': next((acc.account_name for acc in accounts if acc.id == transaction.account_id), 'Unknown')
            })
        
        # 判断是否需要显示账户编号
        unique_accounts = len(set(t.account_id for t in transactions))
        show_account_numbers = unique_accounts > 1
        
        # 计算当前持仓
        current_holding = cumulative_quantity
        
        # 计算股票统计
        total_cost = sum(float(t.unit_price) * float(t.quantity) for t in transactions if t.transaction_type == 'BUY')
        total_sold_value = sum(float(t.unit_price) * float(t.quantity) for t in transactions if t.transaction_type == 'SELL')
        
        stock_stats = {
            'total_transactions': len(transactions),
            'total_cost': total_cost,
            'total_sold_value': total_sold_value,
            'current_holding': current_holding,
            'unique_accounts': unique_accounts
        }
        
        print(f"DEBUG: Passing to template - price_data count: {len(price_data)}, quantity_data count: {len(quantity_data)}")
        print(f"DEBUG: Price data sample: {price_data[:2] if price_data else []}")
        print(f"DEBUG: Quantity data sample: {quantity_data[:2] if quantity_data else []}")
        print(f"DEBUG: Transaction markers count: {len(transaction_markers)}")
        
        # 渲染模板 - 转换stock_info为字典以避免JSON序列化错误
        stock_info_dict = None
        if stock_info:
            stock_info_dict = {
                'id': stock_info.id,
                'symbol': stock_info.symbol,
                'name': stock_info.name,
                'exchange': stock_info.exchange,
                'currency': stock_info.currency,
                'current_price': float(stock_info.current_price) if stock_info.current_price else None
            }
        
        return render_template('investment/stock_detail.html',
                             title=f"{stock_symbol.upper()} - Stock Details",
                             stock_symbol=stock_symbol.upper(),
                             stock_info=stock_info_dict,
                             price_data=price_data,
                             quantity_data=quantity_data,
                             transactions=transactions,
                             all_transactions=all_transactions,
                             transaction_markers=transaction_markers,
                             current_holding=current_holding,
                             stock_stats=stock_stats,
                             accounts=accounts,
                             members=members,
                             account_members=account_members,
                             show_account_numbers=show_account_numbers,
                             price_data_fetch_failed=price_data_fetch_failed,
                             needs_symbol_correction=stock_info_dict and (not stock_info_dict.get('name') or not stock_info_dict.get('exchange')))
    
    except Exception as e:
        print(f"DEBUG: Exception in stock_detail route: {str(e)}")
        print(f"DEBUG: Exception type: {type(e)}")
        import traceback
        print(f"DEBUG: Traceback: {traceback.format_exc()}")
        from flask_babel import _
        flash(_('Error loading stock details: {}').format(str(e)), 'error')
        
        # 获取股票信息以正确判断是否需要修正按钮
        try:
            from app.models.stocks_cache import StocksCache
            stock_info = StocksCache.query.filter_by(symbol=stock_symbol.upper()).first()
            
            # 转换为字典格式
            if stock_info:
                stock_info_dict = {
                    'id': stock_info.id,
                    'symbol': stock_info.symbol,
                    'name': stock_info.name,
                    'exchange': stock_info.exchange,
                    'currency': stock_info.currency,
                    'current_price': float(stock_info.current_price) if stock_info.current_price else None,
                    'price_updated_at': stock_info.price_updated_at
                }
            else:
                stock_info_dict = None
        except:
            stock_info_dict = None
        
        # 不要重定向到overview，而是显示错误页面让用户可以使用修正功能
        return render_template('investment/stock_detail.html',
                             title=f"{stock_symbol.upper()} - Error",
                             stock_symbol=stock_symbol.upper(),
                             stock_info=stock_info_dict,
                             price_data=[],
                             quantity_data=[],
                             transactions=[],
                             all_transactions=[],
                             transaction_markers=[],
                             current_holding=None,
                             stock_stats={},
                             accounts=[],
                             members=[],
                             account_members=[],
                             show_account_numbers=False,
                            price_data_fetch_failed=True,
                            needs_symbol_correction=stock_info_dict and (not stock_info_dict.get('name') or not stock_info_dict.get('exchange')),
                            error_message=str(e))


@bp.route('/test/yfinance')
def test_yfinance():
    """简单测试页面：展示NVDA近一个月历史价格"""
    import yfinance as yf

    rows = []
    error = None

    try:
        ticker = yf.Ticker('NVDA')
        history = ticker.history(period='1mo')

        if history.empty:
            raise ValueError('未获取到NVDA的历史数据。')

        history = history.reset_index()
        for _, record in history.iterrows():
            date_value = record['Date']
            if hasattr(date_value, 'date'):
                date_value = date_value.date()

            rows.append({
                'date': date_value.isoformat() if hasattr(date_value, 'isoformat') else str(date_value),
                'open': float(record.get('Open', 0.0) or 0.0),
                'high': float(record.get('High', 0.0) or 0.0),
                'low': float(record.get('Low', 0.0) or 0.0),
                'close': float(record.get('Close', 0.0) or 0.0),
                'volume': int(record.get('Volume', 0) or 0)
            })
    except Exception as exc:
        logger.exception('获取NVDA历史数据失败: %s', exc)
        error = f"获取NVDA历史数据失败：{exc}"

    return render_template('test/yfinance.html', rows=rows, error=error)


@bp.route('/history-fetcher', methods=['GET', 'POST'])
def fetch_history_tool():
    """手动获取股票历史价格并写入缓存的工具页面"""
    from app.services.stock_history_cache_service import StockHistoryCacheService

    today = date.today()
    default_start = today - timedelta(days=60)

    symbol = ''
    start_date_str = default_start.isoformat()
    end_date_str = today.isoformat()
    currency = 'CAD'
    force_refresh = False

    rows = []
    missing_days = []
    fetch_info = {}
    error = None
    success = False

    if request.method == 'POST':
        symbol = (request.form.get('symbol') or '').strip().upper()
        start_date_str = request.form.get('start_date') or start_date_str
        end_date_str = request.form.get('end_date') or end_date_str
        currency = (request.form.get('currency') or 'CAD').strip().upper()
        force_refresh = request.form.get('force_refresh') == 'on'

        if not symbol:
            error = '请输入股票代码'
        else:
            try:
                start_dt = datetime.strptime(start_date_str, '%Y-%m-%d').date()
                end_dt = datetime.strptime(end_date_str, '%Y-%m-%d').date()
                if start_dt > end_dt:
                    raise ValueError('开始日期不能晚于结束日期')
            except ValueError as exc:
                error = f'日期格式错误：{exc}'
            else:
                service = StockHistoryCacheService()
                try:
                    history_records = service.get_history(symbol, start_dt, end_dt, currency, force_refresh=force_refresh)
                    rows = history_records
                    success = True

                    fetch_info = {
                        'symbol': symbol,
                        'currency': currency,
                        'start': start_dt.isoformat(),
                        'end': end_dt.isoformat(),
                        'count': len(history_records)
                    }

                    existing_dates = set()
                    for record in history_records:
                        record_date = record.get('date') or record.get('trade_date')
                        if record_date:
                            try:
                                existing_dates.add(datetime.strptime(record_date, '%Y-%m-%d').date())
                            except ValueError:
                                continue

                    sorted_dates = sorted(existing_dates)
                    market = service._get_market(symbol, currency)

                    missing_candidates = service._get_missing_trading_days(existing_dates, start_dt, end_dt)

                    for missing in missing_candidates:
                        StockHolidayAttempt.record_attempt(symbol, market, missing, has_data=False)
                        if StockHolidayAttempt.should_promote_to_holiday(missing, market):
                            MarketHoliday.add_holiday_detection(missing, market, symbol)

                    missing_days = [d.isoformat() for d in missing_candidates]
                    fetch_info['missing_count'] = len(missing_days)
                except Exception as exc:
                    current_app.logger.exception('获取历史数据失败: %s', exc)
                    error = f'获取历史数据失败：{exc}'
                    rows = []
                    missing_days = []
                    success = False

    return render_template(
        'test/history_fetch_tool.html',
        symbol=symbol,
        start_date=start_date_str,
        end_date=end_date_str,
        currency=currency,
        force_refresh=force_refresh,
        rows=rows,
        missing_days=missing_days,
        fetch_info=fetch_info,
        error=error,
        success=success
    )


@bp.route('/api/portfolio/async-prices', methods=['POST'])
def get_async_portfolio_prices():
    """异步获取投资组合股票价格API"""
    try:
        from app.services.asset_valuation_service import AssetValuationService
        from app.services.holdings_service import HoldingsService
        
        # 获取请求参数
        data = request.get_json()
        account_ids = data.get('account_ids', [])
        member_id = data.get('member_id')
        family_id = data.get('family_id')
        force_refresh = data.get('force_refresh', False)  # 添加强制刷新参数
        
        if not account_ids:
            return jsonify({
                'success': False,
                'error': 'No account IDs provided'
            }), 400
        
        # 获取基础持仓数据（不包含价格）
        holdings_service = HoldingsService()
        holdings_snapshot = holdings_service.get_holdings_snapshot(
            target=account_ids,
            target_type='account',
            as_of_date=None,
            family_id=family_id
        )
        
        # 获取需要更新价格的股票列表（去重）
        stock_symbols = set()  # 使用set自动去重
        for symbol, account_holdings in holdings_snapshot.holdings.items():
            for account_id, holding in account_holdings.items():
                if holding.current_shares > 0:
                    # 从holding对象获取货币信息
                    currency = getattr(holding, 'currency', 'USD')
                    stock_symbols.add((symbol, currency))  # 使用add方法，自动去重
        
        # 转换为列表
        stock_symbols = list(stock_symbols)
        
        if not stock_symbols:
            return jsonify({
                'success': True,
                'holdings': [],
                'message': 'No holdings found'
            })
        
        # 异步更新股票价格
        from app.services.stock_price_service import StockPriceService
        stock_service = StockPriceService()
        
        # 批量更新价格
        update_results = stock_service.update_prices_for_symbols(stock_symbols, force_refresh=force_refresh)
        
        # 确保数据库事务已提交
        from app import db
        db.session.commit()
        
        # 等待一小段时间确保所有价格更新都完成
        import time
        time.sleep(0.1)
        
        # 使用Portfolio Service重新计算持仓价值
        from app.services.portfolio_service import PortfolioService, TimePeriod
        portfolio_service = PortfolioService(auto_refresh_prices=False)
        portfolio_summary = portfolio_service.get_portfolio_summary(account_ids, TimePeriod.ALL_TIME)

        accounts = Account.query.filter(Account.id.in_(account_ids)).all()
        ownership_map = _build_ownership_map(member_id) if member_id else None
        asset_service = AssetValuationService(auto_refresh_prices=False)
        view_data = _build_portfolio_view_data(
            account_ids=account_ids,
            accounts=accounts,
            portfolio_summary=portfolio_summary,
            asset_service=asset_service,
            ownership_map=ownership_map,
            target_date=date.today(),
            exchange_rates=currency_service.get_cad_usd_rates(),
            account_id=None
        )

        holdings = view_data['holdings']
        cleared_holdings = view_data['cleared_holdings']
        comprehensive_metrics = view_data['metrics']

        return jsonify({
            'success': True,
            'holdings': holdings,
            'cleared_holdings': cleared_holdings,
            'metrics': comprehensive_metrics,
            'daily_change': view_data['daily_change'],
            'exchange_rates': {},
            'update_results': update_results
        })
        
    except Exception as e:
        current_app.logger.error(f"异步获取股票价格失败: {str(e)}", exc_info=True)
        return jsonify({
            'success': False,
            'error': f'Failed to update stock prices: {str(e)}'
        }), 500
