"""
报告和分析API
"""

from flask import request, jsonify
from flask_babel import _
from datetime import datetime, timedelta
from app.models.family import Family
from app.models.member import Member
from app.models.account import Account, AccountMember
# from app.models.holding import CurrentHolding  # CurrentHolding model deleted
from app.models.transaction import Transaction
from app.models.contribution import Contribution
from . import bp


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
    accounts_data = []
    for account in family.accounts:
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
    for account in family.accounts:
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
    
    for account in family.accounts:
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
    years_param = request.args.get('years')
    
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
        account_ids = [acc.id for acc in family.accounts]
    
    # 解析年份参数
    years = None
    if years_param:
        try:
            years = [int(year.strip()) for year in years_param.split(',')]
        except ValueError:
            return jsonify({'error': 'Invalid years parameter format'}), 400
    
    try:
        # 调用统一的投资组合服务
        from app.services.portfolio_service import portfolio_service
        analysis_data = portfolio_service.get_annual_analysis(account_ids, years, member_id=member_id)
        
        return jsonify({
            'family': family.to_dict(),
            'filter_info': {
                'member_id': member_id,
                'account_id': account_id,
                'account_count': len(account_ids)
            },
            'analysis': analysis_data
        })
        
    except Exception as e:
        return jsonify({
            'success': False,
            'error': f'Failed to generate annual analysis: {str(e)}'
        }), 500

@bp.route('/families/<int:family_id>/reports/quarterly-analysis', methods=['GET'])
def get_family_quarterly_analysis(family_id):
    """获取家庭季度分析报告"""
    family = Family.query.get_or_404(family_id)
    
    # 获取参数
    member_id = request.args.get('member_id', type=int)
    account_id = request.args.get('account_id', type=int)
    years_param = request.args.get('years')
    
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
        account_ids = [acc.id for acc in family.accounts]
    
    # 解析年份参数
    years = None
    if years_param:
        try:
            years = [int(year.strip()) for year in years_param.split(',')]
        except ValueError:
            return jsonify({'error': 'Invalid years parameter format'}), 400
    
    try:
        # 调用统一的投资组合服务
        from app.services.portfolio_service import portfolio_service
        analysis_data = portfolio_service.get_quarterly_analysis(account_ids, years, member_id=member_id)
        
        return jsonify({
            'family': family.to_dict(),
            'filter_info': {
                'member_id': member_id,
                'account_id': account_id,
                'account_count': len(account_ids)
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
    months = request.args.get('months', type=int, default=12)
    
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
        account_ids = [acc.id for acc in family.accounts]
    
    try:
        # 调用统一的投资组合服务
        from app.services.portfolio_service import portfolio_service
        analysis_data = portfolio_service.get_monthly_analysis(account_ids, months, member_id=member_id)
        
        return jsonify({
            'family': family.to_dict(),
            'filter_info': {
                'member_id': member_id,
                'account_id': account_id,
                'account_count': len(account_ids)
            },
            'analysis': analysis_data
        })
        
    except Exception as e:
        return jsonify({
            'success': False,
            'error': f'Failed to generate monthly analysis: {str(e)}'
        }), 500

@bp.route('/families/<int:family_id>/reports/daily-analysis', methods=['GET'])
def get_family_daily_analysis(family_id):
    """获取家庭日度分析报告"""
    family = Family.query.get_or_404(family_id)
    
    # 获取参数
    member_id = request.args.get('member_id', type=int)
    account_id = request.args.get('account_id', type=int)
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
        account_ids = [acc.id for acc in family.accounts]
    
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
                'account_count': len(account_ids)
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
        account_ids = [acc.id for acc in family.accounts]
    range_param = request.args.get('range', '1m')

    try:
        # 调用统一的投资组合服务
        from app.services.portfolio_service import portfolio_service
        analysis_data = portfolio_service.get_performance_comparison(account_ids, range_param, member_id=member_id,
                                                                     return_type=request.args.get('return_type', 'mwr'))

        return jsonify({
            'family': family.to_dict(),
            'filter_info': {
                'member_id': member_id,
                'account_id': account_id,
                'account_count': len(account_ids)
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
        account_ids = [acc.id for acc in family.accounts]
    
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
                'account_count': len(account_ids)
            },
            'distribution': distribution_data
        })
        
    except Exception as e:
        return jsonify({
            'success': False,
            'error': f'Failed to generate holdings distribution: {str(e)}'
        }), 500
