#!/usr/bin/env python3

import sys
import os
sys.path.append('/Users/luzhang/Desktop/web1/canadian_family_investment')

from datetime import date
from app import create_app
from app.services.portfolio_service import PortfolioService

def debug_daily_unrealized():
    app = create_app()

    with app.app_context():
        portfolio_service = PortfolioService()
        account_ids = [2]  # 账户2

        print("=== 账户2今日浮动盈亏调试 ===")

        today = date.today()
        print(f"今日: {today}")

        # 获取今日的投资组合摘要
        print(f"\n--- 今日投资组合摘要 ---")
        today_portfolio = portfolio_service.get_portfolio_summary(account_ids)
        today_summary = today_portfolio.get('summary', {})

        print(f"当前市值: ${today_summary.get('total_current_value', 0)}")
        print(f"总成本: ${today_summary.get('total_cost', 0)}")
        print(f"总未实现收益: ${today_summary.get('total_unrealized_gain', 0)}")
        print(f"总收益率: {today_summary.get('total_return_percent', 0):.2f}%")

        # 获取昨日的投资组合摘要
        from datetime import timedelta
        yesterday = today - timedelta(days=1)
        print(f"\n--- 昨日投资组合摘要 ({yesterday}) ---")

        from app.services.portfolio_service import TimePeriod
        yesterday_portfolio = portfolio_service.get_portfolio_summary(
            account_ids, TimePeriod.CUSTOM, yesterday, yesterday)
        yesterday_summary = yesterday_portfolio.get('summary', {})

        print(f"昨日市值: ${yesterday_summary.get('total_current_value', 0)}")
        print(f"昨日成本: ${yesterday_summary.get('total_cost', 0)}")
        print(f"昨日未实现收益: ${yesterday_summary.get('total_unrealized_gain', 0)}")

        # 计算今日浮动盈亏
        print(f"\n--- 今日浮动盈亏计算 ---")
        today_unrealized = today_summary.get('total_unrealized_gain', 0)
        yesterday_unrealized = yesterday_summary.get('total_unrealized_gain', 0)
        daily_change = today_unrealized - yesterday_unrealized

        print(f"今日未实现收益: ${today_unrealized}")
        print(f"昨日未实现收益: ${yesterday_unrealized}")
        print(f"今日浮动盈亏: ${daily_change}")

        # 检查是否有今日的交易影响
        print(f"\n--- 今日交易检查 ---")
        from app.models.transaction import Transaction

        today_transactions = Transaction.query.filter(
            Transaction.account_id.in_(account_ids),
            Transaction.trade_date == today
        ).all()

        print(f"今日交易数: {len(today_transactions)}")
        for tx in today_transactions:
            print(f"  {tx.type} {tx.stock or 'N/A'} {tx.quantity or 0}股 @${tx.price or 0}")

        # 检查持仓详情变化
        print(f"\n--- 持仓详情对比 ---")
        today_holdings = today_portfolio.get('current_holdings', [])
        yesterday_holdings = yesterday_portfolio.get('current_holdings', [])

        print(f"今日持仓数: {len(today_holdings)}")
        print(f"昨日持仓数: {len(yesterday_holdings)}")

        # 按股票代码对比
        today_by_symbol = {h.get('symbol'): h for h in today_holdings}
        yesterday_by_symbol = {h.get('symbol'): h for h in yesterday_holdings}

        all_symbols = set(today_by_symbol.keys()) | set(yesterday_by_symbol.keys())

        print(f"\n持仓变化详情:")
        total_value_change = 0
        total_unrealized_change = 0

        for symbol in sorted(all_symbols):
            today_holding = today_by_symbol.get(symbol, {})
            yesterday_holding = yesterday_by_symbol.get(symbol, {})

            today_value = today_holding.get('current_value', 0)
            yesterday_value = yesterday_holding.get('current_value', 0)
            today_unrealized = today_holding.get('unrealized_gain', 0)
            yesterday_unrealized = yesterday_holding.get('unrealized_gain', 0)

            value_change = today_value - yesterday_value
            unrealized_change = today_unrealized - yesterday_unrealized

            total_value_change += value_change
            total_unrealized_change += unrealized_change

            if abs(value_change) > 0.01 or abs(unrealized_change) > 0.01:
                print(f"  {symbol}:")
                print(f"    市值变化: ${yesterday_value} -> ${today_value} (变化: ${value_change:+.2f})")
                print(f"    未实现变化: ${yesterday_unrealized} -> ${today_unrealized} (变化: ${unrealized_change:+.2f})")

        print(f"\n总计:")
        print(f"  市值变化: ${total_value_change:+.2f}")
        print(f"  未实现收益变化: ${total_unrealized_change:+.2f}")

        # 检查现金余额变化
        print(f"\n--- 现金余额检查 ---")
        from app.services.asset_valuation_service import AssetValuationService
        asset_service = AssetValuationService()

        today_cad, today_usd = asset_service._calculate_cash_balance(2, today)
        yesterday_cad, yesterday_usd = asset_service._calculate_cash_balance(2, yesterday)

        print(f"今日现金: CAD=${today_cad}, USD=${today_usd}")
        print(f"昨日现金: CAD=${yesterday_cad}, USD=${yesterday_usd}")
        print(f"现金变化: CAD=${today_cad - yesterday_cad:+}, USD=${today_usd - yesterday_usd:+}")

if __name__ == '__main__':
    debug_daily_unrealized()