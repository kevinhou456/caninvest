#!/usr/bin/env python3

import sys
import os
sys.path.append('/Users/luzhang/Desktop/web1/canadian_family_investment')

from datetime import date
from app import create_app
from app.services.portfolio_service import PortfolioService
from decimal import Decimal

def debug_2024_unrealized():
    app = create_app()

    with app.app_context():
        portfolio_service = PortfolioService()
        account_ids = [4]  # 账户4

        # 2024年年初和年末
        year_start = date(2024, 1, 1)
        year_end = date(2024, 12, 31)

        print("=== 账户4 2024年未实现收益调试 ===")

        # 获取年初数据
        print(f"\n--- {year_start} 年初数据 ---")
        from app.services.portfolio_service import TimePeriod
        year_start_portfolio = portfolio_service.get_portfolio_summary(
            account_ids, TimePeriod.CUSTOM, year_start, year_start
        )
        year_start_unrealized = year_start_portfolio.get('summary', {}).get('total_unrealized_gain', 0)
        print(f"年初总未实现收益: {year_start_unrealized}")

        # 显示年初持仓详情
        year_start_holdings = year_start_portfolio.get('current_holdings', [])
        print(f"年初持仓数量: {len(year_start_holdings)}")
        for holding in year_start_holdings:
            symbol = holding.get('symbol')
            shares = holding.get('current_shares')
            cost = holding.get('total_cost')
            current_val = holding.get('current_value')
            unrealized = holding.get('unrealized_gain')
            print(f"  {symbol}: {shares}股, 成本={cost}, 市值={current_val}, 未实现={unrealized}")

        # 获取年末数据
        print(f"\n--- {year_end} 年末数据 ---")
        year_end_portfolio = portfolio_service.get_portfolio_summary(
            account_ids, TimePeriod.CUSTOM, year_end, year_end
        )
        year_end_unrealized = year_end_portfolio.get('summary', {}).get('total_unrealized_gain', 0)
        print(f"年末总未实现收益: {year_end_unrealized}")

        # 显示年末持仓详情
        year_end_holdings = year_end_portfolio.get('current_holdings', [])
        print(f"年末持仓数量: {len(year_end_holdings)}")
        for holding in year_end_holdings:
            symbol = holding.get('symbol')
            shares = holding.get('current_shares')
            cost = holding.get('total_cost')
            current_val = holding.get('current_value')
            unrealized = holding.get('unrealized_gain')
            print(f"  {symbol}: {shares}股, 成本={cost}, 市值={current_val}, 未实现={unrealized}")

        # 计算2024年度增量未实现收益
        annual_unrealized_gain = year_end_unrealized - year_start_unrealized
        print(f"\n--- 2024年度增量未实现收益 ---")
        print(f"年末未实现收益: {year_end_unrealized}")
        print(f"年初未实现收益: {year_start_unrealized}")
        print(f"2024年度增量: {annual_unrealized_gain}")

        # 获取当前时点数据作为对比
        print(f"\n--- 当前时点数据 ---")
        current_portfolio = portfolio_service.get_portfolio_summary(account_ids)
        current_unrealized = current_portfolio.get('summary', {}).get('total_unrealized_gain', 0)
        print(f"当前总未实现收益: {current_unrealized}")

if __name__ == '__main__':
    debug_2024_unrealized()