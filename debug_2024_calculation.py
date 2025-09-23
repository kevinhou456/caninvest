#!/usr/bin/env python3

import sys
import os
sys.path.append('/Users/luzhang/Desktop/web1/canadian_family_investment')

from datetime import date
from app import create_app
from app.services.portfolio_service import PortfolioService, TimePeriod

def debug_2024_calculation():
    app = create_app()

    with app.app_context():
        portfolio_service = PortfolioService()
        account_ids = [4]  # 账户4

        print("=== 调试2024年年度计算逻辑 ===")

        # 检查年度分析中2024年使用的基准点
        year_start_2024 = date(2024, 1, 1)
        year_end_2024 = date(2024, 12, 31)

        print(f"\n--- 年度分析使用的基准点 ---")
        print(f"2024年开始: {year_start_2024}")
        print(f"2024年结束: {year_end_2024}")

        # 年初数据
        year_start_portfolio = portfolio_service.get_portfolio_summary(
            account_ids, TimePeriod.CUSTOM, year_start_2024, year_start_2024
        )
        year_start_unrealized = year_start_portfolio.get('summary', {}).get('total_unrealized_gain', 0)
        print(f"年度分析的2024年1月1日未实现收益: {year_start_unrealized}")

        # 年末数据
        year_end_portfolio = portfolio_service.get_portfolio_summary(
            account_ids, TimePeriod.CUSTOM, year_end_2024, year_end_2024
        )
        year_end_unrealized = year_end_portfolio.get('summary', {}).get('total_unrealized_gain', 0)
        print(f"年度分析的2024年12月31日未实现收益: {year_end_unrealized}")

        # 年度分析的计算
        annual_calculated = year_end_unrealized - year_start_unrealized
        print(f"年度分析计算: {year_end_unrealized} - ({year_start_unrealized}) = {annual_calculated}")

        # 正确的基准应该是什么？
        print(f"\n--- 正确的基准点分析 ---")

        # 如果第一条交易是2024年1月1日，那么2023年底应该是0
        base_2023 = date(2023, 12, 31)
        portfolio_2023 = portfolio_service.get_portfolio_summary(
            account_ids, TimePeriod.CUSTOM, base_2023, base_2023
        )
        unrealized_2023 = portfolio_2023.get('summary', {}).get('total_unrealized_gain', 0)
        print(f"2023年12月31日未实现收益: {unrealized_2023}")

        # 正确的2024年增量应该是
        correct_2024_increment = year_end_unrealized - unrealized_2023
        print(f"正确的2024年增量: {year_end_unrealized} - {unrealized_2023} = {correct_2024_increment}")

        # 差异分析
        print(f"\n--- 差异分析 ---")
        print(f"年度分析计算的2024年增量: {annual_calculated}")
        print(f"正确的2024年增量: {correct_2024_increment}")
        print(f"差异: {annual_calculated - correct_2024_increment}")

        # 问题可能在于年度分析使用了错误的年初基准
        print(f"\n--- 问题分析 ---")
        print(f"年度分析使用的年初值: {year_start_unrealized}")
        print(f"这个值看起来是错误的，应该是0或者从前一年末继承")

        # 检查2024年1月1日的持仓详情
        print(f"\n--- 2024年1月1日持仓详情 ---")
        year_start_holdings = year_start_portfolio.get('current_holdings', [])
        print(f"持仓数量: {len(year_start_holdings)}")
        for holding in year_start_holdings[:5]:  # 只显示前5个
            symbol = holding.get('symbol')
            shares = holding.get('current_shares')
            cost = holding.get('total_cost')
            current_val = holding.get('current_value')
            unrealized = holding.get('unrealized_gain')
            print(f"  {symbol}: {shares}股, 成本={cost}, 市值={current_val}, 未实现={unrealized}")

if __name__ == '__main__':
    debug_2024_calculation()