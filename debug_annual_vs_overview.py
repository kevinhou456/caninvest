#!/usr/bin/env python3

import sys
import os
sys.path.append('/Users/luzhang/Desktop/web1/canadian_family_investment')

from datetime import date
from app import create_app
from app.services.portfolio_service import PortfolioService

def debug_annual_vs_overview():
    app = create_app()

    with app.app_context():
        portfolio_service = PortfolioService()
        account_ids = [4]  # 账户4

        print("=== 账户4 年度未实现收益总和 vs Overview 对比 ===")

        # 获取年度分析数据
        print("\n--- 年度分析数据 ---")
        annual_analysis = portfolio_service.get_annual_analysis(account_ids)
        annual_data = annual_analysis.get('annual_data', [])

        print("各年度未实现收益:")
        total_annual_unrealized = 0
        for year_data in annual_data:
            year = year_data.get('year')
            annual_unrealized = year_data.get('annual_unrealized_gain', 0)
            is_member_row = year_data.get('is_member_row', False)
            member_name = year_data.get('member_name', '')

            if is_member_row:
                print(f"  {year}年 ({member_name}成员): {annual_unrealized}")
            else:
                print(f"  {year}年 (联合账户): {annual_unrealized}")
                total_annual_unrealized += annual_unrealized  # 只累加联合账户数据

        print(f"年度增量未实现收益总和: {total_annual_unrealized}")

        # 获取summary中的total_unrealized_gain
        summary_unrealized = annual_analysis.get('summary', {}).get('total_unrealized_gain', 0)
        print(f"Summary中的总未实现收益: {summary_unrealized}")

        # 获取overview数据
        print("\n--- Overview数据 ---")
        overview_portfolio = portfolio_service.get_portfolio_summary(account_ids)
        overview_unrealized = overview_portfolio.get('summary', {}).get('total_unrealized_gain', 0)
        print(f"Overview总未实现收益: {overview_unrealized}")

        # 对比
        print(f"\n--- 对比结果 ---")
        print(f"年度增量总和: {total_annual_unrealized}")
        print(f"Summary总未实现: {summary_unrealized}")
        print(f"Overview总未实现: {overview_unrealized}")
        print(f"年度增量总和 vs Overview差异: {total_annual_unrealized - overview_unrealized}")
        print(f"Summary vs Overview差异: {summary_unrealized - overview_unrealized}")

if __name__ == '__main__':
    debug_annual_vs_overview()