#!/usr/bin/env python3

import sys
import os
sys.path.append('/Users/luzhang/Desktop/web1/canadian_family_investment')

from datetime import date
from app import create_app
from app.services.portfolio_service import PortfolioService, TimePeriod

def debug_2024_details():
    app = create_app()

    with app.app_context():
        portfolio_service = PortfolioService()
        account_ids = [4]  # 账户4

        print("=== 账户4 2024年浮动盈亏详细调试 ===")

        # 获取年度分析数据
        annual_analysis = portfolio_service.get_annual_analysis(account_ids)
        annual_data = annual_analysis.get('annual_data', [])

        # 找到2024年的数据
        data_2024 = None
        for item in annual_data:
            if item.get('year') == 2024 and not item.get('is_member_row', False):
                data_2024 = item
                break

        if data_2024:
            print("\n--- 2024年年度分析数据 ---")
            print(f"年度未实现收益: {data_2024.get('annual_unrealized_gain')}")
            print(f"总资产: {data_2024.get('total_assets')}")
            print(f"年度已实现收益: {data_2024.get('annual_realized_gain')}")
            print(f"年度分红: {data_2024.get('annual_dividends')}")
            print(f"年度利息: {data_2024.get('annual_interest')}")

        # 获取2024年的季度分析
        print("\n--- 2024年季度分析数据 ---")
        quarterly_analysis = portfolio_service.get_quarterly_analysis(account_ids, [2024])
        quarterly_data = quarterly_analysis.get('quarterly_data', [])

        total_quarterly_unrealized = 0
        for quarter_data in quarterly_data:
            if quarter_data.get('year') == 2024 and not quarter_data.get('is_member_row', False):
                quarter = quarter_data.get('quarter')
                quarterly_unrealized = quarter_data.get('quarterly_unrealized_gain', 0)
                total_quarterly_unrealized += quarterly_unrealized
                print(f"Q{quarter}: {quarterly_unrealized}")

        print(f"2024年四个季度未实现收益总和: {total_quarterly_unrealized}")

        # 对比
        if data_2024:
            annual_unrealized = data_2024.get('annual_unrealized_gain', 0)
            print(f"\n--- 对比结果 ---")
            print(f"年度未实现收益: {annual_unrealized}")
            print(f"季度未实现收益总和: {total_quarterly_unrealized}")
            print(f"差异: {annual_unrealized - total_quarterly_unrealized}")

        # 手动验证：2024年年初和年末的实际数据
        print(f"\n--- 手动验证 ---")
        year_start = date(2024, 1, 1)
        year_end = date(2024, 12, 31)

        year_start_portfolio = portfolio_service.get_portfolio_summary(
            account_ids, TimePeriod.CUSTOM, year_start, year_start
        )
        year_end_portfolio = portfolio_service.get_portfolio_summary(
            account_ids, TimePeriod.CUSTOM, year_end, year_end
        )

        year_start_unrealized = year_start_portfolio.get('summary', {}).get('total_unrealized_gain', 0)
        year_end_unrealized = year_end_portfolio.get('summary', {}).get('total_unrealized_gain', 0)
        manual_annual_unrealized = year_end_unrealized - year_start_unrealized

        print(f"2024年1月1日未实现收益: {year_start_unrealized}")
        print(f"2024年12月31日未实现收益: {year_end_unrealized}")
        print(f"手动计算年度增量: {manual_annual_unrealized}")

if __name__ == '__main__':
    debug_2024_details()