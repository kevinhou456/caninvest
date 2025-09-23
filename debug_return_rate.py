#!/usr/bin/env python3

import sys
import os
sys.path.append('/Users/luzhang/Desktop/web1/canadian_family_investment')

from datetime import date
from app import create_app
from app.services.portfolio_service import PortfolioService

def debug_return_rate():
    app = create_app()

    with app.app_context():
        portfolio_service = PortfolioService()
        account_ids = [2]  # 账户2

        print("=== 账户2收益率计算调试 ===")

        # 获取年度分析数据
        annual_analysis = portfolio_service.get_annual_analysis(account_ids)
        annual_data = annual_analysis.get('annual_data', [])

        print("\n--- 年度数据详情 ---")
        for item in annual_data:
            if not item.get('is_member_row', False):
                year = item.get('year')
                total_assets = item.get('total_assets', 0)
                annual_realized = item.get('annual_realized_gain', 0)
                annual_unrealized = item.get('annual_unrealized_gain', 0)
                annual_dividends = item.get('annual_dividends', 0)
                annual_interest = item.get('annual_interest', 0)

                # 计算总收益
                total_returns = annual_realized + annual_unrealized + annual_dividends + annual_interest

                # 计算成本基础
                cost_basis = total_assets - annual_unrealized

                # 计算收益率
                return_rate = (total_returns / cost_basis * 100) if cost_basis > 0 else 0

                print(f"\n{year}年:")
                print(f"  总资产: {total_assets}")
                print(f"  年度已实现收益: {annual_realized}")
                print(f"  年度未实现收益: {annual_unrealized}")
                print(f"  年度分红: {annual_dividends}")
                print(f"  年度利息: {annual_interest}")
                print(f"  总收益: {total_returns}")
                print(f"  成本基础: {cost_basis}")
                print(f"  收益率: {return_rate:.2f}%")

        # 获取Performance比较数据
        print(f"\n--- Performance比较数据 ---")
        try:
            performance_data = portfolio_service.get_performance_comparison(account_ids)
            annual_performance = performance_data.get('annual_data', [])

            for item in annual_performance:
                if not item.get('is_member_row', False):
                    year = item.get('year')
                    return_percent = item.get('annual_return_percent', 0)
                    print(f"{year}年Performance收益率: {return_percent:.2f}%")

        except Exception as e:
            print(f"获取Performance数据出错: {e}")

        # 手动验证计算
        print(f"\n--- 手动验证Overview数据 ---")
        overview = portfolio_service.get_portfolio_summary(account_ids)
        overview_summary = overview.get('summary', {})

        current_value = overview_summary.get('total_current_value', 0)
        total_cost = overview_summary.get('total_cost', 0)
        total_unrealized = overview_summary.get('total_unrealized_gain', 0)
        overall_return_percent = overview_summary.get('total_return_percent', 0)

        print(f"Overview当前市值: {current_value}")
        print(f"Overview总成本: {total_cost}")
        print(f"Overview总未实现收益: {total_unrealized}")
        print(f"Overview收益率: {overall_return_percent:.2f}%")

        # 验证计算
        manual_return_rate = (total_unrealized / total_cost * 100) if total_cost > 0 else 0
        print(f"手动计算收益率: {manual_return_rate:.2f}%")

if __name__ == '__main__':
    debug_return_rate()