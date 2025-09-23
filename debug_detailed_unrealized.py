#!/usr/bin/env python3

import sys
import os
sys.path.append('/Users/luzhang/Desktop/web1/canadian_family_investment')

from datetime import date
from app import create_app
from app.services.portfolio_service import PortfolioService, TimePeriod

def debug_detailed_unrealized():
    app = create_app()

    with app.app_context():
        portfolio_service = PortfolioService()
        account_ids = [4]  # 账户4

        print("=== 账户4年度计算 vs Overview详细分析 ===")

        # 1. 获取年度分析数据
        print("\n--- 1. 年度分析数据 ---")
        annual_analysis = portfolio_service.get_annual_analysis(account_ids)
        annual_data = annual_analysis.get('annual_data', [])
        summary = annual_analysis.get('summary', {})

        total_annual_unrealized = 0
        for item in annual_data:
            if not item.get('is_member_row', False):
                year = item.get('year')
                annual_unrealized = item.get('annual_unrealized_gain', 0)
                total_annual_unrealized += annual_unrealized
                print(f"  {year}年增量未实现收益: {annual_unrealized}")

        print(f"年度增量未实现收益总和: {total_annual_unrealized}")
        print(f"Summary中的总未实现收益: {summary.get('total_unrealized_gain', 0)}")

        # 2. 获取Overview数据
        print("\n--- 2. Overview数据 ---")
        overview_portfolio = portfolio_service.get_portfolio_summary(account_ids)
        overview_unrealized = overview_portfolio.get('summary', {}).get('total_unrealized_gain', 0)
        print(f"Overview总未实现收益: {overview_unrealized}")

        # 3. 手动验证每年的计算
        print("\n--- 3. 手动验证每年的计算 ---")

        # 2023年底作为基准点
        print("2023年底基准点:")
        base_2023 = date(2023, 12, 31)
        portfolio_2023 = portfolio_service.get_portfolio_summary(
            account_ids, TimePeriod.CUSTOM, base_2023, base_2023
        )
        unrealized_2023 = portfolio_2023.get('summary', {}).get('total_unrealized_gain', 0)
        print(f"  2023年底未实现收益: {unrealized_2023}")

        # 2024年底
        print("2024年底:")
        end_2024 = date(2024, 12, 31)
        portfolio_2024 = portfolio_service.get_portfolio_summary(
            account_ids, TimePeriod.CUSTOM, end_2024, end_2024
        )
        unrealized_2024 = portfolio_2024.get('summary', {}).get('total_unrealized_gain', 0)
        print(f"  2024年底未实现收益: {unrealized_2024}")
        print(f"  2024年增量: {unrealized_2024 - unrealized_2023}")

        # 2025年底（当前）
        print("2025年当前:")
        current_portfolio = portfolio_service.get_portfolio_summary(account_ids)
        current_unrealized = current_portfolio.get('summary', {}).get('total_unrealized_gain', 0)
        print(f"  当前未实现收益: {current_unrealized}")
        print(f"  2025年增量: {current_unrealized - unrealized_2024}")

        # 4. 验证增量计算逻辑
        print("\n--- 4. 验证增量计算逻辑 ---")
        manual_2024_increment = unrealized_2024 - unrealized_2023
        manual_2025_increment = current_unrealized - unrealized_2024
        manual_total = manual_2024_increment + manual_2025_increment

        print(f"手动计算2024年增量: {manual_2024_increment}")
        print(f"手动计算2025年增量: {manual_2025_increment}")
        print(f"手动计算总增量: {manual_total}")
        print(f"基准点 + 总增量: {unrealized_2023} + {manual_total} = {unrealized_2023 + manual_total}")
        print(f"应该等于当前未实现收益: {current_unrealized}")

        # 5. 分析差异
        print("\n--- 5. 差异分析 ---")
        print(f"年度增量总和: {total_annual_unrealized}")
        print(f"手动计算增量总和: {manual_total}")
        print(f"年度vs手动差异: {total_annual_unrealized - manual_total}")
        print(f"基准点(2023年底): {unrealized_2023}")
        print(f"如果基准点+年度增量: {unrealized_2023 + total_annual_unrealized}")
        print(f"如果基准点+手动增量: {unrealized_2023 + manual_total}")
        print(f"实际当前值: {current_unrealized}")

        # 6. 检查基准点问题
        print("\n--- 6. 检查不同基准点 ---")

        # 检查第一条交易前的状态
        from app.models.transaction import Transaction
        first_transaction = Transaction.query.filter(
            Transaction.account_id.in_(account_ids)
        ).order_by(Transaction.trade_date.asc()).first()

        if first_transaction:
            first_date = first_transaction.trade_date
            print(f"第一条交易日期: {first_date}")

            # 第一条交易前一天
            before_first = date(first_date.year, 1, 1) if first_date.month > 1 else date(first_date.year - 1, 12, 31)
            portfolio_before = portfolio_service.get_portfolio_summary(
                account_ids, TimePeriod.CUSTOM, before_first, before_first
            )
            unrealized_before = portfolio_before.get('summary', {}).get('total_unrealized_gain', 0)
            print(f"第一条交易前({before_first})未实现收益: {unrealized_before}")

if __name__ == '__main__':
    debug_detailed_unrealized()