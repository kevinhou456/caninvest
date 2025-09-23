#!/usr/bin/env python3

import sys
import os
sys.path.append('/Users/luzhang/Desktop/web1/canadian_family_investment')

from datetime import date
from app import create_app
from app.services.portfolio_service import PortfolioService

def debug_performance_comparison():
    app = create_app()

    with app.app_context():
        portfolio_service = PortfolioService()
        account_ids = [2]  # 账户2

        print("=== 账户2性能比较调试 ===")

        # 测试不同时间范围的性能比较
        ranges = ['1m', '3m', '6m', 'ytd', '1y', '2y', 'all']

        for range_param in ranges:
            print(f"\n--- {range_param.upper()}时间范围 ---")

            try:
                performance_data = portfolio_service.get_performance_comparison(account_ids, range_param)
                summary = performance_data.get('summary', {})

                start_date = summary.get('start_date')
                end_date = summary.get('end_date')
                portfolio_return = summary.get('portfolio_return_percent', 0)
                base_value = summary.get('portfolio_base_value', 0)
                final_value = summary.get('portfolio_final_value', 0)
                total_return = summary.get('portfolio_total_return', 0)

                print(f"  时间范围: {start_date} -> {end_date}")
                print(f"  基础价值: {base_value}")
                print(f"  最终价值: {final_value}")
                print(f"  总收益: {total_return}")
                print(f"  收益率: {portfolio_return:.2f}%")

                # 手动验证计算
                if base_value > 0:
                    manual_return = ((final_value - base_value) / base_value) * 100
                    print(f"  手动计算收益率: {manual_return:.2f}%")
                    print(f"  差异: {portfolio_return - manual_return:.2f}%")

            except Exception as e:
                print(f"  错误: {e}")

        # 特别关注年初至今的计算
        print(f"\n--- 详细YTD分析 ---")
        try:
            ytd_data = portfolio_service.get_performance_comparison(account_ids, 'ytd')
            performance_series = ytd_data.get('performance_series', [])

            print(f"数据点数量: {len(performance_series)}")
            if performance_series:
                first_point = performance_series[0]
                last_point = performance_series[-1]

                print(f"第一个数据点: 日期={first_point.get('date')}, 组合值={first_point.get('portfolio_value')}, 收益率={first_point.get('portfolio_return', 0):.2f}%")
                print(f"最后一个数据点: 日期={last_point.get('date')}, 组合值={last_point.get('portfolio_value')}, 收益率={last_point.get('portfolio_return', 0):.2f}%")

                # 检查收益率计算是否正确
                if len(performance_series) > 1:
                    base_val = performance_series[0].get('portfolio_value', 0)
                    final_val = last_point.get('portfolio_value', 0)
                    if base_val > 0:
                        expected_return = ((final_val - base_val) / base_val) * 100
                        actual_return = last_point.get('portfolio_return', 0)
                        print(f"期望收益率: {expected_return:.2f}%")
                        print(f"实际收益率: {actual_return:.2f}%")
                        print(f"差异: {actual_return - expected_return:.2f}%")

        except Exception as e:
            print(f"YTD分析错误: {e}")

if __name__ == '__main__':
    debug_performance_comparison()