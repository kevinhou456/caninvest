#!/usr/bin/env python3

import sys
import os
sys.path.append('/Users/luzhang/Desktop/web1/canadian_family_investment')

from datetime import date
from app import create_app

def debug_performance_comparison():
    app = create_app()

    with app.app_context():
        print("=== 调试Performance Comparison中账户7的浮动盈亏 ===")

        account_id = 7
        today = date.today()

        from app.services.portfolio_service import portfolio_service

        print(f"\n--- Performance Comparison 数据 ---")
        try:
            # 检查Performance Comparison的计算
            performance_data = portfolio_service.get_performance_comparison([account_id])

            print(f"Performance Comparison返回的数据结构:")
            for key, value in performance_data.items():
                if isinstance(value, dict):
                    print(f"  {key}: (字典，包含{len(value)}个键)")
                    for subkey, subvalue in value.items():
                        if isinstance(subvalue, (int, float)) and abs(subvalue) > 1000000:
                            print(f"    {subkey}: {subvalue} *** 可能有问题的大数值 ***")
                        else:
                            print(f"    {subkey}: {subvalue}")
                else:
                    print(f"  {key}: {value}")

        except Exception as e:
            print(f"Performance Comparison计算出错: {e}")
            import traceback
            traceback.print_exc()

        print(f"\n--- Daily Stats 数据 ---")
        try:
            from app.services.daily_stats_service import DailyStatsService
            daily_stats_service = DailyStatsService()

            daily_snapshot = daily_stats_service.get_daily_snapshot([account_id], today)
            print(f"Daily Stats返回的数据:")
            for key, value in daily_snapshot.items():
                if isinstance(value, (int, float)) and abs(value) > 1000000:
                    print(f"  {key}: {value} *** 可能有问题的大数值 ***")
                else:
                    print(f"  {key}: {value}")

        except Exception as e:
            print(f"Daily Stats计算出错: {e}")
            import traceback
            traceback.print_exc()

        print(f"\n--- Holdings Board 数据 ---")
        try:
            from app.services.portfolio_service import TimePeriod
            holdings_data = portfolio_service.get_portfolio_summary([account_id], TimePeriod.CUSTOM, today, today)

            summary = holdings_data.get('summary', {})
            print(f"Holdings Board Summary:")
            for key, value in summary.items():
                if isinstance(value, (int, float)) and abs(value) > 1000000:
                    print(f"  {key}: {value} *** 可能有问题的大数值 ***")
                else:
                    print(f"  {key}: {value}")

        except Exception as e:
            print(f"Holdings Board计算出错: {e}")
            import traceback
            traceback.print_exc()

        print(f"\n--- 搜索所有可能的浮动盈亏计算 ---")
        # 检查是否有其他地方计算了错误的浮动盈亏
        try:
            from app.services.asset_valuation_service import AssetValuationService
            asset_service = AssetValuationService()

            # 测试_get_account_stats_by_currency方法
            account_stats = asset_service._get_account_stats_by_currency(account_id, today)
            print(f"Asset Service Account Stats:")
            for key, value in account_stats.items():
                if isinstance(value, (int, float)) and abs(float(value)) > 1000000:
                    print(f"  {key}: {value} *** 可能有问题的大数值 ***")
                else:
                    print(f"  {key}: {value}")

        except Exception as e:
            print(f"Asset Service Account Stats计算出错: {e}")
            import traceback
            traceback.print_exc()

if __name__ == '__main__':
    debug_performance_comparison()