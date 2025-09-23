#!/usr/bin/env python3

import sys
import os
sys.path.append('/Users/luzhang/Desktop/web1/canadian_family_investment')

from datetime import date
from app import create_app

def test_daily_stats_calendar():
    app = create_app()

    with app.app_context():
        print("=== 测试Daily Stats日历数据 ===")

        account_id = 3
        year = 2025
        month = 9

        from app.services.daily_stats_service import daily_stats_service

        try:
            # 获取月度日历数据 - 这是daily-stats页面实际使用的
            calendar_data = daily_stats_service.get_monthly_calendar_data([account_id], year, month)

            print(f"账户{account_id}的{year}年{month}月日历数据:")

            # 查找9月10日的数据
            sep10_key = "2025-09-10"
            if sep10_key in calendar_data.daily_stats:
                stats_point = calendar_data.daily_stats[sep10_key]
                print(f"\n9月10日数据:")
                print(f"  总资产: ${stats_point.total_assets}")
                print(f"  股票市值: ${stats_point.stock_market_value}")
                print(f"  现金余额: ${stats_point.cash_balance}")
                print(f"  浮动盈亏: ${stats_point.unrealized_gain}")
                print(f"  日变化: ${stats_point.daily_change}")
                print(f"  日收益率: {stats_point.daily_return_pct}%")
                print(f"  是否交易日: {stats_point.is_trading_day}")
                print(f"  是否有交易: {stats_point.has_transactions}")
            else:
                print(f"没有找到{sep10_key}的数据")
                print(f"可用日期: {list(calendar_data.daily_stats.keys())}")

            # 同时检查9月9日作为对比
            sep9_key = "2025-09-09"
            if sep9_key in calendar_data.daily_stats:
                stats_point = calendar_data.daily_stats[sep9_key]
                print(f"\n9月9日数据（对比）:")
                print(f"  总资产: ${stats_point.total_assets}")
                print(f"  浮动盈亏: ${stats_point.unrealized_gain}")
                print(f"  日变化: ${stats_point.daily_change}")

        except Exception as e:
            print(f"获取日历数据出错: {e}")
            import traceback
            traceback.print_exc()

if __name__ == '__main__':
    test_daily_stats_calendar()