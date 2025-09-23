#!/usr/bin/env python3

import sys
import os
sys.path.append('/Users/luzhang/Desktop/web1/canadian_family_investment')

from datetime import date, timedelta
from app import create_app

def debug_base_value():
    app = create_app()

    with app.app_context():
        print("=== 调试Performance Comparison Base Value计算 ===")

        account_id = 7

        from app.services.portfolio_service import portfolio_service
        from app.services.asset_valuation_service import AssetValuationService

        # 模拟Performance Comparison的计算过程
        today = date.today()
        start_date = today - timedelta(days=30)  # 1个月前

        print(f"期间: {start_date} 到 {today}")

        # 生成日期范围
        current_date = start_date
        date_range = []
        while current_date <= today:
            date_range.append(current_date)
            current_date += timedelta(days=1)

        print(f"总共{len(date_range)}天")

        asset_service = AssetValuationService()
        portfolio_values = []

        # 计算每天的portfolio value
        for i, current_date in enumerate(date_range):
            # 1. 获取股票价值
            try:
                from app.services.portfolio_service import TimePeriod
                portfolio_data = portfolio_service.get_portfolio_summary(
                    [account_id], TimePeriod.CUSTOM, current_date, current_date
                )
                stock_value = float(portfolio_data.get('summary', {}).get('total_current_value', 0))
            except Exception as e:
                print(f"第{i}天({current_date})股票价值计算失败: {e}")
                stock_value = 0

            # 2. 获取现金余额
            try:
                cash_balance = asset_service.get_cash_balance(account_id, current_date)
                cash_total = float(cash_balance['total_cad'])
            except Exception as e:
                print(f"第{i}天({current_date})现金余额计算失败: {e}")
                cash_total = 0

            # 3. 总资产
            total_value = stock_value + cash_total
            portfolio_values.append((current_date, total_value))

            # 打印前5天和后5天的详细信息
            if i < 5 or i >= len(date_range) - 5:
                print(f"  第{i}天({current_date}): 股票${stock_value:.2f} + 现金${cash_total:.2f} = 总计${total_value:.2f}")

        print(f"\n--- Base Value计算分析 ---")
        print(f"Portfolio Values总数: {len(portfolio_values)}")

        # 显示前10天的值
        print("前10天的portfolio values:")
        for i, (date_val, value) in enumerate(portfolio_values[:10]):
            print(f"  {i}: {date_val} = ${value:.2f}")

        # 模拟原来的错误逻辑
        old_base_value = next((value for _, value in portfolio_values if value > 0), 0)
        print(f"\n原来的错误逻辑 - 第一个大于0的值: ${old_base_value:.2f}")

        # 正确的逻辑
        new_base_value = portfolio_values[0][1] if portfolio_values else 0
        print(f"修复后的逻辑 - 第一天的值: ${new_base_value:.2f}")

        # 最终值
        final_value = portfolio_values[-1][1] if portfolio_values else 0
        print(f"最终值: ${final_value:.2f}")

        # 计算差异
        old_total_return = final_value - old_base_value
        new_total_return = final_value - new_base_value

        print(f"\n--- Total Return对比 ---")
        print(f"错误计算: ${final_value:.2f} - ${old_base_value:.2f} = ${old_total_return:.2f}")
        print(f"正确计算: ${final_value:.2f} - ${new_base_value:.2f} = ${new_total_return:.2f}")

        # 计算return percentage
        old_return_pct = ((final_value / old_base_value) - 1) * 100 if old_base_value > 0 else 0
        new_return_pct = ((final_value / new_base_value) - 1) * 100 if new_base_value > 0 else 0

        print(f"\n--- Return Percentage对比 ---")
        print(f"错误计算: {old_return_pct:.2f}%")
        print(f"正确计算: {new_return_pct:.2f}%")

if __name__ == '__main__':
    debug_base_value()