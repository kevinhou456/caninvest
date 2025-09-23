#!/usr/bin/env python3

import sys
import os
sys.path.append('/Users/luzhang/Desktop/web1/canadian_family_investment')

from app import create_app
from app.services.portfolio_service import PortfolioService

def test_months_display():
    app = create_app()

    with app.app_context():
        portfolio_service = PortfolioService()
        account_ids = [4]  # 账户4

        print("=== 测试月度分析显示范围 ===")

        # 测试默认参数（应该显示所有月份）
        monthly_analysis = portfolio_service.get_monthly_analysis(account_ids)
        monthly_data = monthly_analysis.get('monthly_data', [])

        print(f"月度数据条目数: {len(monthly_data)}")
        if monthly_data:
            # 显示最早和最晚的月份
            first_month = monthly_data[0]
            last_month = monthly_data[-1]
            print(f"最早月份: {first_month.get('year')}-{first_month.get('month')}")
            print(f"最晚月份: {last_month.get('year')}-{last_month.get('month')}")

            # 显示前5个和后5个月份
            print("\n前5个月份:")
            for i, month_data in enumerate(monthly_data[:5]):
                print(f"  {month_data.get('year')}-{month_data.get('month'):02d}: 总资产={month_data.get('total_assets', 0):.2f}")

            print("\n后5个月份:")
            for i, month_data in enumerate(monthly_data[-5:]):
                print(f"  {month_data.get('year')}-{month_data.get('month'):02d}: 总资产={month_data.get('total_assets', 0):.2f}")

        # 测试指定月数参数
        print(f"\n=== 测试指定12个月 ===")
        monthly_analysis_12 = portfolio_service.get_monthly_analysis(account_ids, months=12)
        monthly_data_12 = monthly_analysis_12.get('monthly_data', [])
        print(f"指定12个月的数据条目数: {len(monthly_data_12)}")

if __name__ == '__main__':
    test_months_display()