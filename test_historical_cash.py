#!/usr/bin/env python3

import sys
import os
sys.path.append('/Users/luzhang/Desktop/web1/canadian_family_investment')

from datetime import date, timedelta
from app import create_app

def test_historical_cash():
    app = create_app()

    with app.app_context():
        print("=== 测试历史现金计算（允许负余额）===")

        account_id = 7
        today = date.today()

        from app.services.asset_valuation_service import AssetValuationService
        asset_service = AssetValuationService()

        # 测试最近几天的现金计算
        for i in range(5):
            test_date = today - timedelta(days=i)
            try:
                cash_balance = asset_service.get_cash_balance(account_id, test_date)
                cad_balance = cash_balance['cad']
                usd_balance = cash_balance['usd']
                total_cad = cash_balance['total_cad']

                print(f"{test_date}: CAD=${cad_balance}, USD=${usd_balance}, 总计(CAD等值)=${total_cad}")

                # 检查是否有负余额
                if cad_balance < 0 or usd_balance < 0:
                    print(f"  *** 负余额检测：CAD={cad_balance}, USD={usd_balance} ***")

            except Exception as e:
                print(f"{test_date}: 计算出错: {e}")

        # 测试更早的历史日期
        print(f"\n--- 测试历史日期 ---")
        historical_dates = [
            today - timedelta(days=30),
            today - timedelta(days=60),
            today - timedelta(days=90)
        ]

        for test_date in historical_dates:
            try:
                cash_balance = asset_service.get_cash_balance(account_id, test_date)
                cad_balance = cash_balance['cad']
                usd_balance = cash_balance['usd']
                total_cad = cash_balance['total_cad']

                print(f"{test_date}: CAD=${cad_balance}, USD=${usd_balance}, 总计(CAD等值)=${total_cad}")

                # 检查是否有负余额
                if cad_balance < 0 or usd_balance < 0:
                    print(f"  *** 负余额：CAD={cad_balance}, USD={usd_balance} - 这是正常的！***")

            except Exception as e:
                print(f"{test_date}: 计算出错: {e}")

if __name__ == '__main__':
    test_historical_cash()