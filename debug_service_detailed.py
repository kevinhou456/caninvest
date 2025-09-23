#!/usr/bin/env python3

import sys
import os
sys.path.append('/Users/luzhang/Desktop/web1/canadian_family_investment')

from datetime import date
from app import create_app
from app.services.asset_valuation_service import AssetValuationService
from decimal import Decimal

def debug_service_detailed():
    app = create_app()

    with app.app_context():
        print("=== Service方法详细调试 ===")

        asset_service = AssetValuationService()
        account_id = 2
        target_date = date(2025, 8, 24)

        # 直接调用每个方法，看看在哪一步出现了问题

        # 1. 测试_forward_calculate_with_supplement方法
        print(f"--- 测试_forward_calculate_with_supplement ---")
        supplement_cad = Decimal('0')
        supplement_usd = Decimal('1004.19')

        print(f"输入补偿: CAD=${supplement_cad}, USD=${supplement_usd}")

        try:
            result_cad, result_usd = asset_service._forward_calculate_with_supplement(
                account_id, target_date, supplement_cad, supplement_usd
            )
            print(f"输出结果: CAD=${result_cad}, USD=${result_usd}")
        except Exception as e:
            print(f"方法调用出错: {e}")
            import traceback
            traceback.print_exc()

        # 2. 测试完整的_calculate_cash_balance_reverse方法
        print(f"\n--- 测试_calculate_cash_balance_reverse ---")
        try:
            final_cad, final_usd = asset_service._calculate_cash_balance_reverse(account_id, target_date)
            print(f"最终结果: CAD=${final_cad}, USD=${final_usd}")
        except Exception as e:
            print(f"方法调用出错: {e}")
            import traceback
            traceback.print_exc()

        # 3. 测试完整的_calculate_cash_balance方法
        print(f"\n--- 测试_calculate_cash_balance ---")
        try:
            balance_cad, balance_usd = asset_service._calculate_cash_balance(account_id, target_date)
            print(f"最终余额: CAD=${balance_cad}, USD=${balance_usd}")
        except Exception as e:
            print(f"方法调用出错: {e}")
            import traceback
            traceback.print_exc()

        # 4. 测试历史现金余额计算
        print(f"\n--- 测试_calculate_historical_cash_balance ---")
        try:
            hist_cad, hist_usd = asset_service._calculate_historical_cash_balance(account_id, target_date)
            print(f"历史余额: CAD=${hist_cad}, USD=${hist_usd}")
        except Exception as e:
            print(f"方法调用出错: {e}")
            import traceback
            traceback.print_exc()

if __name__ == '__main__':
    debug_service_detailed()