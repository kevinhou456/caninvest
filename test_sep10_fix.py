#!/usr/bin/env python3

import sys
import os
sys.path.append('/Users/luzhang/Desktop/web1/canadian_family_investment')

from datetime import date
from app import create_app

def test_sep10_fix():
    app = create_app()

    with app.app_context():
        print("=== 测试9月10日修复后的现金和浮动盈亏计算 ===")

        account_id = 3
        sep9 = date(2025, 9, 9)
        sep10 = date(2025, 9, 10)

        from app.services.asset_valuation_service import AssetValuationService
        from app.services.portfolio_service import portfolio_service, TimePeriod

        asset_service = AssetValuationService()

        print(f"--- 9月9日资产快照 ---")
        snapshot_sep9 = asset_service.get_asset_snapshot(account_id, sep9)
        print(f"股票市值: ${snapshot_sep9.stock_market_value}")
        print(f"现金余额: CAD=${snapshot_sep9.cash_balance_cad}, USD=${snapshot_sep9.cash_balance_usd}")
        print(f"总资产: ${snapshot_sep9.total_assets}")

        print(f"\n--- 9月10日资产快照 ---")
        snapshot_sep10 = asset_service.get_asset_snapshot(account_id, sep10)
        print(f"股票市值: ${snapshot_sep10.stock_market_value}")
        print(f"现金余额: CAD=${snapshot_sep10.cash_balance_cad}, USD=${snapshot_sep10.cash_balance_usd}")
        print(f"总资产: ${snapshot_sep10.total_assets}")

        print(f"\n--- 资产变化 ---")
        stock_change = snapshot_sep10.stock_market_value - snapshot_sep9.stock_market_value
        cash_change = snapshot_sep10.cash_balance_total_cad - snapshot_sep9.cash_balance_total_cad
        total_change = snapshot_sep10.total_assets - snapshot_sep9.total_assets
        print(f"股票市值变化: ${stock_change}")
        print(f"现金变化: ${cash_change}")
        print(f"总资产变化: ${total_change}")

        print(f"\n--- Portfolio Service浮动盈亏 ---")
        try:
            portfolio_sep9 = portfolio_service.get_portfolio_summary([account_id], TimePeriod.CUSTOM, sep9, sep9)
            portfolio_sep10 = portfolio_service.get_portfolio_summary([account_id], TimePeriod.CUSTOM, sep10, sep10)

            unrealized_sep9 = portfolio_sep9.get('summary', {}).get('total_unrealized_gain', 0)
            unrealized_sep10 = portfolio_sep10.get('summary', {}).get('total_unrealized_gain', 0)

            print(f"9月9日浮动盈亏: ${unrealized_sep9}")
            print(f"9月10日浮动盈亏: ${unrealized_sep10}")
            print(f"日浮动盈亏变化: ${unrealized_sep10 - unrealized_sep9}")

        except Exception as e:
            print(f"Portfolio Service计算出错: {e}")
            import traceback
            traceback.print_exc()

if __name__ == '__main__':
    test_sep10_fix()