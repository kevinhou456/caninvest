#!/usr/bin/env python3

import sys
import os
sys.path.append('/Users/luzhang/Desktop/web1/canadian_family_investment')

from datetime import date, timedelta
from app import create_app

def test_daily_change():
    app = create_app()

    with app.app_context():
        print("=== 测试账户3的每日浮动盈亏（简单逻辑）===")

        account_id = 3
        sep9 = date(2025, 9, 9)
        sep10 = date(2025, 9, 10)

        from app.services.asset_valuation_service import AssetValuationService

        asset_service = AssetValuationService()

        # 9月9日总资产
        snapshot_sep9 = asset_service.get_asset_snapshot(account_id, sep9)
        total_assets_sep9 = snapshot_sep9.total_assets
        print(f"9月9日总资产: ${total_assets_sep9}")

        # 9月10日总资产
        snapshot_sep10 = asset_service.get_asset_snapshot(account_id, sep10)
        total_assets_sep10 = snapshot_sep10.total_assets
        print(f"9月10日总资产: ${total_assets_sep10}")

        # 每日浮动盈亏
        daily_change = total_assets_sep10 - total_assets_sep9
        print(f"\n每日浮动盈亏: ${daily_change}")

        print(f"\n详细分解:")
        print(f"9月9日 - 股票: ${snapshot_sep9.stock_market_value}, 现金: ${snapshot_sep9.cash_balance_total_cad}")
        print(f"9月10日 - 股票: ${snapshot_sep10.stock_market_value}, 现金: ${snapshot_sep10.cash_balance_total_cad}")

if __name__ == '__main__':
    test_daily_change()