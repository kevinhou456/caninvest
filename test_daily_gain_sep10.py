#!/usr/bin/env python3

import sys
import os
sys.path.append('/Users/luzhang/Desktop/web1/canadian_family_investment')

from datetime import date
from app import create_app

def test_daily_gain_sep10():
    app = create_app()

    with app.app_context():
        print("=== 查找9月10日显示的浮动盈亏713.55 ===")

        account_id = 3
        sep9 = date(2025, 9, 9)
        sep10 = date(2025, 9, 10)

        from app.services.asset_valuation_service import AssetValuationService
        from app.services.daily_stats_service import daily_stats_service
        from app.services.portfolio_service import portfolio_service, TimePeriod

        asset_service = AssetValuationService()

        print(f"1. AssetValuationService资产快照:")
        snapshot_sep9 = asset_service.get_asset_snapshot(account_id, sep9)
        snapshot_sep10 = asset_service.get_asset_snapshot(account_id, sep10)
        daily_change = snapshot_sep10.total_assets - snapshot_sep9.total_assets
        print(f"   9月9日总资产: ${snapshot_sep9.total_assets}")
        print(f"   9月10日总资产: ${snapshot_sep10.total_assets}")
        print(f"   每日变化: ${daily_change}")

        print(f"\n2. Portfolio Service浮动盈亏:")
        try:
            portfolio_sep10 = portfolio_service.get_portfolio_summary([account_id], TimePeriod.CUSTOM, sep10, sep10)
            unrealized_gain = portfolio_sep10.get('summary', {}).get('total_unrealized_gain', 0)
            print(f"   9月10日浮动盈亏: ${unrealized_gain}")
        except Exception as e:
            print(f"   Portfolio Service错误: {e}")

        print(f"\n3. Daily Stats Service:")
        try:
            daily_report = daily_stats_service.get_combined_daily_report([account_id], sep10)
            floating_pnl = daily_report.get('floating_pnl', {})
            print(f"   总资产: ${floating_pnl.get('total_assets', 0)}")
            print(f"   浮动盈亏: ${floating_pnl.get('unrealized_gain', 0)}")
            print(f"   日变化: ${floating_pnl.get('daily_change', 0)}")
        except Exception as e:
            print(f"   Daily Stats错误: {e}")

        print(f"\n4. AssetValuationService综合指标:")
        try:
            metrics = asset_service.get_comprehensive_portfolio_metrics([account_id], sep10)
            print(f"   总资产: ${metrics.get('total_assets', {}).get('cad', 0)}")
            print(f"   浮动盈亏: ${metrics.get('unrealized_gain', {}).get('cad', 0)}")
        except Exception as e:
            print(f"   综合指标错误: {e}")

if __name__ == '__main__':
    test_daily_gain_sep10()