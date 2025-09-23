#!/usr/bin/env python3

import sys
import os
sys.path.append('/Users/luzhang/Desktop/web1/canadian_family_investment')

from datetime import date
from app import create_app

def test_overview_debug():
    app = create_app()

    with app.app_context():
        print("=== 调试Overview页面数据 ===")

        account_id = 3
        target_date = date(2025, 9, 10)

        from app.services.asset_valuation_service import AssetValuationService
        from app.services.portfolio_service import portfolio_service, TimePeriod

        asset_service = AssetValuationService()

        print(f"--- AssetValuationService.get_asset_snapshot ---")
        snapshot = asset_service.get_asset_snapshot(account_id, target_date)
        print(f"总资产: ${snapshot.total_assets}")
        print(f"股票市值: ${snapshot.stock_market_value}")
        print(f"现金余额: CAD=${snapshot.cash_balance_cad}, USD=${snapshot.cash_balance_usd}")
        print(f"现金总额(CAD): ${snapshot.cash_balance_total_cad}")

        print(f"\n--- Portfolio Service.get_portfolio_summary ---")
        portfolio_data = portfolio_service.get_portfolio_summary([account_id], TimePeriod.CUSTOM, target_date, target_date)
        summary = portfolio_data.get('summary', {})
        print(f"Portfolio Service总市值: ${summary.get('total_current_value', 0)}")
        print(f"Portfolio Service浮动盈亏: ${summary.get('total_unrealized_gain', 0)}")

        print(f"\n--- AssetValuationService.get_comprehensive_portfolio_metrics ---")
        metrics = asset_service.get_comprehensive_portfolio_metrics([account_id], target_date)
        print(f"总资产: ${metrics.get('total_assets', {}).get('cad', 0)}")
        print(f"浮动盈亏: ${metrics.get('unrealized_gain', {}).get('cad', 0)}")
        print(f"股票市值: ${metrics.get('stock_value', {}).get('cad', 0)}")
        print(f"现金: ${metrics.get('cash_balance', {}).get('total_cad', 0)}")

if __name__ == '__main__':
    test_overview_debug()