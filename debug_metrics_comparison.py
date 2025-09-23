#!/usr/bin/env python3

import sys
import os
sys.path.append('/Users/luzhang/Desktop/web1/canadian_family_investment')

from datetime import date
from app import create_app

def debug_metrics_comparison():
    app = create_app()

    with app.app_context():
        from app.models.family import Family
        from app.models.account import Account
        from app.services.asset_valuation_service import AssetValuationService

        family = Family.query.first()
        account_2 = Account.query.filter_by(family_id=family.id, id=2).first()
        print(f'账户2: {account_2.name}')

        asset_service = AssetValuationService()

        print("=== 使用get_comprehensive_portfolio_metrics ===")
        # 使用get_comprehensive_portfolio_metrics计算
        metrics = asset_service.get_comprehensive_portfolio_metrics([2])
        print(f'总资产 CAD: ${metrics["total_assets"]["cad"]}')
        print(f'总资产 CAD_only: ${metrics["total_assets"]["cad_only"]}')
        print(f'总资产 USD_only: ${metrics["total_assets"]["usd_only"]}')
        print(f'股票市值 CAD: ${metrics["total_assets"]["stock_value_cad"]}')
        print(f'股票市值 USD: ${metrics["total_assets"]["stock_value_usd"]}')
        print(f'现金 CAD: ${metrics["total_assets"]["cash_cad"]}')
        print(f'现金 USD: ${metrics["total_assets"]["cash_usd"]}')

        print("\n=== 使用get_asset_snapshot ===")
        # 使用get_asset_snapshot计算
        snapshot = asset_service.get_asset_snapshot(2, date.today())
        print(f'总资产: ${snapshot.total_assets}')
        print(f'股票市值: ${snapshot.stock_market_value}')
        print(f'现金 CAD: ${snapshot.cash_balance_cad}')
        print(f'现金 USD: ${snapshot.cash_balance_usd}')
        print(f'现金总额(CAD等价): ${snapshot.cash_balance_total_cad}')

        print("\n=== 使用_calculate_account_metrics_by_currency ===")
        # 直接使用_calculate_account_metrics_by_currency
        stock_cad, stock_usd, realized_cad, realized_usd, unrealized_cad, unrealized_usd = asset_service._calculate_account_metrics_by_currency(2, date.today())
        cash_cad, cash_usd = asset_service._calculate_cash_balance(2, date.today())

        print(f'股票市值 CAD: ${stock_cad}')
        print(f'股票市值 USD: ${stock_usd}')
        print(f'现金 CAD: ${cash_cad}')
        print(f'现金 USD: ${cash_usd}')
        print(f'已实现收益 CAD: ${realized_cad}')
        print(f'已实现收益 USD: ${realized_usd}')
        print(f'未实现收益 CAD: ${unrealized_cad}')
        print(f'未实现收益 USD: ${unrealized_usd}')

        # 手动计算总资产验证
        exchange_rate = 1.36  # 当前汇率
        total_assets_manual = float(stock_cad) + float(cash_cad) + (float(stock_usd) + float(cash_usd)) * exchange_rate
        print(f'\n=== 手动验证 ===')
        print(f'手动计算总资产: ${total_assets_manual}')
        print(f'系统计算总资产: ${metrics["total_assets"]["cad"]}')
        print(f'差异: ${metrics["total_assets"]["cad"] - total_assets_manual}')

if __name__ == '__main__':
    debug_metrics_comparison()