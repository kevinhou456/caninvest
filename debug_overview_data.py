#!/usr/bin/env python3

import sys
import os
sys.path.append('/Users/luzhang/Desktop/web1/canadian_family_investment')

from datetime import date
from app import create_app

def debug_overview_data():
    app = create_app()

    with app.app_context():
        from app.models.family import Family
        from app.models.account import Account
        from app.services.asset_valuation_service import AssetValuationService

        family = Family.query.first()
        accounts = Account.query.filter_by(family_id=family.id).all()
        account_ids = [acc.id for acc in accounts]

        asset_service = AssetValuationService()

        print("=== Overview页面数据检查 ===")

        # 获取综合指标
        metrics = asset_service.get_comprehensive_portfolio_metrics(account_ids)

        print(f"1. 总资产:")
        print(f"   CAD合计: ${metrics['total_assets']['cad']:,.2f}")
        print(f"   CAD_only: ${metrics['total_assets']['cad_only']:,.2f}")
        print(f"   USD_only: ${metrics['total_assets']['usd_only']:,.2f}")
        print(f"   股票市值CAD: ${metrics['total_assets']['stock_value_cad']:,.2f}")
        print(f"   股票市值USD: ${metrics['total_assets']['stock_value_usd']:,.2f}")
        print(f"   现金CAD: ${metrics['total_assets']['cash_cad']:,.2f}")
        print(f"   现金USD: ${metrics['total_assets']['cash_usd']:,.2f}")

        # 手动验证总资产计算
        exchange_rate = metrics['exchange_rate']
        manual_total = metrics['total_assets']['cad_only'] + metrics['total_assets']['usd_only'] * exchange_rate
        print(f"   手动验证: {metrics['total_assets']['cad_only']} + {metrics['total_assets']['usd_only']} × {exchange_rate} = {manual_total:.2f}")

        print(f"\n2. 总回报:")
        print(f"   CAD合计: ${metrics['total_return']['cad']:,.2f}")
        print(f"   CAD_only: ${metrics['total_return']['cad_only']:,.2f}")
        print(f"   USD_only: ${metrics['total_return']['usd_only']:,.2f}")

        print(f"\n3. 已实现收益:")
        print(f"   CAD合计: ${metrics['realized_gain']['cad']:,.2f}")
        print(f"   CAD_only: ${metrics['realized_gain']['cad_only']:,.2f}")
        print(f"   USD_only: ${metrics['realized_gain']['usd_only']:,.2f}")

        print(f"\n4. 未实现收益:")
        print(f"   CAD合计: ${metrics['unrealized_gain']['cad']:,.2f}")
        print(f"   CAD_only: ${metrics['unrealized_gain']['cad_only']:,.2f}")
        print(f"   USD_only: ${metrics['unrealized_gain']['usd_only']:,.2f}")

        print(f"\n5. 分红:")
        print(f"   CAD合计: ${metrics['dividends']['cad']:,.2f}")
        print(f"   CAD_only: ${metrics['dividends']['cad_only']:,.2f}")
        print(f"   USD_only: ${metrics['dividends']['usd_only']:,.2f}")

        # 检查持仓表格数据
        print(f"\n=== 持仓表格数据验证 ===")
        portfolio_data = asset_service.get_detailed_portfolio_data(account_ids)
        holdings = portfolio_data.get('current_holdings', [])

        total_current_value_cad = 0
        total_unrealized_gain_cad = 0
        total_unrealized_gain_usd = 0

        for holding in holdings:
            total_current_value_cad += holding['current_value_cad']
            if holding['currency'] == 'CAD':
                total_unrealized_gain_cad += holding['unrealized_gain']
            else:
                total_unrealized_gain_usd += holding['unrealized_gain']

        print(f"持仓股票市值总计(CAD等价): ${total_current_value_cad:,.2f}")
        print(f"持仓未实现收益 CAD: ${total_unrealized_gain_cad:,.2f}")
        print(f"持仓未实现收益 USD: ${total_unrealized_gain_usd:,.2f}")

        # 对比汇总和持仓数据
        print(f"\n=== 数据一致性检查 ===")
        stock_value_diff = abs(metrics['total_assets']['stock_value'] - total_current_value_cad)
        unrealized_cad_diff = abs(metrics['unrealized_gain']['cad_only'] - total_unrealized_gain_cad)
        unrealized_usd_diff = abs(metrics['unrealized_gain']['usd_only'] - total_unrealized_gain_usd)

        print(f"股票市值差异: ${stock_value_diff:.2f}")
        print(f"CAD未实现收益差异: ${unrealized_cad_diff:.2f}")
        print(f"USD未实现收益差异: ${unrealized_usd_diff:.2f}")

        if stock_value_diff < 1 and unrealized_cad_diff < 1 and unrealized_usd_diff < 1:
            print("✅ 汇总数据与持仓数据基本一致")
        else:
            print("❌ 汇总数据与持仓数据不一致")

if __name__ == '__main__':
    debug_overview_data()