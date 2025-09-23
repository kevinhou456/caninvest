#!/usr/bin/env python3

import sys
import os
sys.path.append('/Users/luzhang/Desktop/web1/canadian_family_investment')

from datetime import date
from app import create_app

def test_overview_display():
    app = create_app()

    with app.app_context():
        from app.models.family import Family
        from app.models.account import Account
        from app.services.asset_valuation_service import AssetValuationService
        from app.services.currency_service import CurrencyService

        family = Family.query.first()
        accounts = Account.query.filter_by(family_id=family.id).all()
        account_ids = [acc.id for acc in accounts]

        asset_service = AssetValuationService()
        currency_service = CurrencyService()

        print(f"所有账户: {[(acc.id, acc.name) for acc in accounts]}")

        # 模拟overview页面的数据获取
        comprehensive_metrics = asset_service.get_comprehensive_portfolio_metrics(account_ids)

        print("\n=== Overview页面应该显示的数据 ===")
        print(f"总资产: ${comprehensive_metrics['total_assets']['cad']:,.2f}")
        print(f"  详细: CAD ${comprehensive_metrics['total_assets']['cad_only']:,.2f} | USD ${comprehensive_metrics['total_assets']['usd_only']:,.2f}")

        print(f"总回报: ${comprehensive_metrics['total_return']['cad']:,.2f}")
        print(f"  详细: CAD ${comprehensive_metrics['total_return']['cad_only']:,.2f} | USD ${comprehensive_metrics['total_return']['usd_only']:,.2f}")

        print(f"已实现收益: ${comprehensive_metrics['realized_gain']['cad']:,.2f}")
        print(f"  详细: CAD ${comprehensive_metrics['realized_gain']['cad_only']:,.2f} | USD ${comprehensive_metrics['realized_gain']['usd_only']:,.2f}")

        print(f"未实现收益: ${comprehensive_metrics['unrealized_gain']['cad']:,.2f}")
        print(f"  详细: CAD ${comprehensive_metrics['unrealized_gain']['cad_only']:,.2f} | USD ${comprehensive_metrics['unrealized_gain']['usd_only']:,.2f}")

        print(f"分红: ${comprehensive_metrics['dividends']['cad']:,.2f}")
        print(f"  详细: CAD ${comprehensive_metrics['dividends']['cad_only']:,.2f} | USD ${comprehensive_metrics['dividends']['usd_only']:,.2f}")

        print(f"利息: ${comprehensive_metrics['interest']['cad']:,.2f}")
        print(f"  详细: CAD ${comprehensive_metrics['interest']['cad_only']:,.2f} | USD ${comprehensive_metrics['interest']['usd_only']:,.2f}")

        # 检查是否有明显的异常数据
        print("\n=== 数据合理性检查 ===")
        total_assets = comprehensive_metrics['total_assets']['cad']
        total_return = comprehensive_metrics['total_return']['cad']
        realized_gain = comprehensive_metrics['realized_gain']['cad']
        unrealized_gain = comprehensive_metrics['unrealized_gain']['cad']

        if total_assets <= 0:
            print("❌ 总资产为0或负数，明显异常")
        elif total_assets < 1000:
            print("⚠️ 总资产过小，可能有问题")
        else:
            print(f"✅ 总资产正常: ${total_assets:,.2f}")

        if abs(total_return) > total_assets:
            print("⚠️ 总回报绝对值超过总资产，可能有问题")
        else:
            print(f"✅ 总回报合理: ${total_return:,.2f}")

        # 检查计算是否一致
        calculated_return = realized_gain + unrealized_gain + comprehensive_metrics['dividends']['cad'] + comprehensive_metrics['interest']['cad']
        if abs(calculated_return - total_return) > 0.01:
            print(f"❌ 总回报计算不一致: 计算值${calculated_return:,.2f} vs 系统值${total_return:,.2f}")
        else:
            print(f"✅ 总回报计算一致")

if __name__ == '__main__':
    test_overview_display()