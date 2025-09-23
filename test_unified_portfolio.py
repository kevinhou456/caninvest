#!/usr/bin/env python3

import sys
import os
sys.path.append('/Users/luzhang/Desktop/web1/canadian_family_investment')

from datetime import date
from app import create_app

def test_unified_portfolio():
    app = create_app()

    with app.app_context():
        from app.models.family import Family
        from app.models.account import Account
        from app.services.asset_valuation_service import AssetValuationService

        family = Family.query.first()
        asset_service = AssetValuationService()

        print("=== 测试统一计算逻辑 ===")

        # 1. 测试账户2的汇总数据
        metrics = asset_service.get_comprehensive_portfolio_metrics([2])
        print(f"汇总数据 - 账户2 USD未实现收益: ${metrics['unrealized_gain']['usd_only']}")

        # 2. 测试账户2的持仓表格数据
        portfolio_data = asset_service.get_detailed_portfolio_data([2])
        holdings = portfolio_data.get('current_holdings', [])

        print(f"\n持仓表格数据 - 账户2:")
        total_unrealized_usd = 0
        for holding in holdings:
            if holding['currency'] == 'USD':
                print(f"  {holding['symbol']}: 未实现收益 ${holding['unrealized_gain']}")
                total_unrealized_usd += holding['unrealized_gain']

        print(f"持仓表格 USD未实现收益总计: ${total_unrealized_usd}")

        # 3. 验证HSUV-U.TO的具体数据
        hsuv_holding = next((h for h in holdings if h['symbol'] == 'HSUV-U.TO'), None)
        if hsuv_holding:
            print(f"\nHSUV-U.TO详细数据:")
            print(f"  持股: {hsuv_holding['current_shares']}股")
            print(f"  当前价格: ${hsuv_holding['current_price']}")
            print(f"  市值: ${hsuv_holding['current_value']}")
            print(f"  成本: ${hsuv_holding['total_cost']}")
            print(f"  平均成本: ${hsuv_holding['average_cost']}")
            print(f"  未实现收益: ${hsuv_holding['unrealized_gain']}")

        # 4. 数据一致性检查
        print(f"\n=== 数据一致性检查 ===")
        print(f"汇总数据: ${metrics['unrealized_gain']['usd_only']}")
        print(f"表格数据: ${total_unrealized_usd}")
        diff = abs(metrics['unrealized_gain']['usd_only'] - total_unrealized_usd)
        if diff < 0.01:
            print("✅ 数据一致!")
        else:
            print(f"❌ 数据不一致，差异: ${diff}")

if __name__ == '__main__':
    test_unified_portfolio()