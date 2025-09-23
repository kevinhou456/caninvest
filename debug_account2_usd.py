#!/usr/bin/env python3

import sys
import os
sys.path.append('/Users/luzhang/Desktop/web1/canadian_family_investment')

from datetime import date
from app import create_app

def debug_account2_usd():
    app = create_app()

    with app.app_context():
        from app.models.family import Family
        from app.models.account import Account
        from app.services.asset_valuation_service import AssetValuationService

        family = Family.query.first()
        asset_service = AssetValuationService()

        print("=== 账户2的USD数据详细验算 ===")

        # 1. 单独计算账户2
        stock_cad, stock_usd, realized_cad, realized_usd, unrealized_cad, unrealized_usd = asset_service._calculate_account_metrics_by_currency(2, date.today())
        div_stats = asset_service._calculate_dividend_interest_by_currency(2, date.today())

        print(f"1. 直接计算账户2:")
        print(f"   股票市值 USD: ${stock_usd}")
        print(f"   已实现收益 USD: ${realized_usd}")
        print(f"   未实现收益 USD: ${unrealized_usd}")
        print(f"   分红 USD: ${div_stats['dividends_usd']}")
        print(f"   利息 USD: ${div_stats['interest_usd']}")

        # 手动计算总回报
        manual_total_return_usd = float(realized_usd) + float(unrealized_usd) + float(div_stats['dividends_usd']) + float(div_stats['interest_usd'])
        print(f"   手动计算总回报 USD: ${manual_total_return_usd}")

        # 2. 通过get_comprehensive_portfolio_metrics计算
        metrics = asset_service.get_comprehensive_portfolio_metrics([2])
        print(f"\n2. get_comprehensive_portfolio_metrics结果:")
        print(f"   总回报 USD: ${metrics['total_return']['usd_only']}")
        print(f"   已实现收益 USD: ${metrics['realized_gain']['usd_only']}")
        print(f"   未实现收益 USD: ${metrics['unrealized_gain']['usd_only']}")
        print(f"   分红 USD: ${metrics['dividends']['usd_only']}")
        print(f"   利息 USD: ${metrics['interest']['usd_only']}")

        # 3. 验算总回报组成
        system_total_usd = metrics['realized_gain']['usd_only'] + metrics['unrealized_gain']['usd_only'] + metrics['dividends']['usd_only'] + metrics['interest']['usd_only']
        print(f"\n3. 系统数据验算:")
        print(f"   ${metrics['realized_gain']['usd_only']} + ${metrics['unrealized_gain']['usd_only']} + ${metrics['dividends']['usd_only']} + ${metrics['interest']['usd_only']} = ${system_total_usd}")
        print(f"   系统显示总回报: ${metrics['total_return']['usd_only']}")
        print(f"   差异: ${metrics['total_return']['usd_only'] - system_total_usd}")

        # 4. 检查逻辑错误
        print(f"\n4. 逻辑检查:")
        if metrics['total_return']['usd_only'] > metrics['unrealized_gain']['usd_only']:
            print(f"   ✓ 总回报(${metrics['total_return']['usd_only']}) > 未实现收益(${metrics['unrealized_gain']['usd_only']}) 是正常的")
        else:
            print(f"   ✗ 总回报(${metrics['total_return']['usd_only']}) ≤ 未实现收益(${metrics['unrealized_gain']['usd_only']}) 有问题")

        print(f"   已实现收益为正: ${metrics['realized_gain']['usd_only']} > 0")
        print(f"   这应该能够抵消部分未实现亏损")

if __name__ == '__main__':
    debug_account2_usd()