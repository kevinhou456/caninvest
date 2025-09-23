#!/usr/bin/env python3

import sys
import os
sys.path.append('/Users/luzhang/Desktop/web1/canadian_family_investment')

from datetime import date
from app import create_app

def debug_usd_calculation():
    app = create_app()

    with app.app_context():
        print("=== 调试USD计算错误 ===")

        from app.models.family import Family
        from app.models.account import Account
        from app.services.asset_valuation_service import AssetValuationService

        # 获取家庭和账户
        family = Family.query.first()
        accounts = Account.query.filter_by(family_id=family.id).all()
        account_ids = [acc.id for acc in accounts]

        asset_service = AssetValuationService()

        print(f"所有账户: {[(acc.id, acc.name) for acc in accounts]}")

        # 逐个账户计算并汇总
        total_realized_usd = 0
        total_unrealized_usd = 0
        total_dividends_usd = 0
        total_interest_usd = 0

        print(f"\n=== 逐个账户计算USD数据 ===")
        for account_id in account_ids:
            print(f"\n账户{account_id}:")

            try:
                # 计算各项指标
                _, _, realized_cad, realized_usd, unrealized_cad, unrealized_usd = asset_service._calculate_account_metrics_by_currency(account_id, date.today())
                div_stats = asset_service._calculate_dividend_interest_by_currency(account_id, date.today())

                print(f"  已实现收益 USD: ${realized_usd}")
                print(f"  浮动盈亏 USD: ${unrealized_usd}")
                print(f"  分红 USD: ${div_stats['dividends_usd']}")
                print(f"  利息 USD: ${div_stats['interest_usd']}")

                total_realized_usd += float(realized_usd)
                total_unrealized_usd += float(unrealized_usd)
                total_dividends_usd += float(div_stats['dividends_usd'])
                total_interest_usd += float(div_stats['interest_usd'])

            except Exception as e:
                print(f"  计算出错: {e}")

        manual_total_usd = total_realized_usd + total_unrealized_usd + total_dividends_usd + total_interest_usd
        print(f"\n=== 手动汇总结果 ===")
        print(f"已实现收益 USD: ${total_realized_usd}")
        print(f"浮动盈亏 USD: ${total_unrealized_usd}")
        print(f"分红 USD: ${total_dividends_usd}")
        print(f"利息 USD: ${total_interest_usd}")
        print(f"总回报 USD (手动): ${manual_total_usd}")

        # 对比系统计算结果
        print(f"\n=== 系统计算结果 ===")
        metrics = asset_service.get_comprehensive_portfolio_metrics(account_ids)
        system_total_usd = metrics.get('total_return', {}).get('usd_only', 0)
        system_realized_usd = metrics.get('realized_gain', {}).get('usd_only', 0)
        system_unrealized_usd = metrics.get('unrealized_gain', {}).get('usd_only', 0)
        system_dividends_usd = metrics.get('dividends', {}).get('usd_only', 0)
        system_interest_usd = metrics.get('interest', {}).get('usd_only', 0)

        print(f"已实现收益 USD (系统): ${system_realized_usd}")
        print(f"浮动盈亏 USD (系统): ${system_unrealized_usd}")
        print(f"分红 USD (系统): ${system_dividends_usd}")
        print(f"利息 USD (系统): ${system_interest_usd}")
        print(f"总回报 USD (系统): ${system_total_usd}")

        print(f"\n=== 差异分析 ===")
        print(f"已实现收益差异: {system_realized_usd - total_realized_usd}")
        print(f"浮动盈亏差异: {system_unrealized_usd - total_unrealized_usd}")
        print(f"总回报差异: {system_total_usd - manual_total_usd}")

if __name__ == '__main__':
    debug_usd_calculation()