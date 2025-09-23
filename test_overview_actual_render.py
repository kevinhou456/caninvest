#!/usr/bin/env python3

import sys
import os
sys.path.append('/Users/luzhang/Desktop/web1/canadian_family_investment')

from datetime import date
from app import create_app

def test_overview_actual_render():
    app = create_app()

    with app.app_context():
        print("=== 模拟Overview页面实际渲染 ===")

        from app.models.family import Family
        from app.models.account import Account
        from app.services.asset_valuation_service import AssetValuationService
        from app.services.report_service import ReportService

        # 获取家庭和账户（模拟routes.py的逻辑）
        family = Family.query.first()
        accounts = Account.query.filter_by(family_id=family.id).all()
        account_ids = [acc.id for acc in accounts]

        # 初始化服务（模拟routes.py的逻辑）
        asset_service = AssetValuationService()
        report_service = ReportService()

        # 获取综合投资组合指标（模拟routes.py的逻辑）
        comprehensive_metrics = asset_service.get_comprehensive_portfolio_metrics(
            account_ids,
            ownership_map=None
        )

        # 模拟模板中的显示逻辑
        print("=== 模拟HTML模板显示的数据 ===")

        # Total Assets 显示
        total_assets_display = comprehensive_metrics.get('total_assets', {}).get('cad', 0)
        print(f"总资产显示: ${total_assets_display:,.2f}")

        # 详细分解
        total_assets_cad_only = comprehensive_metrics.get('total_assets', {}).get('cad_only', 0)
        total_assets_usd_only = comprehensive_metrics.get('total_assets', {}).get('usd_only', 0)
        print(f"总资产分解: CAD:${total_assets_cad_only:,.2f}|USD:${total_assets_usd_only:,.2f}")

        # Stock Value 显示
        stock_value = comprehensive_metrics.get('total_assets', {}).get('stock_value', 0)
        print(f"股票市值: ${stock_value:,.2f}")

        # Cash Balance 显示
        cash_balance_total = comprehensive_metrics.get('cash_balance', {}).get('total_cad', 0)
        print(f"现金余额: ${cash_balance_total:,.2f}")

        # Total Return 显示
        total_return = comprehensive_metrics.get('total_return', {}).get('cad', 0)
        print(f"总回报: ${total_return:,.2f}")

        # Realized Gain 显示
        realized_gain = comprehensive_metrics.get('realized_gain', {}).get('cad', 0)
        print(f"已实现收益: ${realized_gain:,.2f}")

        # Unrealized Gain 显示
        unrealized_gain = comprehensive_metrics.get('unrealized_gain', {}).get('cad', 0)
        print(f"浮动盈亏: ${unrealized_gain:,.2f}")

        # 验证总资产计算
        calculated_total = stock_value + cash_balance_total
        print(f"\n=== 验证计算 ===")
        print(f"股票市值 + 现金余额 = ${stock_value:,.2f} + ${cash_balance_total:,.2f} = ${calculated_total:,.2f}")
        print(f"系统计算的总资产: ${total_assets_display:,.2f}")
        print(f"计算是否一致: {abs(calculated_total - total_assets_display) < 0.01}")

if __name__ == '__main__':
    test_overview_actual_render()