#!/usr/bin/env python3

import sys
import os
sys.path.append('/Users/luzhang/Desktop/web1/canadian_family_investment')

from datetime import date
from app import create_app

def debug_overview_route():
    app = create_app()

    with app.app_context():
        print("=== 调试Overview路由完整逻辑 ===")

        # 完全模拟routes.py中overview()函数的逻辑
        from flask import request
        from decimal import Decimal
        from app.models.family import Family
        from app.models.account import Account
        from app.services.asset_valuation_service import AssetValuationService
        from app.services.report_service import ReportService
        from app.services.currency_service import currency_service

        # 获取默认家庭
        family = Family.query.first()
        if not family:
            print("没有找到家庭")
            return

        # 获取过滤参数（模拟没有参数的情况）
        member_id = None
        account_id = None
        time_period = 'all_time'

        try:
            # 初始化统一服务
            asset_service = AssetValuationService()
            report_service = ReportService()

            # 根据过滤条件获取账户
            ownership_map = None
            accounts = Account.query.filter_by(family_id=family.id).all()
            filter_description = "All Members"

            print(f"找到的账户: {[acc.name for acc in accounts]}")

            # 获取汇率信息
            exchange_rates = currency_service.get_cad_usd_rates()
            print(f"汇率: {exchange_rates}")

            # 使用新的统一资产估值服务架构
            account_ids = [acc.id for acc in accounts]

            # 获取综合投资组合指标 - 包含完整的财务计算
            print("\n=== 调用 get_comprehensive_portfolio_metrics ===")
            comprehensive_metrics = asset_service.get_comprehensive_portfolio_metrics(
                account_ids,
                ownership_map=ownership_map
            )

            print("=== comprehensive_metrics 返回的数据 ===")
            print(f"total_assets: {comprehensive_metrics.get('total_assets', {})}")
            print(f"stock_value: {comprehensive_metrics.get('stock_value', {})}")
            print(f"cash_balance: {comprehensive_metrics.get('cash_balance', {})}")

            # 检查是否有数据混乱
            total_assets_cad = comprehensive_metrics.get('total_assets', {}).get('cad', 0)
            stock_value_cad = comprehensive_metrics.get('stock_value', {}).get('cad', 0)
            cash_balance_total = comprehensive_metrics.get('cash_balance', {}).get('total_cad', 0)

            print(f"\n=== 关键数据检查 ===")
            print(f"总资产(cad): ${total_assets_cad}")
            print(f"股票市值(cad): ${stock_value_cad}")
            print(f"现金余额(total_cad): ${cash_balance_total}")

            # 检查是否有字段名称错误
            print(f"\n=== 检查可能的字段错误 ===")
            for key in comprehensive_metrics.get('total_assets', {}):
                value = comprehensive_metrics['total_assets'][key]
                print(f"total_assets.{key}: ${value}")
                if abs(value - 40677.88) < 1:
                    print(f"  ^^^ 这个字段值接近40677.88！")

        except Exception as e:
            print(f"调试过程中发生错误: {e}")
            import traceback
            traceback.print_exc()

if __name__ == '__main__':
    debug_overview_route()