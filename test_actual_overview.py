#!/usr/bin/env python3

import sys
import os
sys.path.append('/Users/luzhang/Desktop/web1/canadian_family_investment')

from datetime import date
from app import create_app

def test_actual_overview():
    app = create_app()

    with app.app_context():
        print("=== 测试实际Overview页面数据 ===")

        # 模拟overview页面的完整逻辑
        from app.models.family import Family
        from app.models.account import Account
        from app.services.asset_valuation_service import AssetValuationService

        # 获取家庭和账户
        family = Family.query.first()
        if not family:
            print("没有找到家庭")
            return

        accounts = Account.query.filter_by(family_id=family.id).all()
        account_ids = [acc.id for acc in accounts]

        print(f"找到账户: {[acc.name for acc in accounts]}")

        # 初始化服务
        asset_service = AssetValuationService()

        # 获取综合投资组合指标 - 这是overview页面实际使用的
        comprehensive_metrics = asset_service.get_comprehensive_portfolio_metrics(account_ids)

        print(f"\n=== Overview页面实际数据 ===")
        print(f"总资产: ${comprehensive_metrics.get('total_assets', {}).get('cad', 0)}")
        print(f"股票市值: ${comprehensive_metrics.get('total_assets', {}).get('stock_value', 0)}")
        print(f"现金余额: ${comprehensive_metrics.get('cash_balance', {}).get('total_cad', 0)}")
        print(f"总回报: ${comprehensive_metrics.get('total_return', {}).get('cad', 0)}")
        print(f"已实现收益: ${comprehensive_metrics.get('realized_gain', {}).get('cad', 0)}")
        print(f"浮动盈亏: ${comprehensive_metrics.get('unrealized_gain', {}).get('cad', 0)}")
        print(f"分红: ${comprehensive_metrics.get('dividends', {}).get('cad', 0)}")
        print(f"利息: ${comprehensive_metrics.get('interest', {}).get('cad', 0)}")

        # 对比账户3的单独数据
        print(f"\n=== 账户3单独数据（对比用）===")
        account_3_snapshot = asset_service.get_asset_snapshot(3)
        print(f"账户3总资产: ${account_3_snapshot.total_assets}")
        print(f"账户3股票市值: ${account_3_snapshot.stock_market_value}")
        print(f"账户3现金: ${account_3_snapshot.cash_balance_total_cad}")

if __name__ == '__main__':
    test_actual_overview()