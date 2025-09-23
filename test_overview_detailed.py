#!/usr/bin/env python3

import sys
import os
sys.path.append('/Users/luzhang/Desktop/web1/canadian_family_investment')

from datetime import date
from app import create_app

def test_overview_detailed():
    app = create_app()

    with app.app_context():
        print("=== 详细检查Overview页面数据 ===")

        from app.models.family import Family
        from app.models.account import Account
        from app.services.asset_valuation_service import AssetValuationService

        # 获取家庭和账户
        family = Family.query.first()
        accounts = Account.query.filter_by(family_id=family.id).all()
        account_ids = [acc.id for acc in accounts]

        print(f"账户列表: {[(acc.id, acc.name) for acc in accounts]}")

        # 初始化服务
        asset_service = AssetValuationService()

        # 获取综合投资组合指标 - 这是overview页面使用的
        metrics = asset_service.get_comprehensive_portfolio_metrics(account_ids)

        print(f"\n=== Overview页面完整数据结构 ===")

        # 总资产
        print(f"总资产:")
        total_assets = metrics.get('total_assets', {})
        for key, value in total_assets.items():
            print(f"  {key}: ${value}")

        # 总回报
        print(f"\n总回报:")
        total_return = metrics.get('total_return', {})
        for key, value in total_return.items():
            print(f"  {key}: ${value}")

        # 已实现收益
        print(f"\n已实现收益:")
        realized_gain = metrics.get('realized_gain', {})
        for key, value in realized_gain.items():
            print(f"  {key}: ${value}")

        # 浮动盈亏
        print(f"\n浮动盈亏:")
        unrealized_gain = metrics.get('unrealized_gain', {})
        for key, value in unrealized_gain.items():
            print(f"  {key}: ${value}")

        # 分红
        print(f"\n分红:")
        dividends = metrics.get('dividends', {})
        for key, value in dividends.items():
            print(f"  {key}: ${value}")

        # 利息
        print(f"\n利息:")
        interest = metrics.get('interest', {})
        for key, value in interest.items():
            print(f"  {key}: ${value}")

        # 现金余额
        print(f"\n现金余额:")
        cash_balance = metrics.get('cash_balance', {})
        for key, value in cash_balance.items():
            print(f"  {key}: ${value}")

        # 股票市值
        print(f"\n股票市值:")
        stock_value = metrics.get('stock_value', {})
        for key, value in stock_value.items():
            print(f"  {key}: ${value}")

        # 检查单个账户的数据作为对比
        print(f"\n=== 单个账户数据对比 ===")
        for account_id in account_ids[:3]:  # 只检查前3个账户
            snapshot = asset_service.get_asset_snapshot(account_id)
            print(f"账户{account_id}: 总资产=${snapshot.total_assets}, 股票=${snapshot.stock_market_value}, 现金=${snapshot.cash_balance_total_cad}")

if __name__ == '__main__':
    test_overview_detailed()