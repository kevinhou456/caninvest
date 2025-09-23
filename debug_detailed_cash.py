#!/usr/bin/env python3

import sys
import os
sys.path.append('/Users/luzhang/Desktop/web1/canadian_family_investment')

from datetime import date, timedelta
from app import create_app
from app.services.asset_valuation_service import AssetValuationService

def debug_detailed_cash():
    app = create_app()

    with app.app_context():
        asset_service = AssetValuationService()
        account_id = 2

        print("=== 账户2现金余额详细调试 ===")

        today = date.today()
        test_dates = [
            today - timedelta(days=29),  # 1M ago - should be 0
            today - timedelta(days=89),  # 3M ago - should be 0
            today - timedelta(days=179), # 6M ago - should be 0
            date(2024, 10, 30),          # First transaction date
            date(2024, 10, 29),          # Day before first transaction - should be 0
            today                        # Today
        ]

        for test_date in test_dates:
            print(f"\n--- {test_date} 现金余额分析 ---")

            try:
                # 检查股票持仓
                holdings = asset_service._get_holdings_at_date(account_id, test_date)
                print(f"  股票持仓: {holdings}")

                # 检查现金余额
                cash_cad, cash_usd = asset_service._calculate_cash_balance(account_id, test_date)
                print(f"  现金CAD: {cash_cad}")
                print(f"  现金USD: {cash_usd}")

                # 计算股票市值
                stock_value, stock_details = asset_service._calculate_stock_market_value(account_id, test_date)
                print(f"  股票市值: {stock_value}")

                # 总资产
                usd_to_cad = 1.35  # 假设汇率
                cash_total_cad = cash_cad + cash_usd * 1.35
                total_assets = stock_value + cash_total_cad
                print(f"  总资产: {total_assets}")

                # 如果有异常情况，显示更多详情
                if test_date < date(2024, 10, 30) and total_assets > 0:
                    print(f"  *** 异常：第一笔交易前应该为0，但计算出了 {total_assets} ***")
                    print(f"      股票详情: {stock_details}")

            except Exception as e:
                print(f"  错误: {e}")

        # 检查交易记录时间范围
        print(f"\n--- 交易记录检查 ---")
        from app.models.transaction import Transaction

        # 找到第一笔和最后一笔交易
        first_tx = Transaction.query.filter_by(account_id=account_id).order_by(Transaction.trade_date.asc()).first()
        last_tx = Transaction.query.filter_by(account_id=account_id).order_by(Transaction.trade_date.desc()).first()

        if first_tx:
            print(f"第一笔交易: {first_tx.trade_date} - {first_tx.type} - {first_tx.stock}")

        if last_tx:
            print(f"最后一笔交易: {last_tx.trade_date} - {last_tx.type} - {last_tx.stock}")

        # 检查现金交易记录
        cash_txs = Transaction.query.filter(
            Transaction.account_id == account_id,
            Transaction.type.in_(['DEPOSIT', 'WITHDRAWAL'])
        ).order_by(Transaction.trade_date.asc()).all()

        print(f"现金交易记录数: {len(cash_txs)}")
        for tx in cash_txs[:5]:  # 显示前5条
            print(f"  {tx.trade_date}: {tx.type} ${tx.amount}")

if __name__ == '__main__':
    debug_detailed_cash()