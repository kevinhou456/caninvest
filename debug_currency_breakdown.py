#!/usr/bin/env python3

import sys
import os
sys.path.append('/Users/luzhang/Desktop/web1/canadian_family_investment')

from datetime import date
from app import create_app
from app.services.asset_valuation_service import AssetValuationService

def debug_currency_breakdown():
    app = create_app()

    with app.app_context():
        from app.models.transaction import Transaction

        print("=== 账户2货币字段调试 ===")

        # 检查所有交易的货币字段
        transactions = Transaction.query.filter_by(account_id=2)\
            .order_by(Transaction.trade_date.asc()).all()

        print(f"总交易数: {len(transactions)}")

        # 统计货币字段
        currency_stats = {}
        no_currency_count = 0

        for tx in transactions:
            currency = tx.currency
            if currency:
                if currency not in currency_stats:
                    currency_stats[currency] = {'count': 0, 'types': set()}
                currency_stats[currency]['count'] += 1
                currency_stats[currency]['types'].add(tx.type)
            else:
                no_currency_count += 1

        print(f"\n--- 货币字段统计 ---")
        for currency, data in currency_stats.items():
            print(f"{currency}: {data['count']}笔交易, 类型: {list(data['types'])}")

        if no_currency_count > 0:
            print(f"无货币字段: {no_currency_count}笔交易")

        # 检查现金相关交易的货币字段
        print(f"\n--- 现金相关交易详情 ---")
        cash_transactions = [tx for tx in transactions if tx.type in ['DEPOSIT', 'WITHDRAW', 'DIVIDEND', 'INTEREST']]

        for tx in cash_transactions[:10]:  # 显示前10笔
            currency = tx.currency or "无货币字段"
            amount = tx.amount or "无金额"
            print(f"{tx.trade_date}: {tx.type} {currency} ${amount}")

        # 检查股票交易的货币字段
        print(f"\n--- 股票交易货币获取测试 ---")
        asset_service = AssetValuationService()
        stock_transactions = [tx for tx in transactions if tx.type in ['BUY', 'SELL'] and tx.stock]

        unique_stocks = list(set([tx.stock for tx in stock_transactions if tx.stock]))

        for symbol in unique_stocks[:5]:  # 测试前5个股票
            stock_info = asset_service._get_stock_info(symbol)
            currency = stock_info.get('currency', 'USD')
            print(f"{symbol}: 推断货币为 {currency}")

        # 手动测试现金反推计算
        print(f"\n--- 手动测试现金反推 ---")
        test_date = date(2025, 8, 24)

        try:
            cad_balance, usd_balance = asset_service._calculate_cash_balance(2, test_date)
            print(f"计算结果: CAD=${cad_balance}, USD=${usd_balance}")
        except Exception as e:
            print(f"计算出错: {e}")
            import traceback
            traceback.print_exc()

if __name__ == '__main__':
    debug_currency_breakdown()