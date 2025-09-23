#!/usr/bin/env python3

import sys
import os
sys.path.append('/Users/luzhang/Desktop/web1/canadian_family_investment')

from datetime import date
from app import create_app
from app.services.asset_valuation_service import AssetValuationService
from decimal import Decimal

def debug_supplement_manual():
    app = create_app()

    with app.app_context():
        from app.models.transaction import Transaction

        print("=== 手动复现_forward_calculate_with_supplement ===")

        asset_service = AssetValuationService()
        account_id = 2
        target_date = date(2025, 8, 24)
        supplement_cad = Decimal('0')
        supplement_usd = Decimal('1004.19')

        print(f"目标日期: {target_date}")
        print(f"补偿金额: CAD=${supplement_cad}, USD=${supplement_usd}")

        # 获取该账户最早的交易日期
        earliest_transaction = Transaction.query.filter_by(account_id=account_id)\
            .order_by(Transaction.trade_date.asc()).first()

        if earliest_transaction:
            print(f"最早交易: {earliest_transaction.trade_date}")
        else:
            print("没有交易记录")
            return

        # 从最早交易日期开始，以补充金额作为初始余额
        current_cad = supplement_cad
        current_usd = supplement_usd

        # 获取从最早交易到目标日期的所有交易
        transactions = Transaction.query.filter(
            Transaction.account_id == account_id,
            Transaction.trade_date <= target_date,
            Transaction.type.in_(['DEPOSIT', 'WITHDRAW', 'BUY', 'SELL', 'DIVIDEND', 'INTEREST'])
        ).order_by(Transaction.trade_date.asc()).all()

        print(f"需要处理的交易数: {len(transactions)}")

        # 手动应用所有交易影响
        print(f"\n--- 逐笔交易处理 ---")
        for i, tx in enumerate(transactions[:30]):  # 显示前30笔
            old_cad, old_usd = current_cad, current_usd

            # 手动调用_apply_transaction_impact
            current_cad, current_usd = asset_service._apply_transaction_impact(
                current_cad, current_usd, tx
            )

            # 计算变化
            change_cad = current_cad - old_cad
            change_usd = current_usd - old_usd

            # 显示交易详情
            currency = tx.currency or 'USD'
            if tx.type in ['DEPOSIT', 'WITHDRAW', 'DIVIDEND', 'INTEREST']:
                amount = tx.amount or 0
                detail = f"{tx.type} {amount} {currency}"
            elif tx.type in ['BUY', 'SELL']:
                quantity = tx.quantity or 0
                price = tx.price or 0
                fee = tx.fee or 0
                if tx.type == 'BUY':
                    total = quantity * price + fee
                else:
                    total = quantity * price - fee
                detail = f"{tx.type} {tx.stock} {quantity}@{price} = {total} {currency}"
            else:
                detail = f"{tx.type} {tx.stock or 'N/A'}"

            print(f"  {i+1:2d}. {tx.trade_date}: {detail}")
            print(f"      变化: CAD{change_cad:+}, USD{change_usd:+}")
            print(f"      余额: CAD=${current_cad}, USD=${current_usd}")

            # 检查是否出现了异常大的变化
            if abs(change_cad) > 10000 or abs(change_usd) > 10000:
                print(f"      *** 异常大的变化！检查交易详情 ***")
                print(f"          交易类型: {tx.type}")
                print(f"          股票: {tx.stock}")
                print(f"          数量: {tx.quantity}")
                print(f"          价格: {tx.price}")
                print(f"          金额: {tx.amount}")
                print(f"          费用: {tx.fee}")
                print(f"          货币: {tx.currency}")

        print(f"\n--- 最终结果 ---")
        print(f"手动计算结果: CAD=${current_cad}, USD=${current_usd}")

        # 对比Service方法的结果
        service_cad, service_usd = asset_service._forward_calculate_with_supplement(
            account_id, target_date, supplement_cad, supplement_usd
        )
        print(f"Service方法结果: CAD=${service_cad}, USD=${service_usd}")

        print(f"差异: CAD=${service_cad - current_cad}, USD=${service_usd - current_usd}")

if __name__ == '__main__':
    debug_supplement_manual()