#!/usr/bin/env python3

import sys
import os
sys.path.append('/Users/luzhang/Desktop/web1/canadian_family_investment')

from datetime import date
from app import create_app
from app.services.asset_valuation_service import AssetValuationService
from decimal import Decimal

def debug_supplement_logic():
    app = create_app()

    with app.app_context():
        from app.models.transaction import Transaction

        print("=== 现金补偿逻辑调试 ===")

        asset_service = AssetValuationService()
        account_id = 2
        target_date = date(2025, 8, 24)

        # 手动执行反推计算的每个步骤
        print(f"目标日期: {target_date}")

        # 1. 反推计算
        print(f"\n--- 1. 手动反推计算 ---")
        from app.models.cash import Cash
        cash_record = Cash.get_account_cash(account_id)
        current_cad = Decimal('0')
        current_usd = Decimal('0')

        future_transactions = Transaction.query.filter(
            Transaction.account_id == account_id,
            Transaction.trade_date > target_date,
            Transaction.type.in_(['DEPOSIT', 'WITHDRAW', 'BUY', 'SELL', 'DIVIDEND', 'INTEREST'])
        ).order_by(Transaction.trade_date.desc()).all()

        historical_cad = current_cad
        historical_usd = current_usd

        print(f"目标日期后交易数: {len(future_transactions)}")

        for tx in future_transactions:
            historical_cad, historical_usd = asset_service._reverse_transaction_impact(
                historical_cad, historical_usd, tx
            )

        print(f"反推结果: CAD=${historical_cad}, USD=${historical_usd}")

        # 2. 检查是否需要补偿
        need_supplement_cad = Decimal('0')
        need_supplement_usd = Decimal('0')

        if historical_cad < 0:
            need_supplement_cad = -historical_cad
            historical_cad = Decimal('0')

        if historical_usd < 0:
            need_supplement_usd = -historical_usd
            historical_usd = Decimal('0')

        print(f"需要补偿: CAD=${need_supplement_cad}, USD=${need_supplement_usd}")

        # 3. 如果需要补偿，执行正推计算
        if need_supplement_cad > 0 or need_supplement_usd > 0:
            print(f"\n--- 2. 正推补偿计算 ---")

            # 获取最早交易
            earliest_transaction = Transaction.query.filter_by(account_id=account_id)\
                .order_by(Transaction.trade_date.asc()).first()

            if earliest_transaction:
                print(f"最早交易: {earliest_transaction.trade_date}")

            # 从最早交易开始，以补偿金额作为初始余额
            current_cad = need_supplement_cad
            current_usd = need_supplement_usd
            print(f"初始补偿余额: CAD=${current_cad}, USD=${current_usd}")

            # 获取从最早交易到目标日期的所有交易
            transactions = Transaction.query.filter(
                Transaction.account_id == account_id,
                Transaction.trade_date <= target_date,
                Transaction.type.in_(['DEPOSIT', 'WITHDRAW', 'BUY', 'SELL', 'DIVIDEND', 'INTEREST'])
            ).order_by(Transaction.trade_date.asc()).all()

            print(f"需要正推的交易数: {len(transactions)}")

            # 正向应用所有交易影响（显示前20笔）
            for i, tx in enumerate(transactions[:20]):
                old_cad, old_usd = current_cad, current_usd
                current_cad, current_usd = asset_service._apply_transaction_impact(
                    current_cad, current_usd, tx
                )

                currency = tx.currency or 'USD'
                if tx.type in ['DEPOSIT', 'WITHDRAW', 'DIVIDEND', 'INTEREST']:
                    amount = tx.amount or 0
                    change_str = f"${amount} {currency}"
                elif tx.type in ['BUY', 'SELL']:
                    quantity = tx.quantity or 0
                    price = tx.price or 0
                    fee = tx.fee or 0
                    if tx.type == 'BUY':
                        total = quantity * price + fee
                    else:
                        total = quantity * price - fee
                    change_str = f"${total} {currency}"
                else:
                    change_str = "N/A"

                print(f"  {i+1}. {tx.trade_date}: {tx.type} {change_str}")
                print(f"     前: CAD=${old_cad}, USD=${old_usd}")
                print(f"     后: CAD=${current_cad}, USD=${current_usd}")

            print(f"\n正推最终结果: CAD=${current_cad}, USD=${current_usd}")

            # 4. 使用AssetValuationService的补偿方法验证
            print(f"\n--- 3. Service补偿方法验证 ---")
            try:
                service_cad, service_usd = asset_service._forward_calculate_with_supplement(
                    account_id, target_date, need_supplement_cad, need_supplement_usd
                )
                print(f"Service补偿结果: CAD=${service_cad}, USD=${service_usd}")
                print(f"与手动计算差异: CAD=${service_cad - current_cad}, USD=${service_usd - current_usd}")
            except Exception as e:
                print(f"Service补偿计算出错: {e}")

        # 5. 完整的AssetValuationService结果
        print(f"\n--- 4. 完整Service结果验证 ---")
        try:
            final_cad, final_usd = asset_service._calculate_cash_balance_reverse(account_id, target_date)
            print(f"Service最终结果: CAD=${final_cad}, USD=${final_usd}")
        except Exception as e:
            print(f"Service最终计算出错: {e}")

if __name__ == '__main__':
    debug_supplement_logic()