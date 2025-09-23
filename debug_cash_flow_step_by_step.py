#!/usr/bin/env python3

import sys
import os
sys.path.append('/Users/luzhang/Desktop/web1/canadian_family_investment')

from datetime import date
from app import create_app
from app.services.asset_valuation_service import AssetValuationService
from decimal import Decimal

def debug_cash_flow_step_by_step():
    app = create_app()

    with app.app_context():
        from app.models.transaction import Transaction

        print("=== 账户2现金流逐步调试 ===")

        asset_service = AssetValuationService()
        account_id = 2
        target_date = date(2025, 8, 24)

        print(f"目标日期: {target_date}")

        # 1. 获取到目标日期的所有交易
        transactions = Transaction.query.filter(
            Transaction.account_id == account_id,
            Transaction.trade_date <= target_date,
            Transaction.type.in_(['DEPOSIT', 'WITHDRAW', 'BUY', 'SELL', 'DIVIDEND', 'INTEREST'])
        ).order_by(Transaction.trade_date.asc()).all()

        print(f"相关交易数: {len(transactions)}")

        # 2. 方法1：从零开始累计计算
        print(f"\n--- 方法1：从零开始累计 ---")
        cad_balance = Decimal('0')
        usd_balance = Decimal('0')

        for i, tx in enumerate(transactions[:20]):  # 只显示前20笔
            old_cad, old_usd = cad_balance, usd_balance
            cad_balance, usd_balance = asset_service._apply_transaction_impact(
                cad_balance, usd_balance, tx
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

            print(f"  {i+1}. {tx.trade_date}: {tx.type} {tx.stock or 'N/A'} {change_str}")
            print(f"     前: CAD=${old_cad}, USD=${old_usd}")
            print(f"     后: CAD=${cad_balance}, USD=${usd_balance}")

        print(f"\n最终结果 - CAD: ${cad_balance}, USD: ${usd_balance}")

        if cad_balance < 0 or usd_balance < 0:
            print(f"出现负余额，需要反推计算")

            # 3. 方法2：反推计算
            print(f"\n--- 方法2：反推计算 ---")
            try:
                reverse_cad, reverse_usd = asset_service._calculate_cash_balance_reverse(account_id, target_date)
                print(f"反推结果 - CAD: ${reverse_cad}, USD: ${reverse_usd}")
            except Exception as e:
                print(f"反推计算出错: {e}")
                import traceback
                traceback.print_exc()

        else:
            print(f"余额为正，不需要反推")

        # 4. 检查当前现金记录
        print(f"\n--- 当前现金记录 ---")
        from app.models.cash import Cash
        cash_record = Cash.get_account_cash(account_id)
        if cash_record:
            print(f"数据库记录 - CAD: ${cash_record.cad}, USD: ${cash_record.usd}")
        else:
            print("无现金记录")

if __name__ == '__main__':
    debug_cash_flow_step_by_step()