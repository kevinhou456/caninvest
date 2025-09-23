#!/usr/bin/env python3

import sys
import os
sys.path.append('/Users/luzhang/Desktop/web1/canadian_family_investment')

from datetime import date
from app import create_app
from app.services.asset_valuation_service import AssetValuationService
from decimal import Decimal

def debug_reverse_logic():
    app = create_app()

    with app.app_context():
        from app.models.transaction import Transaction
        from app.models.cash import Cash

        print("=== 现金反推逻辑详细分析 ===")

        asset_service = AssetValuationService()
        account_id = 2
        target_date = date(2025, 8, 24)

        print(f"目标日期: {target_date}")

        # 1. 获取当前现金余额（反推起点）
        print(f"\n--- 1. 反推起点 ---")
        cash_record = Cash.get_account_cash(account_id)
        if cash_record:
            current_cad = Decimal(str(cash_record.cad or 0))
            current_usd = Decimal(str(cash_record.usd or 0))
        else:
            current_cad = Decimal('0')
            current_usd = Decimal('0')

        print(f"当前现金余额 - CAD: ${current_cad}, USD: ${current_usd}")

        # 2. 获取目标日期后的所有交易（需要反向处理）
        print(f"\n--- 2. 目标日期后的交易 ---")
        future_transactions = Transaction.query.filter(
            Transaction.account_id == account_id,
            Transaction.trade_date > target_date,
            Transaction.type.in_(['DEPOSIT', 'WITHDRAW', 'BUY', 'SELL', 'DIVIDEND', 'INTEREST'])
        ).order_by(Transaction.trade_date.desc()).all()

        print(f"目标日期后交易数: {len(future_transactions)}")

        # 3. 逐步反推每笔交易
        print(f"\n--- 3. 逐步反推交易 ---")
        historical_cad = current_cad
        historical_usd = current_usd

        for i, tx in enumerate(future_transactions[:20]):  # 显示前20笔
            old_cad, old_usd = historical_cad, historical_usd

            # 手动执行反向交易逻辑
            currency = tx.currency or 'USD'

            if tx.type == 'DEPOSIT':
                # 反向：减去存款
                amount = Decimal(str(tx.amount or 0))
                if currency == 'CAD':
                    historical_cad -= amount
                else:
                    historical_usd -= amount
                operation = f"反向DEPOSIT: -{amount} {currency}"

            elif tx.type == 'WITHDRAW':
                # 反向：加上取款
                amount = Decimal(str(tx.amount or 0))
                if currency == 'CAD':
                    historical_cad += amount
                else:
                    historical_usd += amount
                operation = f"反向WITHDRAW: +{amount} {currency}"

            elif tx.type == 'BUY':
                # 反向：加回买入成本
                quantity = Decimal(str(tx.quantity or 0))
                price = Decimal(str(tx.price or 0))
                total_cost = quantity * price + Decimal(str(tx.fee or 0))
                if currency == 'CAD':
                    historical_cad += total_cost
                else:
                    historical_usd += total_cost
                operation = f"反向BUY: +{total_cost} {currency}"

            elif tx.type == 'SELL':
                # 反向：减去卖出收入
                quantity = Decimal(str(tx.quantity or 0))
                price = Decimal(str(tx.price or 0))
                net_proceeds = quantity * price - Decimal(str(tx.fee or 0))
                if currency == 'CAD':
                    historical_cad -= net_proceeds
                else:
                    historical_usd -= net_proceeds
                operation = f"反向SELL: -{net_proceeds} {currency}"

            elif tx.type == 'DIVIDEND':
                # 反向：减去分红收入
                amount = Decimal(str(tx.amount or 0))
                if currency == 'CAD':
                    historical_cad -= amount
                else:
                    historical_usd -= amount
                operation = f"反向DIVIDEND: -{amount} {currency}"

            elif tx.type == 'INTEREST':
                # 反向：减去利息收入
                amount = Decimal(str(tx.amount or 0))
                if currency == 'CAD':
                    historical_cad -= amount
                else:
                    historical_usd -= amount
                operation = f"反向INTEREST: -{amount} {currency}"
            else:
                operation = "未知交易类型"

            print(f"  {i+1}. {tx.trade_date}: {tx.type} {tx.stock or 'N/A'}")
            print(f"     操作: {operation}")
            print(f"     前: CAD=${old_cad}, USD=${old_usd}")
            print(f"     后: CAD=${historical_cad}, USD=${historical_usd}")

        print(f"\n--- 4. 反推最终结果 ---")
        print(f"反推到{target_date}的现金余额:")
        print(f"CAD: ${historical_cad}")
        print(f"USD: ${historical_usd}")

        # 5. 检查是否需要补偿
        print(f"\n--- 5. 补偿需求分析 ---")
        need_supplement_cad = Decimal('0')
        need_supplement_usd = Decimal('0')

        if historical_cad < 0:
            need_supplement_cad = -historical_cad
            print(f"CAD需要补偿: ${need_supplement_cad}")
        else:
            print(f"CAD无需补偿")

        if historical_usd < 0:
            need_supplement_usd = -historical_usd
            print(f"USD需要补偿: ${need_supplement_usd}")
        else:
            print(f"USD无需补偿")

        # 6. 验证使用AssetValuationService的结果
        print(f"\n--- 6. AssetValuationService验证 ---")
        try:
            service_cad, service_usd = asset_service._calculate_cash_balance_reverse(account_id, target_date)
            print(f"Service结果 - CAD: ${service_cad}, USD: ${service_usd}")

            print(f"差异分析:")
            print(f"  CAD差异: ${service_cad - historical_cad}")
            print(f"  USD差异: ${service_usd - historical_usd}")

        except Exception as e:
            print(f"Service计算出错: {e}")

if __name__ == '__main__':
    debug_reverse_logic()