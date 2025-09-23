#!/usr/bin/env python3

import sys
import os
sys.path.append('/Users/luzhang/Desktop/web1/canadian_family_investment')

from datetime import date, timedelta
from app import create_app
from decimal import Decimal

def verify_cash_consistency():
    app = create_app()

    with app.app_context():
        print("=== 验证现金计算一致性：今天→历史→今天 ===")

        account_id = 7
        today = date.today()

        from app.services.asset_valuation_service import AssetValuationService
        from app.models.cash import Cash
        from app.models.transaction import Transaction

        asset_service = AssetValuationService()

        print(f"账户ID: {account_id}")
        print(f"今天日期: {today}")

        # 1. 获取今天的真实现金（Cash表）
        cash_record = Cash.get_account_cash(account_id)
        if cash_record:
            real_cad_today = Decimal(str(cash_record.cad))
            real_usd_today = Decimal(str(cash_record.usd))
            print(f"\n--- 第1步：今天的真实现金（Cash表）---")
            print(f"CAD: ${real_cad_today}")
            print(f"USD: ${real_usd_today}")
        else:
            print("ERROR: Cash表中没有该账户记录")
            return

        # 2. 使用历史交易记录计算今天的现金
        print(f"\n--- 第2步：用交易记录计算今天的现金 ---")
        calculated_cad_today, calculated_usd_today = asset_service._calculate_historical_cash_balance(account_id, today)
        print(f"CAD: ${calculated_cad_today}")
        print(f"USD: ${calculated_usd_today}")

        # 3. 检查差异
        cad_diff = real_cad_today - calculated_cad_today
        usd_diff = real_usd_today - calculated_usd_today
        print(f"\n--- 第3步：差异分析 ---")
        print(f"CAD差异: ${cad_diff} (真实 - 计算)")
        print(f"USD差异: ${usd_diff} (真实 - 计算)")

        tolerance = Decimal('0.01')  # 允许1分钱的误差
        if abs(cad_diff) <= tolerance and abs(usd_diff) <= tolerance:
            print("✓ 现金计算一致，无问题")
        else:
            print("✗ 现金计算不一致，存在计算错误")

            # 4. 如果不一致，分析原因
            print(f"\n--- 第4步：分析计算错误原因 ---")

            # 获取所有交易记录
            transactions = Transaction.query.filter(
                Transaction.account_id == account_id,
                Transaction.type.in_(['DEPOSIT', 'WITHDRAWAL', 'BUY', 'SELL', 'DIVIDEND', 'INTEREST'])
            ).order_by(Transaction.trade_date.asc()).all()

            print(f"总交易记录数: {len(transactions)}")

            # 手动逐步计算
            cad_balance = Decimal('0')
            usd_balance = Decimal('0')

            print(f"\n逐笔交易分析:")
            print(f"初始: CAD=${cad_balance}, USD=${usd_balance}")

            # 显示最近10笔交易的影响
            recent_transactions = transactions[-10:] if len(transactions) > 10 else transactions

            for i, tx in enumerate(recent_transactions):
                old_cad, old_usd = cad_balance, usd_balance
                cad_balance, usd_balance = asset_service._apply_transaction_impact(cad_balance, usd_balance, tx)

                cad_change = cad_balance - old_cad
                usd_change = usd_balance - old_usd

                if abs(cad_change) > Decimal('0.01') or abs(usd_change) > Decimal('0.01'):
                    print(f"  {tx.trade_date}: {tx.type} {tx.stock or ''}")
                    if tx.type in ['DEPOSIT', 'WITHDRAWAL', 'DIVIDEND', 'INTEREST']:
                        print(f"    金额: ${tx.amount}({tx.currency})")
                    else:
                        print(f"    {tx.quantity}股 x ${tx.price} + 手续费${tx.fee} ({tx.currency})")
                    print(f"    现金变化: CAD${cad_change:+}, USD${usd_change:+}")
                    print(f"    余额: CAD=${cad_balance}, USD=${usd_balance}")

            print(f"\n最终计算结果: CAD=${cad_balance}, USD=${usd_balance}")
            print(f"应该匹配的真实值: CAD=${real_cad_today}, USD=${real_usd_today}")

        # 5. 验证反向一致性：如果我们有正确的今天现金，能否反推出正确的历史现金
        print(f"\n--- 第5步：反向验证（假设今天现金正确）---")
        yesterday = today - timedelta(days=1)
        week_ago = today - timedelta(days=7)

        print("如果以今天的真实现金为基准，反推历史现金:")

        # 获取今天的交易
        today_transactions = Transaction.query.filter(
            Transaction.account_id == account_id,
            Transaction.trade_date == today,
            Transaction.type.in_(['DEPOSIT', 'WITHDRAWAL', 'BUY', 'SELL', 'DIVIDEND', 'INTEREST'])
        ).all()

        if today_transactions:
            print(f"今天有{len(today_transactions)}笔交易，需要反推")

            # 反推昨天的现金
            reverse_cad = real_cad_today
            reverse_usd = real_usd_today

            for tx in today_transactions:
                # 反向应用交易影响
                currency = tx.currency or 'USD'

                if tx.type == 'DEPOSIT':
                    amount = Decimal(str(tx.amount or 0))
                    if currency == 'CAD':
                        reverse_cad -= amount  # 反向：减去存入
                    else:
                        reverse_usd -= amount

                elif tx.type == 'WITHDRAWAL':
                    amount = Decimal(str(tx.amount or 0))
                    if currency == 'CAD':
                        reverse_cad += amount  # 反向：加回取出
                    else:
                        reverse_usd += amount

                elif tx.type == 'BUY':
                    quantity = Decimal(str(tx.quantity or 0))
                    price = Decimal(str(tx.price or 0))
                    total_cost = quantity * price + Decimal(str(tx.fee or 0))
                    if currency == 'CAD':
                        reverse_cad += total_cost  # 反向：加回买入花费
                    else:
                        reverse_usd += total_cost

                elif tx.type == 'SELL':
                    quantity = Decimal(str(tx.quantity or 0))
                    price = Decimal(str(tx.price or 0))
                    net_proceeds = quantity * price - Decimal(str(tx.fee or 0))
                    if currency == 'CAD':
                        reverse_cad -= net_proceeds  # 反向：减去卖出收入
                    else:
                        reverse_usd -= net_proceeds

            print(f"反推的昨天现金: CAD=${reverse_cad}, USD=${reverse_usd}")

            # 用历史计算验证昨天
            hist_cad_yesterday, hist_usd_yesterday = asset_service._calculate_historical_cash_balance(account_id, yesterday)
            print(f"历史计算昨天现金: CAD=${hist_cad_yesterday}, USD=${hist_usd_yesterday}")

            reverse_cad_diff = reverse_cad - hist_cad_yesterday
            reverse_usd_diff = reverse_usd - hist_usd_yesterday
            print(f"反推 vs 历史计算差异: CAD${reverse_cad_diff:+}, USD${reverse_usd_diff:+}")

        else:
            print("今天没有交易，昨天的现金应该和今天相同")
            hist_cad_yesterday, hist_usd_yesterday = asset_service._calculate_historical_cash_balance(account_id, yesterday)
            print(f"历史计算昨天现金: CAD=${hist_cad_yesterday}, USD=${hist_usd_yesterday}")
            print(f"与今天真实现金对比: CAD${real_cad_today - hist_cad_yesterday:+}, USD${real_usd_today - hist_usd_yesterday:+}")

if __name__ == '__main__':
    verify_cash_consistency()