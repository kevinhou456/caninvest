#!/usr/bin/env python3

import sys
import os
sys.path.append('/Users/luzhang/Desktop/web1/canadian_family_investment')

from datetime import date, timedelta
from app import create_app

def debug_cash_jump():
    app = create_app()

    with app.app_context():
        print("=== 调试账户7现金突然增加150万的问题 ===")

        account_id = 7
        today = date.today()
        yesterday = today - timedelta(days=1)

        from app.services.asset_valuation_service import AssetValuationService
        from app.models.cash import Cash
        from app.models.transaction import Transaction

        asset_service = AssetValuationService()

        print(f"--- 对比昨天和今天的现金计算 ---")

        # 昨天的现金
        try:
            yesterday_cash = asset_service.get_cash_balance(account_id, yesterday)
            print(f"昨天({yesterday})现金余额:")
            print(f"  CAD: ${yesterday_cash['cad']}")
            print(f"  USD: ${yesterday_cash['usd']}")
            print(f"  总计(CAD): ${yesterday_cash['total_cad']}")
        except Exception as e:
            print(f"昨天现金计算出错: {e}")

        # 今天的现金
        try:
            today_cash = asset_service.get_cash_balance(account_id, today)
            print(f"\n今天({today})现金余额:")
            print(f"  CAD: ${today_cash['cad']}")
            print(f"  USD: ${today_cash['usd']}")
            print(f"  总计(CAD): ${today_cash['total_cad']}")
        except Exception as e:
            print(f"今天现金计算出错: {e}")

        print(f"\n--- 检查Cash表中的实际数据 ---")
        # 检查Cash表
        cash_record = Cash.get_account_cash(account_id)
        if cash_record:
            print(f"Cash表中的记录:")
            print(f"  CAD: ${cash_record.cad}")
            print(f"  USD: ${cash_record.usd}")
            print(f"  更新时间: {cash_record.updated_at}")
        else:
            print("Cash表中没有该账户的记录")

        print(f"\n--- 检查最近的交易记录 ---")
        # 检查最近几天的交易
        recent_transactions = Transaction.query.filter(
            Transaction.account_id == account_id,
            Transaction.trade_date >= yesterday - timedelta(days=2)
        ).order_by(Transaction.trade_date.desc()).all()

        print(f"最近{len(recent_transactions)}笔交易:")
        for tx in recent_transactions:
            if tx.type in ['DEPOSIT', 'WITHDRAWAL']:
                print(f"  {tx.trade_date}: {tx.type} ${tx.amount}({tx.currency})")
            elif tx.type in ['BUY', 'SELL']:
                total = float(tx.quantity) * float(tx.price)
                print(f"  {tx.trade_date}: {tx.type} {tx.stock} {tx.quantity}股@${tx.price} = ${total:.2f}({tx.currency}) 手续费${tx.fee}")
            else:
                print(f"  {tx.trade_date}: {tx.type} ${tx.amount}({tx.currency})")

        print(f"\n--- 分别计算今天和昨天的现金（调试模式）---")
        # 手动计算昨天的现金
        print(f"\n昨天现金计算过程:")
        try:
            cad_yesterday, usd_yesterday = asset_service._calculate_historical_cash_balance(account_id, yesterday)
            print(f"  历史计算结果: CAD=${cad_yesterday}, USD=${usd_yesterday}")
        except Exception as e:
            print(f"  历史计算出错: {e}")
            import traceback
            traceback.print_exc()

        # 手动计算今天的现金
        print(f"\n今天现金计算过程:")
        # 今天应该从Cash表读取
        if cash_record:
            print(f"  从Cash表读取: CAD=${cash_record.cad}, USD=${cash_record.usd}")
        else:
            print(f"  Cash表无记录，回退到历史计算")
            try:
                cad_today, usd_today = asset_service._calculate_historical_cash_balance(account_id, today)
                print(f"  历史计算结果: CAD=${cad_today}, USD=${usd_today}")
            except Exception as e:
                print(f"  历史计算出错: {e}")

if __name__ == '__main__':
    debug_cash_jump()