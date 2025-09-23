#!/usr/bin/env python3

import sys
import os
sys.path.append('/Users/luzhang/Desktop/web1/canadian_family_investment')

from datetime import date
from app import create_app

def debug_account7_dividends():
    app = create_app()

    with app.app_context():
        print("=== 调试账户7的分红数据 ===")

        from app.models.transaction import Transaction

        # 查看账户7的分红交易详情
        account_7_dividends = Transaction.query.filter(
            Transaction.account_id == 7,
            Transaction.type == 'DIVIDEND'
        ).order_by(Transaction.trade_date.desc()).limit(20).all()

        print(f"账户7最近20笔分红交易:")
        for tx in account_7_dividends:
            print(f"  ID:{tx.id} 日期:{tx.trade_date} 股票:{tx.stock} 金额:{tx.amount} 币种:{tx.currency} 数量:{tx.quantity} 价格:{tx.price}")

        # 检查是否有非零分红
        non_zero_dividends = Transaction.query.filter(
            Transaction.account_id == 7,
            Transaction.type == 'DIVIDEND',
            Transaction.amount != 0,
            Transaction.amount.isnot(None)
        ).all()

        print(f"\n账户7非零分红交易数量: {len(non_zero_dividends)}")
        for tx in non_zero_dividends[:10]:
            print(f"  日期:{tx.trade_date} 股票:{tx.stock} 金额:{tx.amount} 币种:{tx.currency}")

        # 检查数据库字段类型和值
        print(f"\n=== 检查数据库字段情况 ===")
        sample_tx = account_7_dividends[0] if account_7_dividends else None
        if sample_tx:
            print(f"示例交易字段:")
            print(f"  amount type: {type(sample_tx.amount)}")
            print(f"  amount value: {repr(sample_tx.amount)}")
            print(f"  amount == 0: {sample_tx.amount == 0}")
            print(f"  amount is None: {sample_tx.amount is None}")

        # 检查所有账户的分红情况
        print(f"\n=== 所有账户分红汇总 ===")
        from sqlalchemy import func
        dividend_summary = Transaction.query.filter(
            Transaction.type == 'DIVIDEND'
        ).with_entities(
            Transaction.account_id,
            func.count(Transaction.id).label('count'),
            func.sum(Transaction.amount).label('total_amount')
        ).group_by(Transaction.account_id).all()

        for account_id, count, total_amount in dividend_summary:
            print(f"  账户{account_id}: {count}笔分红, 总额${total_amount or 0}")

if __name__ == '__main__':
    debug_account7_dividends()