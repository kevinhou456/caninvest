#!/usr/bin/env python3

import sys
import os
sys.path.append('/Users/luzhang/Desktop/web1/canadian_family_investment')

from datetime import date
from app import create_app

def debug_missing_amounts():
    app = create_app()

    with app.app_context():
        from app.models.transaction import Transaction

        print("=== 账户2缺失金额调试 ===")

        # 查看所有字段来了解交易记录的完整结构
        first_tx = Transaction.query.filter_by(account_id=2).first()
        if first_tx:
            print("--- 交易记录字段 ---")
            for column in Transaction.__table__.columns:
                field_name = column.name
                field_value = getattr(first_tx, field_name, 'N/A')
                print(f"{field_name}: {field_value}")

        # 检查有问题的交易记录
        print(f"\n--- 无金额的现金交易 ---")
        cash_txs_no_amount = Transaction.query.filter(
            Transaction.account_id == 2,
            Transaction.type.in_(['DEPOSIT', 'WITHDRAW', 'DIVIDEND', 'INTEREST']),
            Transaction.amount.is_(None)
        ).order_by(Transaction.trade_date.asc()).all()

        print(f"无金额现金交易数: {len(cash_txs_no_amount)}")

        for tx in cash_txs_no_amount[:10]:  # 显示前10笔
            print(f"  {tx.trade_date}: {tx.type} {tx.currency} - ID: {tx.id}")
            print(f"    amount: {tx.amount}, quantity: {tx.quantity}, price: {tx.price}, fee: {tx.fee}")
            print(f"    stock: {tx.stock}, description: {getattr(tx, 'description', 'N/A')}")

        # 检查有金额的现金交易
        print(f"\n--- 有金额的现金交易 ---")
        cash_txs_with_amount = Transaction.query.filter(
            Transaction.account_id == 2,
            Transaction.type.in_(['DEPOSIT', 'WITHDRAW', 'DIVIDEND', 'INTEREST']),
            Transaction.amount.isnot(None)
        ).order_by(Transaction.trade_date.asc()).all()

        print(f"有金额现金交易数: {len(cash_txs_with_amount)}")

        for tx in cash_txs_with_amount[:10]:  # 显示前10笔
            print(f"  {tx.trade_date}: {tx.type} {tx.currency} ${tx.amount}")

        # 检查股票交易是否有合理的价格和数量
        print(f"\n--- 股票交易检查 ---")
        stock_txs = Transaction.query.filter(
            Transaction.account_id == 2,
            Transaction.type.in_(['BUY', 'SELL'])
        ).order_by(Transaction.trade_date.asc()).all()

        print(f"股票交易数: {len(stock_txs)}")

        total_buy_value = 0
        total_sell_value = 0
        for tx in stock_txs[:10]:  # 显示前10笔
            quantity = tx.quantity or 0
            price = tx.price or 0
            fee = tx.fee or 0
            value = quantity * price + fee if tx.type == 'BUY' else quantity * price - fee

            if tx.type == 'BUY':
                total_buy_value += value
            else:
                total_sell_value += value

            print(f"  {tx.trade_date}: {tx.type} {tx.stock} {quantity}股 @${price} (费用:${fee}) = ${value}")

        print(f"\n总买入价值(前10笔): ${total_buy_value}")
        print(f"总卖出价值(前10笔): ${total_sell_value}")

if __name__ == '__main__':
    debug_missing_amounts()