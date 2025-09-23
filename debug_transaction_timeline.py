#!/usr/bin/env python3

import sys
import os
sys.path.append('/Users/luzhang/Desktop/web1/canadian_family_investment')

from datetime import date, timedelta
from app import create_app

def debug_transaction_timeline():
    app = create_app()

    with app.app_context():
        from app.models.transaction import Transaction

        print("=== 账户2交易时间线调试 ===")

        # 获取所有交易记录
        transactions = Transaction.query.filter_by(account_id=2)\
            .order_by(Transaction.trade_date.asc()).all()

        print(f"总交易数: {len(transactions)}")

        if transactions:
            first_tx = transactions[0]
            last_tx = transactions[-1]
            print(f"第一笔交易: {first_tx.trade_date}")
            print(f"最后一笔交易: {last_tx.trade_date}")

            # 显示所有交易的时间分布
            print(f"\n--- 交易时间分布 ---")
            years = {}
            for tx in transactions:
                year = tx.trade_date.year
                if year not in years:
                    years[year] = {'total': 0, 'types': {}}
                years[year]['total'] += 1

                tx_type = tx.type or 'UNKNOWN'
                if tx_type not in years[year]['types']:
                    years[year]['types'][tx_type] = 0
                years[year]['types'][tx_type] += 1

            for year, data in sorted(years.items()):
                print(f"{year}年: {data['total']}笔交易")
                for tx_type, count in data['types'].items():
                    print(f"  {tx_type}: {count}笔")

            # 显示最近的交易
            print(f"\n--- 最近10笔交易 ---")
            recent_transactions = transactions[-10:]
            for tx in recent_transactions:
                amount_str = f"${tx.amount}" if tx.amount else "N/A"
                quantity_str = f"{tx.quantity}股" if tx.quantity else "N/A"
                print(f"{tx.trade_date}: {tx.type} {tx.stock or 'N/A'} {quantity_str} {amount_str}")

        # 检查当前日期相对位置
        today = date.today()
        print(f"\n--- 当前日期: {today} ---")

        # 检查性能比较的日期范围
        test_ranges = [
            ('1M', today - timedelta(days=29)),
            ('3M', today - timedelta(days=89)),
            ('6M', today - timedelta(days=179)),
            ('YTD', date(today.year, 1, 1)),
            ('1Y', today - timedelta(days=364))
        ]

        print(f"\n--- 性能比较日期范围检查 ---")
        for label, start_date in test_ranges:
            if transactions:
                first_tx_date = transactions[0].trade_date
                if start_date < first_tx_date:
                    print(f"{label}: {start_date} (在第一笔交易{first_tx_date}之前 - 应该为0)")
                else:
                    print(f"{label}: {start_date} (在第一笔交易之后 - 应该有数据)")
            else:
                print(f"{label}: {start_date} (无交易记录)")

if __name__ == '__main__':
    debug_transaction_timeline()