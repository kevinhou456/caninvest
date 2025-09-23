#!/usr/bin/env python3

import sys
import os
sys.path.append('/Users/luzhang/Desktop/web1/canadian_family_investment')

from datetime import date
from app import create_app

def test_currency_breakdown():
    app = create_app()

    with app.app_context():
        print("=== 测试币种分离计算详情 ===")

        from app.models.family import Family
        from app.models.account import Account
        from app.services.asset_valuation_service import AssetValuationService
        from app.models.transaction import Transaction

        # 获取家庭和账户
        family = Family.query.first()
        accounts = Account.query.filter_by(family_id=family.id).all()
        account_ids = [acc.id for acc in accounts]

        asset_service = AssetValuationService()

        print(f"检查的账户: {[(acc.id, acc.name) for acc in accounts]}")

        # 检查分红和利息交易
        print(f"\n=== 检查分红和利息交易 ===")
        div_transactions = Transaction.query.filter(
            Transaction.account_id.in_(account_ids),
            Transaction.type.in_(['DIVIDEND', 'INTEREST'])
        ).all()

        div_cad_total = 0
        div_usd_total = 0
        for tx in div_transactions:
            currency = tx.currency or 'USD'
            amount = float(tx.amount or 0)
            print(f"  账户{tx.account_id}: {tx.type} ${amount} ({currency}) - {tx.trade_date}")
            if currency == 'CAD':
                div_cad_total += amount
            else:
                div_usd_total += amount

        print(f"分红利息合计: CAD=${div_cad_total}, USD=${div_usd_total}")

        # 检查几个账户的详细计算
        print(f"\n=== 检查单个账户的币种分离 ===")
        for account_id in account_ids[:3]:
            print(f"\n账户{account_id}:")

            # 检查该账户的美元股票
            usd_stocks = Transaction.query.filter(
                Transaction.account_id == account_id,
                Transaction.type.in_(['BUY', 'SELL']),
                Transaction.currency == 'USD'
            ).all()

            if usd_stocks:
                print(f"  美元股票交易: {len(usd_stocks)}笔")
                for tx in usd_stocks[:3]:
                    print(f"    {tx.type} {tx.stock} {tx.quantity}股 @ ${tx.price} ({tx.trade_date})")

            # 使用新方法计算
            try:
                stock_cad, stock_usd, realized_cad, realized_usd, unrealized_cad, unrealized_usd = asset_service._calculate_account_metrics_by_currency(account_id, date.today())
                print(f"  股票市值: CAD=${stock_cad}, USD=${stock_usd}")
                print(f"  已实现收益: CAD=${realized_cad}, USD=${realized_usd}")
                print(f"  浮动盈亏: CAD=${unrealized_cad}, USD=${unrealized_usd}")
            except Exception as e:
                print(f"  计算出错: {e}")

        # 检查整体的dividend/interest计算
        print(f"\n=== 检查AssetValuationService的dividend/interest计算 ===")
        try:
            # 调用dividend/interest计算方法
            for account_id in account_ids[:3]:
                div_stats = asset_service._calculate_dividend_interest_by_currency(account_id, date.today())
                print(f"账户{account_id}: 分红CAD=${div_stats['dividends_cad']}, USD=${div_stats['dividends_usd']}")
                print(f"           利息CAD=${div_stats['interest_cad']}, USD=${div_stats['interest_usd']}")
        except Exception as e:
            print(f"dividend/interest计算出错: {e}")

if __name__ == '__main__':
    test_currency_breakdown()