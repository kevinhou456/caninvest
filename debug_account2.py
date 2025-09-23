#!/usr/bin/env python3

import sys
import os
sys.path.append('/Users/luzhang/Desktop/web1/canadian_family_investment')

from datetime import date
from app import create_app

def debug_account2():
    app = create_app()

    with app.app_context():
        from app.models.family import Family
        from app.models.account import Account
        from app.services.asset_valuation_service import AssetValuationService
        from app.models.transaction import Transaction

        family = Family.query.first()
        account_2 = Account.query.filter_by(family_id=family.id, id=2).first()
        print(f'账户2: {account_2.name}')

        asset_service = AssetValuationService()

        # 单独计算账户2
        metrics = asset_service.get_comprehensive_portfolio_metrics([2])
        print(f'账户2总资产: CAD=${metrics["total_assets"]["cad"]}, USD=${metrics["total_assets"]["usd_only"]}')
        print(f'账户2总回报: CAD=${metrics["total_return"]["cad"]}, USD=${metrics["total_return"]["usd_only"]}')
        print(f'账户2已实现收益: CAD=${metrics["realized_gain"]["cad"]}, USD=${metrics["realized_gain"]["usd_only"]}')
        print(f'账户2未实现收益: CAD=${metrics["unrealized_gain"]["cad"]}, USD=${metrics["unrealized_gain"]["usd_only"]}')

        # 手动计算验证
        stock_cad, stock_usd, realized_cad, realized_usd, unrealized_cad, unrealized_usd = asset_service._calculate_account_metrics_by_currency(2, date.today())
        cash_cad, cash_usd = asset_service._calculate_cash_balance(2, date.today())

        print(f'\n手动计算验证:')
        print(f'股票市值: CAD=${stock_cad}, USD=${stock_usd}')
        print(f'现金余额: CAD=${cash_cad}, USD=${cash_usd}')
        print(f'已实现收益: CAD=${realized_cad}, USD=${realized_usd}')
        print(f'未实现收益: CAD=${unrealized_cad}, USD=${unrealized_usd}')

        total_assets_cad = stock_cad + cash_cad
        total_assets_usd = stock_usd + cash_usd

        print(f'总资产验证: CAD=${total_assets_cad}, USD=${total_assets_usd}')

        # 检查交易记录
        print(f'\n=== 账户2的买卖交易 ===')
        transactions = Transaction.query.filter(
            Transaction.account_id == 2,
            Transaction.type.in_(['BUY', 'SELL'])
        ).order_by(Transaction.trade_date.desc()).limit(10).all()

        for tx in transactions:
            print(f'  {tx.trade_date}: {tx.type} {tx.stock} {tx.quantity}股 @ ${tx.price} ({tx.currency})')

if __name__ == '__main__':
    debug_account2()