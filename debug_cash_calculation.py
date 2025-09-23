#!/usr/bin/env python3

import sys
import os
sys.path.append('/Users/luzhang/Desktop/web1/canadian_family_investment')

from datetime import date
from app import create_app

def debug_cash_calculation():
    app = create_app()

    with app.app_context():
        from app.models.family import Family
        from app.models.account import Account
        from app.services.asset_valuation_service import AssetValuationService
        from app.models.cash import Cash

        family = Family.query.first()
        account_2 = Account.query.filter_by(family_id=family.id, id=2).first()
        print(f'账户2: {account_2.name}')

        asset_service = AssetValuationService()

        # 检查现金表中的数据
        cash_record = Cash.query.filter_by(account_id=2).first()
        if cash_record:
            print(f'Cash表中账户2的数据:')
            print(f'  CAD现金: ${cash_record.cad_balance}')
            print(f'  USD现金: ${cash_record.usd_balance}')
        else:
            print('Cash表中没有账户2的记录')

        # 检查AssetValuationService的现金计算
        cash_cad, cash_usd = asset_service._calculate_cash_balance(2, date.today())
        print(f'AssetValuationService计算的现金:')
        print(f'  CAD现金: ${cash_cad}')
        print(f'  USD现金: ${cash_usd}')

        # 检查get_asset_snapshot的结果
        snapshot = asset_service.get_asset_snapshot(2, date.today())
        print(f'get_asset_snapshot结果:')
        print(f'  总资产: ${snapshot.total_assets}')
        print(f'  股票市值: ${snapshot.stock_market_value}')
        print(f'  CAD现金: ${snapshot.cash_balance_cad}')
        print(f'  USD现金: ${snapshot.cash_balance_usd}')
        print(f'  总现金(CAD等价): ${snapshot.cash_balance_total_cad}')

if __name__ == '__main__':
    debug_cash_calculation()