#!/usr/bin/env python3

import sys
import os
sys.path.append('/Users/luzhang/Desktop/web1/canadian_family_investment')

from datetime import date
from app import create_app

def debug_exchange_rate():
    app = create_app()

    with app.app_context():
        from app.services.asset_valuation_service import AssetValuationService
        from app.services.currency_service import CurrencyService

        asset_service = AssetValuationService()
        currency_service = CurrencyService()

        # 检查汇率
        exchange_rate = currency_service.get_current_rate('USD', 'CAD')
        print(f'当前汇率 USD->CAD: {exchange_rate}')

        # 手动计算
        stock_cad = 3233.12
        stock_usd = 40433.38
        manual_total = stock_cad + stock_usd * float(exchange_rate)
        print(f'手动计算: CAD {stock_cad} + USD {stock_usd} × {exchange_rate} = ${manual_total}')

        # 检查get_comprehensive_portfolio_metrics内部计算
        metrics = asset_service.get_comprehensive_portfolio_metrics([2])
        print(f'系统计算结果: ${metrics["total_assets"]["cad"]}')
        print(f'系统exchange_rate: {metrics["exchange_rate"]}')

        # 检查系统内部的汇率计算
        print(f'\n=== 系统内部数据 ===')
        print(f'stock_value_cad: {metrics["total_assets"]["stock_value_cad"]}')
        print(f'stock_value_usd: {metrics["total_assets"]["stock_value_usd"]}')

        # 按系统汇率重新计算
        system_total = metrics["total_assets"]["stock_value_cad"] + metrics["total_assets"]["stock_value_usd"] * metrics["exchange_rate"]
        print(f'按系统汇率计算: {metrics["total_assets"]["stock_value_cad"]} + {metrics["total_assets"]["stock_value_usd"]} × {metrics["exchange_rate"]} = ${system_total}')

if __name__ == '__main__':
    debug_exchange_rate()