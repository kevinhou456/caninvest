#!/usr/bin/env python3

import sys
import os
sys.path.append('/Users/luzhang/Desktop/web1/canadian_family_investment')

from datetime import date, timedelta
from app import create_app
from app.services.asset_valuation_service import AssetValuationService

def debug_historical_holdings():
    app = create_app()

    with app.app_context():
        asset_service = AssetValuationService()
        account_id = 2

        print("=== 账户2历史持仓详细调试 ===")

        today = date.today()
        test_dates = [
            today - timedelta(days=29),  # 1M
            today - timedelta(days=89),  # 3M
            today,                       # Today
        ]

        for test_date in test_dates:
            print(f"\n--- {test_date} 持仓详细分析 ---")

            try:
                # 获取持仓
                holdings = asset_service._get_holdings_at_date(account_id, test_date)
                print(f"  持仓数量: {len(holdings)}")

                for symbol, shares in holdings.items():
                    print(f"  {symbol}: {shares}股")

                    # 获取股票信息
                    stock_info = asset_service._get_stock_info(symbol)
                    currency = stock_info.get('currency', 'USD')
                    print(f"    货币: {currency}")

                    # 获取历史价格
                    if test_date < today:
                        price = asset_service._get_historical_stock_price(symbol, test_date, currency)
                        print(f"    历史价格({test_date}): ${price}")
                    else:
                        price = asset_service.stock_price_service.get_cached_stock_price(symbol, currency)
                        print(f"    当前价格: ${price}")

                    if price and price > 0:
                        market_value = float(shares) * float(price)
                        print(f"    市值: ${market_value}")
                    else:
                        print(f"    无法获取价格")

                # 计算总股票市值
                stock_market_value, stock_details = asset_service._calculate_stock_market_value(account_id, test_date)
                print(f"  总股票市值: ${stock_market_value}")

                # 获取现金余额
                cash_cad, cash_usd = asset_service._calculate_cash_balance(account_id, test_date)
                print(f"  现金CAD: ${cash_cad}")
                print(f"  现金USD: ${cash_usd}")

                # 计算总资产 (手动)
                usd_to_cad = 1.35  # 假设汇率
                total_cash_cad = cash_cad + cash_usd * 1.35
                total_assets = stock_market_value + total_cash_cad
                print(f"  手动计算总资产: ${total_assets}")

                # 使用asset_service获取快照
                snapshot = asset_service.get_asset_snapshot(account_id, test_date)
                print(f"  快照总资产: ${snapshot.total_assets}")

            except Exception as e:
                print(f"  错误: {e}")
                import traceback
                traceback.print_exc()

if __name__ == '__main__':
    debug_historical_holdings()