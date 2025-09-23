#!/usr/bin/env python3

import sys
import os
sys.path.append('/Users/luzhang/Desktop/web1/canadian_family_investment')

from datetime import date
from app import create_app
from decimal import Decimal

def debug_unrealized_gain():
    app = create_app()

    with app.app_context():
        print("=== 调试账户7浮动盈亏计算 ===")

        account_id = 7
        today = date.today()

        from app.services.asset_valuation_service import AssetValuationService
        from app.services.portfolio_service import portfolio_service
        from app.models.transaction import Transaction

        asset_service = AssetValuationService()

        print(f"\n--- 第一步：使用Asset Valuation Service计算 ---")
        # 1. 使用Asset Valuation Service获取综合指标
        try:
            comprehensive_metrics = asset_service.get_comprehensive_portfolio_metrics([account_id], today)
            unrealized_from_asset_service = comprehensive_metrics['unrealized_gain']['cad']
            print(f"Asset Valuation Service计算的浮动盈亏: ${unrealized_from_asset_service}")
        except Exception as e:
            print(f"Asset Valuation Service计算出错: {e}")
            unrealized_from_asset_service = "ERROR"

        print(f"\n--- 第二步：使用Portfolio Service计算 ---")
        # 2. 使用Portfolio Service获取投资组合摘要
        try:
            portfolio_summary = Transaction.get_portfolio_summary(account_id)
            print(f"Portfolio Service投资组合摘要:")
            for symbol, data in portfolio_summary.items():
                if symbol and data['total_shares'] > 0:
                    print(f"  {symbol}: 持股{data['total_shares']}, 总成本${data['total_cost']}, 平均成本${data['average_cost']}")
        except Exception as e:
            print(f"Portfolio Service计算出错: {e}")

        print(f"\n--- 第三步：使用统一的get_portfolio_summary ---")
        # 3. 使用统一的get_portfolio_summary
        try:
            from app.services.portfolio_service import TimePeriod
            unified_portfolio = portfolio_service.get_portfolio_summary([account_id], TimePeriod.CUSTOM, today, today)
            print(f"统一Portfolio Service结果:")
            summary = unified_portfolio.get('summary', {})
            print(f"  总市值: ${summary.get('total_current_value', 0)}")
            print(f"  总成本: ${summary.get('total_cost', 0)}")
            print(f"  未实现收益: ${summary.get('total_unrealized_gain', 0)}")

            # 检查持仓详情
            current_holdings = unified_portfolio.get('current_holdings', [])
            print(f"\n  当前持仓详情:")
            total_unrealized_manual = 0
            for holding in current_holdings:
                if holding['account_id'] == account_id:
                    unrealized = holding['unrealized_gain']
                    total_unrealized_manual += unrealized
                    print(f"    {holding['symbol']}: 市值${holding['current_value']}, 成本${holding['total_cost']}, 浮动盈亏${unrealized}")

            print(f"  手动汇总的浮动盈亏: ${total_unrealized_manual}")

        except Exception as e:
            print(f"统一Portfolio Service计算出错: {e}")

        print(f"\n--- 第四步：直接计算验证 ---")
        # 4. 直接验证计算
        try:
            # 获取当前持仓
            holdings = asset_service._get_holdings_at_date(account_id, today)
            print(f"当前持仓: {holdings}")

            total_market_value = Decimal('0')
            total_cost_basis = Decimal('0')

            for symbol, shares in holdings.items():
                if shares <= 0:
                    continue

                # 获取当前价格
                stock_info = asset_service._get_stock_info(symbol)
                currency = stock_info.get('currency', 'USD')
                current_price = asset_service.stock_price_service.get_cached_stock_price(symbol, currency)

                if current_price:
                    market_value = Decimal(str(shares)) * Decimal(str(current_price))

                    # 转换为CAD
                    if currency == 'USD':
                        exchange_rate = asset_service.currency_service.get_current_rate('USD', 'CAD')
                        market_value_cad = market_value * Decimal(str(exchange_rate))
                    else:
                        market_value_cad = market_value

                    total_market_value += market_value_cad

                    # 计算成本基础
                    cost_basis = asset_service._calculate_cost_basis(account_id, symbol, today, Decimal(str(shares)))
                    total_cost_basis += cost_basis

                    unrealized_gain = market_value_cad - cost_basis

                    print(f"  {symbol}: {shares}股 x ${current_price}({currency}) = ${market_value_cad:.2f}(CAD), 成本${cost_basis:.2f}, 浮动${unrealized_gain:.2f}")

            total_unrealized_direct = total_market_value - total_cost_basis
            print(f"\n直接计算结果:")
            print(f"  总市值(CAD): ${total_market_value:.2f}")
            print(f"  总成本(CAD): ${total_cost_basis:.2f}")
            print(f"  浮动盈亏(CAD): ${total_unrealized_direct:.2f}")

        except Exception as e:
            print(f"直接计算出错: {e}")
            import traceback
            traceback.print_exc()

        print(f"\n--- 对比结果 ---")
        print(f"Asset Valuation Service: ${unrealized_from_asset_service}")
        if 'total_unrealized_manual' in locals():
            print(f"统一Portfolio Service: ${total_unrealized_manual}")
        if 'total_unrealized_direct' in locals():
            print(f"直接计算验证: ${total_unrealized_direct:.2f}")

if __name__ == '__main__':
    debug_unrealized_gain()