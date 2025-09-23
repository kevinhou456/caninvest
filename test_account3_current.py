#!/usr/bin/env python3

import sys
import os
sys.path.append('/Users/luzhang/Desktop/web1/canadian_family_investment')

from datetime import date
from app import create_app

def test_account3_current():
    app = create_app()

    with app.app_context():
        print("=== 测试账户3当前现金输入后的9月10日浮动盈亏 ===")

        account_id = 3
        sep10 = date(2025, 9, 10)

        from app.services.asset_valuation_service import AssetValuationService
        from app.services.portfolio_service import portfolio_service, TimePeriod
        from app.models.cash import Cash

        asset_service = AssetValuationService()

        # 检查Cash表中的当前现金
        print(f"--- 检查Cash表中账户3的现金 ---")
        cash_record = Cash.get_account_cash(account_id)
        if cash_record:
            print(f"Cash表记录: CAD=${cash_record.cad}, USD=${cash_record.usd}")
        else:
            print("Cash表中没有账户3的记录")

        # 检查9月10日的资产快照
        print(f"\n--- 9月10日资产快照 ---")
        snapshot = asset_service.get_asset_snapshot(account_id, sep10)
        print(f"股票市值: ${snapshot.stock_market_value}")
        print(f"现金余额: CAD=${snapshot.cash_balance_cad}, USD=${snapshot.cash_balance_usd}")
        print(f"总资产: ${snapshot.total_assets}")

        # 检查9月10日的持仓
        print(f"\n--- 9月10日持仓详情 ---")
        holdings = asset_service._get_holdings_at_date(account_id, sep10)
        for symbol, shares in holdings.items():
            if shares > 0:
                stock_info = asset_service._get_stock_info(symbol)
                currency = stock_info.get('currency', 'USD')
                price = asset_service.stock_price_service.get_cached_stock_price(symbol, currency)
                market_value = float(shares) * float(price or 0)
                cost_basis = asset_service._calculate_cost_basis(account_id, symbol, sep10, shares)
                unrealized = market_value - float(cost_basis)
                print(f"  {symbol}: {shares}股 @ ${price}({currency}) = ${market_value:.2f}, 成本${cost_basis:.2f}, 浮动${unrealized:.2f}")

        # 使用Portfolio Service计算浮动盈亏
        print(f"\n--- Portfolio Service计算 ---")
        try:
            portfolio_data = portfolio_service.get_portfolio_summary([account_id], TimePeriod.CUSTOM, sep10, sep10)
            summary = portfolio_data.get('summary', {})
            total_unrealized = summary.get('total_unrealized_gain', 0)
            print(f"总浮动盈亏: ${total_unrealized}")

            # 检查个股详情
            current_holdings = portfolio_data.get('current_holdings', [])
            print(f"\nPortfolio Service个股详情:")
            for holding in current_holdings:
                if holding['account_id'] == account_id:
                    print(f"  {holding['symbol']}: 市值${holding['current_value']:.2f}, 成本${holding['total_cost']:.2f}, 浮动${holding['unrealized_gain']:.2f}")

        except Exception as e:
            print(f"Portfolio Service计算出错: {e}")
            import traceback
            traceback.print_exc()

        # 检查Asset Valuation Service的综合指标
        print(f"\n--- Asset Valuation Service综合指标 ---")
        try:
            metrics = asset_service.get_comprehensive_portfolio_metrics([account_id], sep10)
            unrealized_cad = metrics['unrealized_gain']['cad']
            print(f"Asset Service浮动盈亏: ${unrealized_cad}")
        except Exception as e:
            print(f"Asset Service计算出错: {e}")

if __name__ == '__main__':
    test_account3_current()