#!/usr/bin/env python3

import sys
import os
sys.path.append('/Users/luzhang/Desktop/web1/canadian_family_investment')

from datetime import date
from app import create_app

def debug_account2_holdings():
    app = create_app()

    with app.app_context():
        from app.models.family import Family
        from app.models.account import Account
        from app.services.asset_valuation_service import AssetValuationService
        from app.models.transaction import Transaction

        family = Family.query.first()
        asset_service = AssetValuationService()

        print("=== 账户2持仓股票详细分析 ===")

        # 1. 获取账户2的当前持仓
        holdings = asset_service._get_holdings_at_date(2, date.today())
        print(f"账户2当前持仓: {holdings}")

        total_unrealized_gain = 0
        total_market_value = 0

        # 2. 逐个股票分析
        for symbol, shares in holdings.items():
            if shares <= 0:
                continue

            print(f"\n--- 股票 {symbol} ---")
            print(f"持股数量: {shares}股")

            # 获取币种
            currency = Transaction.get_currency_by_stock_symbol(symbol)
            print(f"币种: {currency}")

            # 获取当前价格
            current_price = asset_service.stock_price_service.get_cached_stock_price(symbol, currency)
            if current_price:
                market_value = float(shares) * float(current_price)
                print(f"当前价格: ${current_price}")
                print(f"市值: ${market_value}")
                total_market_value += market_value

                # 计算成本基础
                cost_basis = asset_service._calculate_cost_basis(2, symbol, date.today(), shares)
                print(f"成本基础: ${cost_basis}")

                # 计算浮动盈亏
                unrealized_gain = market_value - float(cost_basis)
                print(f"浮动盈亏: ${unrealized_gain}")
                total_unrealized_gain += unrealized_gain

                # 检查是否只看USD股票
                if currency == 'USD':
                    print(f"*** USD股票浮动盈亏: ${unrealized_gain}")

        print(f"\n=== 汇总 ===")
        print(f"总市值: ${total_market_value}")
        print(f"总浮动盈亏(手动计算): ${total_unrealized_gain}")

        # 3. 对比系统计算
        stock_cad, stock_usd, realized_cad, realized_usd, unrealized_cad, unrealized_usd = asset_service._calculate_account_metrics_by_currency(2, date.today())
        print(f"\n系统计算结果:")
        print(f"USD未实现收益: ${unrealized_usd}")
        print(f"CAD未实现收益: ${unrealized_cad}")

        # 4. 检查是否有已清仓股票影响计算
        print(f"\n=== 检查已清仓股票 ===")
        all_symbols = Transaction.query.filter(
            Transaction.account_id == 2,
            Transaction.stock.isnot(None),
            Transaction.type.in_(['BUY', 'SELL'])
        ).with_entities(Transaction.stock).distinct().all()

        cleared_stocks = []
        for (symbol,) in all_symbols:
            if symbol not in holdings or holdings.get(symbol, 0) <= 0:
                cleared_stocks.append(symbol)

        if cleared_stocks:
            print(f"已清仓股票: {cleared_stocks}")
            print("这些股票不应该影响未实现收益计算")

if __name__ == '__main__':
    debug_account2_holdings()