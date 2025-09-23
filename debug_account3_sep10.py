#!/usr/bin/env python3

import sys
import os
sys.path.append('/Users/luzhang/Desktop/web1/canadian_family_investment')

from datetime import date, timedelta
from app import create_app

def debug_account3_sep10():
    app = create_app()

    with app.app_context():
        print("=== 调试账户3在9月10日的日浮动盈亏计算 ===")

        account_id = 3
        target_date = date(2025, 9, 10)
        previous_date = date(2025, 9, 9)

        from app.services.asset_valuation_service import AssetValuationService
        from app.services.portfolio_service import portfolio_service, TimePeriod
        from app.models.transaction import Transaction

        asset_service = AssetValuationService()

        print(f"账户ID: {account_id}")
        print(f"目标日期: {target_date}")
        print(f"前一日期: {previous_date}")

        # 1. 检查9月10日的交易
        print(f"\n--- 第1步：检查{target_date}的交易记录 ---")
        transactions_sep10 = Transaction.query.filter(
            Transaction.account_id == account_id,
            Transaction.trade_date == target_date
        ).all()

        print(f"9月10日共有{len(transactions_sep10)}笔交易:")
        for tx in transactions_sep10:
            if tx.type == 'SELL':
                print(f"  卖出: {tx.stock} {tx.quantity}股 @ ${tx.price} (手续费${tx.fee}) 净收入${tx.net_amount}")
            elif tx.type == 'BUY':
                print(f"  买入: {tx.stock} {tx.quantity}股 @ ${tx.price} (手续费${tx.fee}) 总成本${tx.net_amount}")
            else:
                print(f"  {tx.type}: {tx.stock or ''} ${tx.amount or 0}({tx.currency})")

        # 2. 对比9月9日和9月10日的资产快照
        print(f"\n--- 第2步：对比两天的资产快照 ---")

        # 9月9日
        snapshot_sep9 = asset_service.get_asset_snapshot(account_id, previous_date)
        print(f"9月9日资产快照:")
        print(f"  股票市值: ${snapshot_sep9.stock_market_value}")
        print(f"  现金余额: CAD=${snapshot_sep9.cash_balance_cad}, USD=${snapshot_sep9.cash_balance_usd}")
        print(f"  总资产: ${snapshot_sep9.total_assets}")

        # 9月10日
        snapshot_sep10 = asset_service.get_asset_snapshot(account_id, target_date)
        print(f"\n9月10日资产快照:")
        print(f"  股票市值: ${snapshot_sep10.stock_market_value}")
        print(f"  现金余额: CAD=${snapshot_sep10.cash_balance_cad}, USD=${snapshot_sep10.cash_balance_usd}")
        print(f"  总资产: ${snapshot_sep10.total_assets}")

        # 计算变化
        total_change = snapshot_sep10.total_assets - snapshot_sep9.total_assets
        stock_change = snapshot_sep10.stock_market_value - snapshot_sep9.stock_market_value
        cash_change = snapshot_sep10.cash_balance_total_cad - snapshot_sep9.cash_balance_total_cad

        print(f"\n资产变化:")
        print(f"  总资产变化: ${total_change}")
        print(f"  股票市值变化: ${stock_change}")
        print(f"  现金变化: ${cash_change}")

        # 3. 使用Portfolio Service计算浮动盈亏
        print(f"\n--- 第3步：Portfolio Service浮动盈亏计算 ---")
        try:
            portfolio_sep9 = portfolio_service.get_portfolio_summary([account_id], TimePeriod.CUSTOM, previous_date, previous_date)
            portfolio_sep10 = portfolio_service.get_portfolio_summary([account_id], TimePeriod.CUSTOM, target_date, target_date)

            unrealized_sep9 = portfolio_sep9.get('summary', {}).get('total_unrealized_gain', 0)
            unrealized_sep10 = portfolio_sep10.get('summary', {}).get('total_unrealized_gain', 0)

            print(f"9月9日浮动盈亏: ${unrealized_sep9}")
            print(f"9月10日浮动盈亏: ${unrealized_sep10}")
            print(f"日浮动盈亏变化: ${unrealized_sep10 - unrealized_sep9}")

        except Exception as e:
            print(f"Portfolio Service计算出错: {e}")

        # 4. 分析持仓变化
        print(f"\n--- 第4步：分析持仓变化 ---")

        # 9月9日持仓
        holdings_sep9 = asset_service._get_holdings_at_date(account_id, previous_date)
        print(f"9月9日持仓: {dict(holdings_sep9)}")

        # 9月10日持仓
        holdings_sep10 = asset_service._get_holdings_at_date(account_id, target_date)
        print(f"9月10日持仓: {dict(holdings_sep10)}")

        # 找出变化的股票
        all_symbols = set(holdings_sep9.keys()) | set(holdings_sep10.keys())
        print(f"\n持仓变化:")
        for symbol in all_symbols:
            shares_sep9 = holdings_sep9.get(symbol, 0)
            shares_sep10 = holdings_sep10.get(symbol, 0)
            change = shares_sep10 - shares_sep9
            if abs(change) > 0.01:
                print(f"  {symbol}: {shares_sep9} → {shares_sep10} (变化: {change:+}股)")

        # 5. 详细分析卖出的股票
        print(f"\n--- 第5步：分析卖出股票的影响 ---")
        for tx in transactions_sep10:
            if tx.type == 'SELL':
                symbol = tx.stock
                print(f"\n分析卖出股票: {symbol}")

                # 获取该股票的成本基础
                shares_before = holdings_sep9.get(symbol, 0)
                shares_after = holdings_sep10.get(symbol, 0)
                shares_sold = shares_before - shares_after

                print(f"  卖出前持股: {shares_before}")
                print(f"  卖出后持股: {shares_after}")
                print(f"  实际卖出: {shares_sold}")
                print(f"  交易记录卖出: {tx.quantity}")

                if abs(float(shares_sold) - float(tx.quantity)) > 0.01:
                    print(f"  ⚠️ 持仓变化与交易记录不匹配!")

                # 计算该股票的成本基础变化
                if shares_before > 0:
                    cost_before = asset_service._calculate_cost_basis(account_id, symbol, previous_date, shares_before)
                    print(f"  卖出前成本基础: ${cost_before}")

                if shares_after > 0:
                    cost_after = asset_service._calculate_cost_basis(account_id, symbol, target_date, shares_after)
                    print(f"  卖出后成本基础: ${cost_after}")
                    cost_change = cost_after - cost_before if shares_before > 0 else 0
                    print(f"  成本基础变化: ${cost_change}")

                # 获取股价
                stock_info = asset_service._get_stock_info(symbol)
                currency = stock_info.get('currency', 'USD')
                price_sep9 = asset_service.stock_price_service.get_cached_stock_price(symbol, currency)
                price_sep10 = asset_service.stock_price_service.get_cached_stock_price(symbol, currency)

                print(f"  9月9日股价: ${price_sep9}({currency})")
                print(f"  9月10日股价: ${price_sep10}({currency})")

                # 计算理论收入
                theoretical_proceeds = shares_sold * float(price_sep10 or tx.price)
                actual_proceeds = float(tx.net_amount)
                print(f"  理论收入: ${theoretical_proceeds}")
                print(f"  实际收入: ${actual_proceeds}")

if __name__ == '__main__':
    debug_account3_sep10()