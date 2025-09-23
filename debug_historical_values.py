#!/usr/bin/env python3

import sys
import os
sys.path.append('/Users/luzhang/Desktop/web1/canadian_family_investment')

from datetime import date, timedelta
from app import create_app
from app.services.portfolio_service import PortfolioService
from app.services.asset_valuation_service import AssetValuationService

def debug_historical_values():
    app = create_app()

    with app.app_context():
        portfolio_service = PortfolioService()
        asset_service = AssetValuationService()
        account_ids = [2]  # 账户2

        print("=== 账户2历史估值调试 ===")

        today = date.today()
        test_dates = [
            today - timedelta(days=29),  # 1M
            today - timedelta(days=89),  # 3M
            today - timedelta(days=179), # 6M
            date(today.year, 1, 1),      # YTD
            today - timedelta(days=364), # 1Y
            today                        # Today
        ]

        for test_date in test_dates:
            print(f"\n--- {test_date} 估值分析 ---")

            try:
                # 使用asset_service获取快照
                snapshot = asset_service.get_asset_snapshot(2, test_date)
                print(f"  总资产: {snapshot.total_assets}")
                print(f"  股票持仓价值: {snapshot.stocks_value}")
                print(f"  现金余额: {snapshot.cash_balance}")
                print(f"  总成本: {snapshot.total_cost}")
                print(f"  未实现收益: {snapshot.unrealized_gain}")

                # 使用portfolio_service获取详细持仓
                portfolio = portfolio_service.get_portfolio_summary([2],
                    portfolio_service.TimePeriod.CUSTOM, test_date, test_date)
                summary = portfolio.get('summary', {})
                holdings = portfolio.get('current_holdings', [])

                print(f"  Portfolio总资产: {summary.get('total_current_value', 0)}")
                print(f"  Portfolio总成本: {summary.get('total_cost', 0)}")
                print(f"  Portfolio未实现: {summary.get('total_unrealized_gain', 0)}")
                print(f"  持仓数量: {len(holdings)}")

                # 显示持仓详情
                for holding in holdings[:5]:  # 前5个持仓
                    symbol = holding.get('symbol')
                    shares = holding.get('current_shares')
                    current_price = holding.get('current_price')
                    current_value = holding.get('current_value')
                    cost = holding.get('total_cost')
                    print(f"    {symbol}: {shares}股 @ ${current_price} = ${current_value} (成本: ${cost})")

            except Exception as e:
                print(f"  错误: {e}")

        # 检查是否是价格数据问题
        print(f"\n--- 价格数据检查 ---")
        from app.models.transaction import Transaction

        # 获取账户2的所有交易记录
        transactions = Transaction.query.filter_by(account_id=2).order_by(Transaction.trade_date.asc()).all()
        print(f"账户2总交易数: {len(transactions)}")

        if transactions:
            first_trade = transactions[0]
            last_trade = transactions[-1]
            print(f"第一笔交易: {first_trade.trade_date} - {first_trade.stock} - {first_trade.type}")
            print(f"最后一笔交易: {last_trade.trade_date} - {last_trade.stock} - {last_trade.type}")

            # 检查1个月前是否有交易活动
            one_month_ago = today - timedelta(days=29)
            recent_trades = [t for t in transactions if t.trade_date >= one_month_ago]
            print(f"最近一个月交易数: {len(recent_trades)}")

if __name__ == '__main__':
    debug_historical_values()