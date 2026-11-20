"""
T5008 与核心 ACB 使用同一交易顺序：同一天先买后卖（即使卖出记录先录入）。
"""
import io
import unittest
from contextlib import redirect_stdout
from datetime import date
from decimal import Decimal
from unittest.mock import patch

from app import create_app, db
from app.models.account import Account, AccountType
from app.models.family import Family
from app.models.transaction import Transaction


class T5008SameDayOrderTest(unittest.TestCase):
    def setUp(self):
        self.output = redirect_stdout(io.StringIO())
        self.output.__enter__()
        # 测试不访问网络：外部价格、IPO 日期、汇率全部打桩
        self.patches = [
            patch('app.services.stock_price_service.StockPriceService.get_stock_history',
                  return_value=({}, {'error': 'offline'})),
            patch('app.services.stock_history_cache_service.StockHistoryCacheService._query_ipo_online',
                  return_value=None),
            patch('app.services.currency_service.CurrencyService.get_current_rate', return_value=Decimal('1.35')),
            patch('app.services.currency_service.CurrencyService.get_annual_average_rate', return_value=Decimal('1.35')),
        ]
        for p in self.patches:
            p.start()
        self.app = create_app('testing')
        self.context = self.app.app_context()
        self.context.push()
        db.create_all()
        family = Family(name='F')
        db.session.add(family)
        db.session.flush()
        acct_type = AccountType(name='Regular', tax_advantaged=False)
        db.session.add(acct_type)
        db.session.flush()
        self.account = Account(name='A', family_id=family.id, account_type_id=acct_type.id)
        db.session.add(self.account)
        db.session.flush()

        def tx(day, typ, qty, price):
            db.session.add(Transaction(account_id=self.account.id, trade_date=date(2025, 3, day), type=typ,
                                       stock='XYZ.TO', quantity=qty, price=price, fee=0, currency='CAD'))
            db.session.flush()
        tx(3, 'BUY', 10, 10)
        tx(5, 'SELL', 10, 12)   # 先录入卖出
        tx(5, 'BUY', 5, 11)     # 后录入买入（同一天）
        db.session.commit()

    def tearDown(self):
        db.session.remove()
        db.drop_all()
        self.context.pop()
        for p in self.patches:
            p.stop()
        self.output.__exit__(None, None, None)

    def test_t5008_uses_buy_first_same_as_core(self):
        from app.api.reports import _compute_t5008_for_accounts
        from app.services.portfolio_service import PortfolioService, TimePeriod
        records, _ = _compute_t5008_for_accounts([self.account.id], date(2025, 1, 1), date(2025, 12, 31),
                                                 {self.account.id: 'A'})
        self.assertEqual(len(records), 1)
        # 先买后卖：卖出前 15 股、总成本 155 → 卖 10 股的成本 = 103.33，收益 = 120 - 103.33
        self.assertAlmostEqual(records[0]['acb'], 103.33, places=2)
        self.assertAlmostEqual(records[0]['gain'], 16.67, places=2)
        core = PortfolioService().get_portfolio_summary([self.account.id], TimePeriod.CUSTOM,
                                                        end_date=date(2025, 12, 31))
        holding = core['current_holdings'][0]
        self.assertAlmostEqual(holding['realized_gain'], records[0]['gain'], places=2)


if __name__ == '__main__':
    unittest.main()
