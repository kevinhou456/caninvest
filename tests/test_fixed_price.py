"""
固定价格证券（投资储蓄账户 RBF2010.TO）：当前价与历史价恒为 $10，不访问外部接口；
利息按“利息收入 + 按 $10 买入份额”两条记录录入后，卖出全部份额不卖超、收益为 0。
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


def _no_network(*args, **kwargs):
    raise AssertionError('固定价格证券不应访问外部接口')


class FixedPriceTest(unittest.TestCase):
    def setUp(self):
        self.output = redirect_stdout(io.StringIO())
        self.output.__enter__()
        self.patches = [
            patch('yfinance.Ticker', side_effect=_no_network),
            patch('yfinance.download', side_effect=_no_network),
            patch('app.services.stock_price_service.StockPriceService.get_stock_history', side_effect=_no_network),
        ]
        for p in self.patches:
            p.start()
        self.app = create_app('testing')
        self.context = self.app.app_context()
        self.context.push()
        db.create_all()

    def tearDown(self):
        db.session.remove()
        db.drop_all()
        self.context.pop()
        for p in self.patches:
            p.stop()
        self.output.__exit__(None, None, None)

    def test_current_and_history_price_are_fixed(self):
        from app.services.stock_price_service import StockPriceService
        from app.services.stock_history_cache_service import StockHistoryCacheService
        service = StockPriceService()
        self.assertEqual(service.get_cached_stock_price('RBF2010.TO', 'CAD'), Decimal('10'))
        self.assertEqual(service.get_stock_price('RBF2010.TO')['price'], 10.0)
        self.assertIsNone(service.get_stock_price('RBF2010.TO', 'USD'))
        self.assertTrue(service.update_stock_price('RBF2010.TO', 'CAD', force_refresh=True))

        rows = StockHistoryCacheService().get_history('RBF2010.TO', date(2026, 4, 1), date(2026, 4, 12), 'CAD')
        # 4/1~4/12 共 8 个工作日
        self.assertEqual(len(rows), 8)
        self.assertTrue(all(r['close'] == 10.0 for r in rows))
        self.assertTrue(all(date.fromisoformat(r['date']).weekday() < 5 for r in rows))

    def test_reinvested_interest_split_has_no_oversell_and_zero_gain(self):
        from app.services.portfolio_service import PortfolioService, TimePeriod
        family = Family(name='F')
        db.session.add(family)
        db.session.flush()
        acct_type = AccountType(name='Regular', tax_advantaged=False)
        db.session.add(acct_type)
        db.session.flush()
        account = Account(name='A', family_id=family.id, account_type_id=acct_type.id)
        db.session.add(account)
        db.session.flush()

        def tx(day, typ, qty, price, amount):
            db.session.add(Transaction(account_id=account.id, trade_date=day, type=typ, stock='RBF2010.TO',
                                       quantity=qty, price=price, amount=amount, fee=0, currency='CAD'))
        tx(date(2026, 4, 1), 'BUY', 1000, 10, 10000)
        # 利息 12.5 份：拆成利息收入 $125 + 按 $10 买入 12.5 份
        tx(date(2026, 4, 30), 'INTEREST', 1, 125, 125)
        tx(date(2026, 4, 30), 'BUY', 12.5, 10, 125)
        tx(date(2026, 5, 15), 'SELL', 1012.5, 10, 10125)
        db.session.commit()

        mid = PortfolioService().get_portfolio_summary([account.id], TimePeriod.CUSTOM, end_date=date(2026, 5, 1))
        holding = mid['current_holdings'][0]
        self.assertAlmostEqual(holding['current_shares'], 1012.5)
        self.assertAlmostEqual(holding['current_value'], 10125.0)

        end = PortfolioService().get_portfolio_summary([account.id], TimePeriod.CUSTOM, end_date=date(2026, 6, 1))
        cleared = end['cleared_holdings'][0]
        self.assertEqual(cleared['oversold_events'], [])
        self.assertAlmostEqual(cleared['realized_gain'], 0.0)
        self.assertAlmostEqual(cleared['interest'] if 'interest' in cleared else cleared['total_interest'], 125.0)


if __name__ == '__main__':
    unittest.main()
