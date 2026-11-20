"""
多日期批量计算与逐日计算的一致性测试：
  get_portfolio_summary_series(ids, 日期们)[d] == get_portfolio_summary(ids, CUSTOM, start(d), d)
  get_cash_balances(账户, 日期们)[d]            == get_cash_balance(账户, d)
逐日结果在各自独立的 app context 里计算（不受请求级记忆影响），批量结果在同一个 context 里计算。
"""
import io
import unittest
from contextlib import redirect_stdout
from datetime import date, timedelta
from decimal import Decimal
from unittest.mock import patch

from app import create_app, db
from app.models.account import Account, AccountType
from app.models.cash import Cash
from app.models.family import Family
from app.models.stock_price_history import StockPriceHistory
from app.models.stocks_cache import StocksCache
from app.models.transaction import Transaction

TODAY = date.today()
START = TODAY - timedelta(days=60)


def _weekdays(start, end):
    d = start
    while d <= end:
        if d.weekday() < 5:
            yield d
        d += timedelta(days=1)


class SeriesConsistencyTest(unittest.TestCase):
    def setUp(self):
        self.output = redirect_stdout(io.StringIO())
        self.output.__enter__()
        self.patches = [
            patch('app.services.stock_price_service.StockPriceService.get_stock_history',
                  return_value=({}, {'error': 'offline'})),
            patch('app.services.currency_service.CurrencyService.get_current_rate',
                  return_value=Decimal('1.35')),
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
        acct_type = AccountType(name='Regular')
        db.session.add(acct_type)
        db.session.flush()
        a1 = Account(name='A1', family_id=family.id, account_type_id=acct_type.id)
        a2 = Account(name='A2', family_id=family.id, account_type_id=acct_type.id)
        db.session.add_all([a1, a2])
        db.session.flush()
        self.account_ids = [a1.id, a2.id]

        for symbol, currency, base in (('AAA', 'USD', 100), ('BBB.TO', 'CAD', 30), ('CCC', 'USD', 7)):
            db.session.add(StocksCache(symbol=symbol, currency=currency, name=symbol,
                                       first_trade_date=date(2000, 1, 1),
                                       current_price=Decimal(str(base + 1.5))))
            for i, d in enumerate(_weekdays(START - timedelta(days=20), TODAY)):
                close = Decimal(str(base)) + Decimal(i % 7) / 4
                db.session.add(StockPriceHistory(symbol=symbol, currency=currency, trade_date=d,
                                                 open_price=close, high_price=close, low_price=close,
                                                 close_price=close, volume=1, data_source='test'))

        def tx(acct, days, typ, stock=None, qty=0, price=0, fee=0, amount=None, currency='USD'):
            db.session.add(Transaction(account_id=acct.id, trade_date=START + timedelta(days=days), type=typ,
                                       stock=stock, quantity=qty, price=price, fee=fee, amount=amount,
                                       currency=currency))
        tx(a1, 0, 'DEPOSIT', amount=10000)
        tx(a1, 1, 'BUY', 'AAA', 10, 100, 1)
        tx(a1, 1, 'SELL', 'AAA', 3, 101, 1)          # 同一天先买后卖
        tx(a1, 5, 'BUY', 'AAA', 5, 98.5, 0)
        tx(a1, 9, 'DIVIDEND', 'AAA', amount=12.34)
        tx(a1, 12, 'SELL', 'AAA', 12, 103, 2)        # 清仓
        tx(a1, 20, 'BUY', 'AAA', 4, 99, 0)           # 清仓后再买
        tx(a1, 3, 'BUY', 'CCC', 100, 7.1, 0)
        tx(a1, 25, 'SELL', 'CCC', 40, 7.3, 0)
        tx(a1, 26, 'TAX', 'AAA', amount=1.85)
        tx(a1, 30, 'WITHDRAWAL', amount=500)
        tx(a2, 2, 'DEPOSIT', amount=5000, currency='CAD')
        tx(a2, 4, 'BUY', 'BBB.TO', 50, 30.2, 4.95, currency='CAD')
        tx(a2, 40, 'SELL', 'BBB.TO', 20, 31, 4.95, currency='CAD')
        tx(a2, 41, 'INTEREST', amount=3.21, currency='CAD')
        tx(a2, 59, 'BUY', 'BBB.TO', 10, 30.8, 0, currency='CAD')
        tx(a1, 35, 'SELL', 'CCC', 70, 7.2, 0)           # 卖超：此时只持有 60 股
        tx(a1, 45, 'BUY', 'CCC', 30, 7.0, 0)            # 卖超后又买回
        db.session.add(Cash(account_id=a1.id, cad=Decimal('12.50'), usd=Decimal('321.09')))
        db.session.commit()

        # 覆盖：首笔交易之前、交易当天、周末、清仓日、今天
        self.dates = sorted({START - timedelta(days=3), START, START + timedelta(days=1),
                             START + timedelta(days=12), START + timedelta(days=13), START + timedelta(days=33),
                             TODAY - timedelta(days=1), TODAY} |
                            {START + timedelta(days=i) for i in range(0, 61, 4)})

    def tearDown(self):
        db.session.remove()
        db.drop_all()
        self.context.pop()
        for p in self.patches:
            p.stop()
        self.output.__exit__(None, None, None)

    def _per_date_summary(self, account_ids, d, start):
        """独立的参照实现（优化前的算法）：每个 (账户, 股票) 用 get_position_snapshot
        自己查询截止日前的交易、从头重放，再按原规则分到 持仓/已清仓。"""
        from app.services.portfolio_service import PortfolioService, TimePeriod
        with self.app.app_context():  # 独立请求，无记忆
            ps = PortfolioService()
            current, cleared = [], []
            for account_id in account_ids:
                symbols = db.session.query(Transaction.stock).filter(
                    Transaction.account_id == account_id,
                    Transaction.stock.isnot(None),
                    Transaction.stock != ''
                ).distinct().all()
                for (symbol,) in symbols:
                    position = ps.get_position_snapshot(symbol, account_id, d)
                    if position.current_shares > 0:
                        current.append(position.to_dict())
                    elif position.total_sold_shares > 0:
                        cleared.append(position.to_dict())
            reference = ps._assemble_portfolio_summary(TimePeriod.CUSTOM.value, start, d, current, cleared)
            # 单日期的 get_portfolio_summary 也必须与参照一致
            self.assertEqual(ps.get_portfolio_summary(account_ids, TimePeriod.CUSTOM, start, d), reference)
            return reference

    def test_portfolio_series_matches_per_date(self):
        from app.services.portfolio_service import PortfolioService
        for ids in (self.account_ids, [self.account_ids[0]], [self.account_ids[1]], list(reversed(self.account_ids))):
            for start_for in (None, lambda d: d):
                with self.app.app_context():
                    series = PortfolioService().get_portfolio_summary_series(ids, self.dates, start_date_for=start_for)
                for d in self.dates:
                    expected = self._per_date_summary(ids, d, start_for(d) if start_for else None)
                    self.assertEqual(series[d], expected, f'accounts={ids} date={d}')
        # 确认测试数据确实覆盖到持仓、清仓
        some = self._per_date_summary(self.account_ids, START + timedelta(days=13), None)
        self.assertTrue(some['current_holdings'] and some['cleared_holdings'])
        # 卖超记录只出现在卖超日期之后，且批量结果里各日期互不影响
        before = self._per_date_summary(self.account_ids, START + timedelta(days=33), None)
        after = self._per_date_summary(self.account_ids, START + timedelta(days=37), None)
        events = lambda summary: [e for h in summary['current_holdings'] + summary['cleared_holdings']
                                  for e in h['oversold_events']]
        self.assertEqual(events(before), [])
        self.assertEqual([round(e['shortfall'], 4) for e in events(after)], [10.0])

    def test_cash_series_matches_per_date(self):
        from app.services.asset_valuation_service import AssetValuationService
        for account_id in self.account_ids:
            with self.app.app_context():
                series = AssetValuationService().get_cash_balances(account_id, self.dates)
            for d in self.dates:
                with self.app.app_context():
                    expected = AssetValuationService().get_cash_balance(account_id, d)
                self.assertEqual(series[d], expected, f'account={account_id} date={d}')

    def test_returned_holdings_are_copies(self):
        from app.services.portfolio_service import PortfolioService, TimePeriod
        ps = PortfolioService()
        d = START + timedelta(days=13)
        first = ps.get_portfolio_summary(self.account_ids, TimePeriod.CUSTOM, None, d)
        first['current_holdings'][0]['current_value'] = -1
        first['current_holdings'].clear()
        again = ps.get_portfolio_summary(self.account_ids, TimePeriod.CUSTOM, None, d)
        self.assertEqual(again, self._per_date_summary(self.account_ids, d, None))


if __name__ == '__main__':
    unittest.main()
