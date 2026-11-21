"""
拆股/合股：核心持仓、T5008、价格还原、拆股当天的今日涨跌、导入过滤。
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
from app.models.stock_price_history import StockPriceHistory
from app.models.transaction import Transaction


class StockSplitTest(unittest.TestCase):
    def setUp(self):
        self.output = redirect_stdout(io.StringIO())
        self.output.__enter__()
        self.patches = [
            patch('app.services.stock_price_service.StockPriceService.get_stock_history',
                  return_value=({}, {'error': 'offline'})),
            patch('app.services.stock_price_service.StockPriceService.get_stock_splits', return_value=None),
            patch('app.services.stock_history_cache_service.StockHistoryCacheService._query_ipo_online',
                  return_value=None),
            patch('app.services.currency_service.CurrencyService.get_current_rate', return_value=Decimal('1.35')),
            patch('app.services.currency_service.CurrencyService.get_annual_average_rate', return_value=Decimal('1.35')),
            patch('app.services.currency_service.CurrencyService.get_rates_for_dates',
                  side_effect=lambda dates, *a, **k: {d: Decimal('1.35') for d in dates}),
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

    def tearDown(self):
        db.session.remove()
        db.drop_all()
        self.context.pop()
        for p in self.patches:
            p.stop()
        self.output.__exit__(None, None, None)

    def tx(self, day, typ, qty, price, symbol='XYZ', currency='USD'):
        db.session.add(Transaction(account_id=self.account.id, trade_date=day, type=typ, stock=symbol,
                                   quantity=qty, price=price, fee=0, currency=currency))
        db.session.flush()

    def price(self, day, close, symbol='XYZ', currency='USD'):
        db.session.add(StockPriceHistory(symbol=symbol, currency=currency, trade_date=day, close_price=close,
                                         open_price=close, high_price=close, low_price=close))
        db.session.flush()

    def summary(self, day):
        from app.services.portfolio_service import PortfolioService, TimePeriod
        return PortfolioService().get_portfolio_summary([self.account.id], TimePeriod.CUSTOM, end_date=day)

    def holding(self, day, symbol='XYZ'):
        s = self.summary(day)
        return [h for h in s['current_holdings'] + s['cleared_holdings'] if h['symbol'] == symbol][0]

    def test_forward_split_core_and_t5008(self):
        from app.services.split_service import record_split
        from app.api.reports import _compute_t5008_for_accounts
        self.tx(date(2026, 5, 4), 'BUY', 2, 1000)
        self.tx(date(2026, 6, 1), 'SELL', 8, 130)     # 拆股当天，先录入卖出
        self.tx(date(2026, 6, 1), 'BUY', 1, 125)      # 同日买入（排在卖出前）
        self.tx(date(2026, 6, 10), 'SELL', 4, 140)
        record_split('XYZ', 'USD', date(2026, 6, 1), 8)
        record_split('OTHER', 'USD', date(2026, 5, 20), 10)   # 其他股票的拆股不影响 XYZ
        db.session.commit()

        before = self.holding(date(2026, 5, 31))
        self.assertAlmostEqual(before['current_shares'], 2)
        self.assertAlmostEqual(before['average_cost'], 1000)

        # 6/1：拆成 16 股 → 买 1 股（17 股，成本 2125）→ 卖 8 股
        h = self.holding(date(2026, 6, 1))
        self.assertAlmostEqual(h['current_shares'], 9)
        self.assertAlmostEqual(h['total_cost'], 2125 * 9 / 17, places=6)
        self.assertAlmostEqual(h['realized_gain'], 8 * 130 - 2125 * 8 / 17, places=6)
        self.assertAlmostEqual(h['total_bought_shares'], 17)   # 2×8 + 1
        self.assertAlmostEqual(h['total_sold_shares'], 8)
        self.assertEqual(h['oversold_events'], [])

        end = self.holding(date(2026, 6, 30))
        self.assertAlmostEqual(end['current_shares'], 5)
        expected_gain = (8 * 130 - 2125 * 8 / 17) + (4 * 140 - 2125 * 4 / 17)
        self.assertAlmostEqual(end['realized_gain'], expected_gain, places=6)
        self.assertEqual(end['oversold_events'], [])

        records, _ = _compute_t5008_for_accounts([self.account.id], date(2026, 1, 1), date(2026, 12, 31),
                                                 {self.account.id: 'A'})
        self.assertEqual([r['quantity'] for r in records], [8, 4])
        self.assertAlmostEqual(sum(r['gain'] for r in records), expected_gain * 1.35, places=1)

    def test_reverse_split(self):
        from app.services.split_service import record_split
        self.tx(date(2026, 3, 2), 'BUY', 100, 2)
        self.tx(date(2026, 4, 10), 'SELL', 10, 25)
        record_split('XYZ', 'USD', date(2026, 4, 1), Decimal('0.1'))
        db.session.commit()
        h = self.holding(date(2026, 4, 30))
        self.assertAlmostEqual(h['current_shares'], 0)
        self.assertAlmostEqual(h['realized_gain'], 250 - 200, places=6)
        self.assertEqual(h['oversold_events'], [])

    def test_unknown_split_still_reported_as_oversold(self):
        # 没登记拆股时，拆股后的卖出照常被标记为卖超
        self.tx(date(2026, 5, 4), 'BUY', 2, 1000)
        self.tx(date(2026, 6, 10), 'SELL', 16, 130)
        db.session.commit()
        self.assertEqual(len(self.holding(date(2026, 6, 30))['oversold_events']), 1)

    def test_price_restored_to_actual_and_daily_change_on_split_day(self):
        from app.services.split_service import record_split
        from app.services.stock_history_cache_service import StockHistoryCacheService
        record_split('XYZ', 'USD', date(2026, 6, 1), 8)
        db.session.commit()
        service = StockHistoryCacheService()
        ts = lambda d: int(__import__('datetime').datetime(d.year, d.month, d.day).timestamp())
        raw = {
            'timestamp': [ts(date(2026, 5, 29)), ts(date(2026, 6, 1)), ts(date(2026, 6, 2))],
            'open': [125.0, 126.0, 127.0], 'high': [125.0, 126.0, 127.0], 'low': [125.0, 126.0, 127.0],
            'close': [125.0, None, 127.0], 'volume': [1, 1, 1],
        }
        rows = service._process_raw_data('XYZ', raw, 'USD')
        # 拆股前一天还原成实际价 1000；没有收盘价的那天不保存
        self.assertEqual([(r['trade_date'], r['close_price']) for r in rows],
                         [(date(2026, 5, 29), 1000.0), (date(2026, 6, 2), 127.0)])

        self.tx(date(2026, 5, 4), 'BUY', 2, 1000)
        self.price(date(2026, 5, 29), 1000)
        self.price(date(2026, 6, 1), 126)
        db.session.commit()
        h = self.holding(date(2026, 6, 1))
        self.assertAlmostEqual(h['current_shares'], 16)
        self.assertAlmostEqual(h['current_value'], 16 * 126)
        self.assertAlmostEqual(h['previous_close'], 125)          # 1000 ÷ 8
        self.assertAlmostEqual(h['daily_change_value'], 16 * (126 - 125))
        self.assertAlmostEqual(h['daily_change_percent'], 0.8)

        before = self.holding(date(2026, 5, 29))
        self.assertAlmostEqual(before['current_value'], 2 * 1000)

    def test_split_day_without_close_uses_converted_previous_close(self):
        # 拆股当天 Yahoo 没有收盘价：用前一交易日收盘价换算到拆股后的口径，不能按拆股前价格 × 拆股后股数
        from app.services.split_service import record_split
        record_split('XYZ', 'USD', date(2026, 6, 1), 8)
        self.tx(date(2026, 5, 4), 'BUY', 2, 1000)
        self.price(date(2026, 5, 29), 1000)
        db.session.commit()
        h = self.holding(date(2026, 6, 1))
        self.assertAlmostEqual(h['current_shares'], 16)
        self.assertAlmostEqual(h['current_price'], 125)
        self.assertAlmostEqual(h['current_value'], 2000)

    def test_record_split_and_import_filter(self):
        from app.services.split_service import record_split, get_splits, parse_yahoo_splits
        from app.api.imports import _is_split_activity, parse_transaction_type
        self.assertTrue(record_split('xyz', 'usd', date(2026, 6, 1), 8))
        self.assertFalse(record_split('XYZ', 'USD', date(2026, 6, 1), 8))
        self.assertFalse(record_split('XYZ', 'USD', date(2026, 7, 1), 1))
        db.session.commit()
        self.assertEqual(get_splits('XYZ', 'USD'), ((date(2026, 6, 1), Decimal('8')),))
        self.assertEqual(get_splits('XYZ', 'CAD'), ())
        self.assertEqual(parse_yahoo_splits([(date(2026, 1, 2), 0.0), (date(2026, 1, 3), 0.5),
                                             (date(2025, 12, 30), 0.993), (date(2025, 12, 31), 1.021)]),
                         [(date(2026, 1, 3), Decimal('0.50000000'))])
        self.assertTrue(_is_split_activity('StockSplit'))
        self.assertTrue(_is_split_activity('Stock split', ''))
        self.assertFalse(_is_split_activity('Dividend', 'Cash dividend distribution'))
        self.assertIsNone(parse_transaction_type('STOCK SPLIT'))
        self.assertEqual(parse_transaction_type('BUY'), 'BUY')


if __name__ == '__main__':
    unittest.main()
