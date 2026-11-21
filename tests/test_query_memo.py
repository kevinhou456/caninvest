"""
请求级查询记忆（app/utils/query_memo.py）的正确性测试：
同一请求里发生写入（补价格、新增节假日、新增交易等）之后，后续读取必须看到新数据，
不能返回写入前记住的旧结果；不同请求之间不共享记忆。
"""
import io
import unittest
from contextlib import redirect_stdout
from datetime import date, datetime, timezone
from decimal import Decimal
from unittest.mock import patch

from flask import Flask

from app import db
from app.models.market_holiday import MarketHoliday, StockHolidayAttempt
from app.models.stock_price_history import StockPriceHistory
from app.models.stocks_cache import StocksCache
from app.models.stock_split import StockSplit
from app.models.transaction import Transaction
from app.services.stock_history_cache_service import StockHistoryCacheService
from app.utils import query_memo

# 2025-03-03 是周一，到 2025-03-07 周五，都不是节假日
MON, TUE, WED, THU, FRI = (date(2025, 3, d) for d in (3, 4, 5, 6, 7))


def _price_row(day, close):
    return StockPriceHistory(symbol='TEST', currency='USD', trade_date=day,
                             open_price=close, high_price=close, low_price=close,
                             close_price=close, volume=100, data_source='test')


class QueryMemoTest(unittest.TestCase):
    def setUp(self):
        self.app = Flask(__name__)
        self.app.config.update(SQLALCHEMY_DATABASE_URI='sqlite:///:memory:',
                               SQLALCHEMY_TRACK_MODIFICATIONS=False)
        db.init_app(self.app)
        self.context = self.app.app_context()
        self.context.push()
        for model in (StockPriceHistory, MarketHoliday, StockHolidayAttempt, StocksCache, Transaction, StockSplit):
            model.__table__.create(db.engine)
        # 有 IPO 日期，避免走网络查询
        db.session.add(StocksCache(symbol='TEST', currency='USD', name='Test', first_trade_date=date(2020, 1, 1)))
        for i, day in enumerate((MON, TUE, WED, THU)):
            db.session.add(_price_row(day, 10 + i))
        db.session.commit()
        self.service = StockHistoryCacheService()
        self.output = redirect_stdout(io.StringIO())
        self.output.__enter__()

    def tearDown(self):
        self.output.__exit__(None, None, None)
        db.session.remove()
        db.engine.dispose()
        self.context.pop()

    def _warm_full_history(self):
        for _ in range(StockHistoryCacheService.FULL_HISTORY_AFTER + 1):
            self.service._get_from_database('TEST', MON, THU, 'USD')
        self.assertIsNotNone(query_memo.peek('price_history', ('TEST', 'USD')), '应已整只股票读入记忆')

    def test_price_written_and_committed_mid_request_is_visible(self):
        self._warm_full_history()
        StockPriceHistory.bulk_upsert([{'symbol': 'TEST', 'currency': 'USD', 'trade_date': FRI,
                                        'close_price': 99}])
        dates = [r['date'] for r in self.service._get_from_database('TEST', MON, FRI, 'USD')]
        self.assertIn(FRI.isoformat(), dates)

    def test_price_flushed_without_commit_is_visible(self):
        self._warm_full_history()
        db.session.add(_price_row(FRI, 99))
        db.session.flush()
        closes = {r['date']: r['close'] for r in self.service._get_from_database('TEST', MON, FRI, 'USD')}
        self.assertEqual(closes.get(FRI.isoformat()), 99.0)

    def test_fetch_path_backfills_gap_and_returns_new_price(self):
        """模拟联网补价格：get_history 发现周五缺失 -> 调用外部接口(打桩) -> 写库 -> 返回含周五的数据。"""
        self._warm_full_history()
        ts = int(datetime(2025, 3, 7, 12, tzinfo=timezone.utc).timestamp())
        raw = {'timestamp': [ts], 'open': [50], 'high': [50], 'low': [50], 'close': [50], 'volume': [1]}
        with patch.object(self.service.stock_service, 'get_stock_history', return_value=(raw, {})) as fetch, \
                patch.object(self.service.stock_service, 'get_stock_splits', return_value=[]):
            history = self.service.get_history('TEST', MON, FRI, 'USD')
        self.assertTrue(fetch.called)
        closes = {r['date']: r['close'] for r in history}
        self.assertEqual(closes.get(FRI.isoformat()), 50.0)
        # 再读一次（记忆已重建）仍然能看到
        closes = {r['date']: r['close'] for r in self.service._get_from_database('TEST', MON, FRI, 'USD')}
        self.assertEqual(closes.get(FRI.isoformat()), 50.0)

    def test_last_trading_price_refreshes_after_new_price(self):
        from app.services.portfolio_service import PortfolioService
        ps = PortfolioService()
        with patch.object(ps.history_cache_service.stock_service, 'get_stock_history', return_value=({}, {})):
            self.assertEqual(ps._get_last_trading_price('TEST', 'USD', FRI), Decimal('13'))
            db.session.add(_price_row(FRI, 77))
            db.session.commit()
            self.assertEqual(ps._get_last_trading_price('TEST', 'USD', FRI), Decimal('77'))

    def test_holiday_added_mid_request_is_visible(self):
        self.assertFalse(MarketHoliday.is_holiday(FRI, 'US'))
        db.session.add(MarketHoliday(holiday_date=FRI, market='US'))
        db.session.commit()
        self.assertTrue(MarketHoliday.is_holiday(FRI, 'US'))
        self.assertFalse(MarketHoliday.is_holiday(FRI, 'CA'))

    def test_no_data_dates_refresh_after_attempt_recorded(self):
        self.assertEqual(self.service._get_known_no_data_dates('TEST', 'US', MON, FRI), set())
        StockHolidayAttempt.record_attempt('TEST', 'US', FRI, has_data=False)
        self.assertEqual(self.service._get_known_no_data_dates('TEST', 'US', MON, FRI), {FRI})

    def test_currency_lookup_refreshes_after_new_transaction(self):
        self.assertIsNone(Transaction.get_currency_by_stock_symbol('NEWSYM'))
        db.session.add(Transaction(trade_date=MON, type='BUY', stock='NEWSYM', quantity=1, price=1,
                                   currency='CAD', account_id=1))
        db.session.flush()
        self.assertEqual(Transaction.get_currency_by_stock_symbol('NEWSYM'), 'CAD')

    def test_stocks_cache_change_is_visible(self):
        self.assertIsNone(StocksCache.get_cached('NEW', 'CAD'))
        db.session.add(StocksCache(symbol='NEW', currency='CAD', name='New'))
        db.session.commit()
        self.assertEqual(StocksCache.get_cached('NEW', 'CAD').name, 'New')

    def test_memo_not_shared_between_requests(self):
        calls = []
        query_memo.memo('t', 'k', lambda: calls.append(1) or 1)
        query_memo.memo('t', 'k', lambda: calls.append(1) or 1)
        self.assertEqual(len(calls), 1)
        with self.app.app_context():  # 新请求
            query_memo.memo('t', 'k', lambda: calls.append(1) or 1)
        self.assertEqual(len(calls), 2)

    def test_returned_rows_are_copies(self):
        self._warm_full_history()
        rows = self.service._get_from_database('TEST', MON, THU, 'USD')
        rows[0]['close'] = -1
        again = self.service._get_from_database('TEST', MON, THU, 'USD')
        self.assertEqual(again[0]['close'], 10.0)


if __name__ == '__main__':
    unittest.main()
