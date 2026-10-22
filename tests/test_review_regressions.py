"""Run with: arch -arm64 venv/bin/python -m unittest discover -s tests -p 'test_*.py'.

Uses only an isolated SQLite memory database; never starts the production app.
"""
import io
import json
import unittest
from contextlib import ExitStack, redirect_stdout
from datetime import date, timedelta
from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import patch

import pandas as pd
from flask import Flask
from app import db
from app.api import imports, reports
from app.models.transaction import Transaction
from app.models.cash import Cash
from app.models.performance_daily_cache import PerformanceDailyCache
from app.models.account import AccountMember
from app.models.report_analysis_cache import ReportAnalysisCache
from app.services.portfolio_service import PortfolioService
from app.services.asset_valuation_service import AssetValuationService


class ReviewRegressions(unittest.TestCase):
    def setUp(self):
        self.app = Flask(__name__)
        self.app.config.update(SQLALCHEMY_DATABASE_URI='sqlite:///:memory:',
                               SQLALCHEMY_TRACK_MODIFICATIONS=False)
        db.init_app(self.app)
        self.context = self.app.app_context()
        self.context.push()
        for model in (Transaction, Cash, AccountMember, ReportAnalysisCache, PerformanceDailyCache):
            model.__table__.create(db.engine)
        self.output = redirect_stdout(io.StringIO())
        self.output.__enter__()

    def tearDown(self):
        db.session.remove()
        db.engine.dispose()
        self.context.pop()
        self.output.__exit__(None, None, None)

    def add_transaction(self, **overrides):
        fields = dict(account_id=1, trade_date=date(2025, 9, 1), type='INTEREST',
                      stock=None, quantity=0, price=0, amount=10, currency='CAD', fee=0)
        fields.update(overrides)
        transaction = Transaction(**fields)
        db.session.add(transaction)
        db.session.commit()
        return transaction

    def import_rows(self, rows, overwrite=False, **options):
        frame = pd.DataFrame(rows)
        mapping = dict(date='date', type='type', amount='amount', currency='currency')
        with self.app.test_request_context(json=dict(session_id='test', account_id=1,
                column_mappings=mapping, overwrite_range=overwrite, **options)):
            with ExitStack() as stack:
                stack.enter_context(patch.object(imports, 'Account'))
                stack.enter_context(patch.object(imports, '_load_csv_dataframe', return_value=(frame, {})))
                stack.enter_context(patch.object(imports.os.path, 'exists', return_value=True))
                stack.enter_context(patch.object(imports.os, 'remove'))
                stack.enter_context(patch.object(imports, 'save_format_mapping'))
                stack.enter_context(patch.object(imports, '_', side_effect=lambda text: text))
                result = imports.import_csv_with_mapping()
                response, status = result if isinstance(result, tuple) else (result, 200)
                return response.get_json(), status

    def csv_row(self, **overrides):
        row = dict(date='2025-09-01', type='Interest', amount=2, currency='CAD')
        row.update(overrides)
        return row

    def test_partial_import_reports_invalid_rows(self):
        result, status = self.import_rows([self.csv_row(), self.csv_row(type='Unknown')])
        self.assertEqual(status, 200)
        self.assertEqual(result['created_count'], 1)
        self.assertEqual(result['failed_count'], 1)
        self.assertIn('Row 3', result['errors'][0])
        self.assertEqual(Transaction.query.count(), 1)

    def test_overwrite_parse_error_preserves_all_old_rows(self):
        old = self.add_transaction()
        old_id = old.id
        result, status = self.import_rows([self.csv_row(), self.csv_row(type='Unknown')], True)
        self.assertEqual(status, 400)
        self.assertFalse(result['success'])
        self.assertEqual(result['deleted_count'], 0)
        self.assertEqual(Transaction.query.one().id, old_id)
        self.assertEqual(Transaction.query.one().amount, 10)

    def test_overwrite_processing_failure_rolls_back_delete_and_inserts(self):
        self.add_transaction()
        with patch.object(Transaction, 'is_duplicate', side_effect=[False, ValueError('simulated row failure')]):
            result, status = self.import_rows([self.csv_row(), self.csv_row(amount=3)], True)
        self.assertEqual(status, 400)
        self.assertEqual(result['created_count'], 0)
        self.assertEqual(Transaction.query.count(), 1)
        self.assertEqual(Transaction.query.one().amount, 10)

    def test_commit_failure_restores_overwritten_rows(self):
        self.add_transaction()
        with patch.object(db.session, 'commit', side_effect=RuntimeError('simulated commit failure')):
            result, status = self.import_rows([self.csv_row()], True)
        self.assertEqual(status, 500)
        self.assertFalse(result['success'])
        self.assertEqual(Transaction.query.one().amount, 10)

    def test_successful_full_month_overwrite_preserves_other_months(self):
        self.add_transaction(trade_date=date(2025, 9, 20))
        self.add_transaction(trade_date=date(2025, 8, 31))
        result, status = self.import_rows([self.csv_row()], True, extend_to_full_month=True)
        self.assertEqual(status, 200)
        self.assertEqual(result['deleted_count'], 1)
        self.assertEqual(Transaction.query.count(), 2)
        self.assertIsNotNone(Transaction.query.filter_by(trade_date=date(2025, 8, 31)).first())

    def test_different_interest_amounts_import_and_reimport_is_idempotent(self):
        rows = [self.csv_row(amount=10), self.csv_row(amount=50)]
        result, _ = self.import_rows(rows)
        self.assertEqual(result['created_count'], 2)
        result, _ = self.import_rows(rows)
        self.assertEqual(result['created_count'], 0)
        self.assertEqual(result['skipped_count'], 2)
        self.assertEqual(Transaction.query.count(), 2)

    def test_dividend_duplicate_detection_includes_amount(self):
        self.add_transaction(type='DIVIDEND', stock='ABC')
        fields = dict(account_id=1, trade_date=date(2025, 9, 1), type='DIVIDEND',
                      stock='ABC', quantity=0, price=0, currency='CAD', fee=0)
        self.assertFalse(Transaction.is_duplicate(**fields, amount=50))
        self.assertTrue(Transaction.is_duplicate(**fields, amount=10))

    def test_existing_trade_without_amount_is_still_duplicate(self):
        self.add_transaction(type='BUY', stock='ABC', quantity=2, price=10, amount=None)
        self.assertTrue(Transaction.is_duplicate(account_id=1, trade_date=date(2025, 9, 1),
            type='BUY', stock='ABC', quantity=2, price=10, currency='CAD', fee=0, amount=20))

    def test_cache_invalidation_all_positions_and_formats(self):
        groups = ([3], [3, 7], [1, 3, 7], [1, 3], [1, 13, 30])
        for compact in (False, True):
            for group in groups:
                text = json.dumps(group, separators=(',', ':')) if compact else json.dumps(group)
                db.session.add(ReportAnalysisCache(cache_type='annual', cache_key=text,
                    family_id=1, account_ids_json=text, params_json='{}', data_json='{}'))
            db.session.commit()
            reports.invalidate_report_cache_for_account(3)
            self.assertEqual([json.loads(r.account_ids_json) for r in ReportAnalysisCache.query.all()], [[1, 13, 30]])
            ReportAnalysisCache.query.delete()
            db.session.commit()

    def test_exact_header_mapping_beats_substring(self):
        mapping = imports.smart_header_mapping(['transaction_date', 'transaction_type', 'symbol', 'quantity', 'price'])
        self.assertEqual(mapping['date'], 'transaction_date')
        self.assertEqual(mapping['type'], 'transaction_type')
        frame = pd.DataFrame([dict(transaction_date='2025-09-01', transaction_type='Buy', symbol='ABC', quantity=2, price=10)])
        rows, errors = imports.process_csv_with_mapping(frame, mapping)
        self.assertEqual(errors, [])
        self.assertEqual(rows[0]['type'], 'BUY')

    def test_fee_historical_cash_and_currency_separation(self):
        today = date.today()
        db.session.add(Cash(account_id=1, cad=80, usd=40))
        db.session.commit()
        self.add_transaction(trade_date=today, type='FEE', amount=20)
        self.add_transaction(trade_date=today, type='FEE', amount=10, currency='USD')
        service = AssetValuationService.__new__(AssetValuationService)
        service.currency_service = SimpleNamespace(get_current_rate=lambda *args: Decimal('1.4'))
        previous = service.get_cash_balance(1, today - timedelta(days=1))
        self.assertEqual(previous, dict(cad=100, usd=50, total_cad=Decimal('170')))
        self.assertEqual(service.get_cash_balance(1, today)['cad'], 80)
        self.assertEqual(service.get_cash_balance(1, today)['usd'], 40)

    def test_cash_forward_and_reverse_are_inverses(self):
        service = AssetValuationService.__new__(AssetValuationService)
        for currency in ('CAD', 'USD'):
            for kind in ('BUY', 'SELL', 'DEPOSIT', 'WITHDRAWAL', 'FEE', 'DIVIDEND', 'INTEREST'):
                with self.subTest(currency=currency, kind=kind):
                    tx = SimpleNamespace(type=kind, currency=currency, amount=20, quantity=3, price=10, fee=1)
                    balances = service._apply_transaction_impact(Decimal('80'), Decimal('40'), tx)
                    self.assertEqual(service._reverse_transaction_impact(*balances, tx), (80, 40))

    def test_missing_current_cash_keeps_negative_history(self):
        self.add_transaction(trade_date=date.today(), amount=20)
        service = AssetValuationService.__new__(AssetValuationService)
        self.assertEqual(service._calculate_cash_balance(1, date.today() - timedelta(days=1)), (-20, 0))
        self.assertEqual(service._calculate_cash_balance(1, date.today()), (0, 0))

    def test_monthly_and_quarterly_assets_with_ownership(self):
        service = PortfolioService.__new__(PortfolioService)
        holdings = [dict(account_id=1, currency='USD', current_value=1000, unrealized_gain=0, realized_gain=0),
                    dict(account_id=2, currency='CAD', current_value=500, unrealized_gain=0, realized_gain=0)]
        def portfolio(ids, *args, **kwargs):
            return dict(current_holdings=[h for h in holdings if h['account_id'] in ids],
                        cleared_holdings=[], summary={'total_unrealized_gain': 0})
        asset = SimpleNamespace(get_income_totals=lambda *a, **kw: dict(net_income=0), get_withholding_tax_totals=lambda *a, **kw: dict(cad=0, usd=0, total_cad=0), get_cash_balance=lambda account_id, *a: dict(cad=-200, usd=-100) if account_id == 1 else dict(cad=50, usd=0))
        db.session.add(AccountMember(account_id=1, member_id=1, ownership_percentage=50))
        db.session.add(AccountMember(account_id=2, member_id=1, ownership_percentage=25))
        db.session.commit()
        with patch.object(service, 'get_portfolio_summary', side_effect=portfolio), patch('app.services.portfolio_service.AssetValuationService', return_value=asset), patch('app.services.portfolio_service.currency_service.get_current_rate', return_value=1.4):
            for ids, member_id, cad, usd in [([1], None, -200, 900), ([1, 2], None, 350, 900), ([1, 2], 1, 37.5, 450)]:
                with self.subTest(ids=ids, member_id=member_id):
                    quarterly = service.get_quarterly_analysis(ids, years=[2025], member_id=member_id)
                    monthly = service.get_monthly_analysis(ids, months=1, member_id=member_id)
                    expected = cad + usd * 1.4
                    for result, key in ((quarterly, 'quarterly_data'), (monthly, 'monthly_data')):
                        current = result['current_totals']
                        self.assertAlmostEqual(current['total_assets'], expected)
                        self.assertAlmostEqual(current['total_assets_cad'], cad)
                        self.assertAlmostEqual(current['total_assets_usd'], usd)
                        for row in result[key]:
                            self.assertAlmostEqual(row['total_assets'], expected)
                            self.assertAlmostEqual(row['currency_breakdown']['total_assets_cad'], cad)
                            self.assertAlmostEqual(row['currency_breakdown']['total_assets_usd'], usd)

    def test_usd_only_and_empty_portfolios(self):
        service = PortfolioService.__new__(PortfolioService)
        asset = SimpleNamespace(get_income_totals=lambda *a, **kw: dict(net_income=0), get_withholding_tax_totals=lambda *a, **kw: dict(cad=0, usd=0, total_cad=0), get_cash_balance=lambda *a: dict(cad=0, usd=0))
        for holdings, expected in [([], 0), ([dict(account_id=1, currency='USD',
                current_value=1000, unrealized_gain=0, realized_gain=0)], 1400)]:
            portfolio = dict(current_holdings=holdings, cleared_holdings=[], summary={'total_unrealized_gain': 0})
            with patch.object(service, 'get_portfolio_summary', return_value=portfolio), patch('app.services.portfolio_service.AssetValuationService', return_value=asset), patch('app.services.portfolio_service.currency_service.get_current_rate', return_value=1.4):
                quarterly = service.get_quarterly_analysis([1], years=[2025])
                monthly = service.get_monthly_analysis([1], months=1)
                self.assertEqual(quarterly['quarterly_data'][0]['total_assets'], expected)
                self.assertEqual(monthly['monthly_data'][0]['total_assets'], expected)
                self.assertEqual(quarterly['current_totals']['total_assets'], expected)
                self.assertEqual(monthly['current_totals']['total_assets'], expected)

    def test_tax_cash_flow_import_and_refund(self):
        from test_csv_activity_types import ActivityCsvTests
        frame = ActivityCsvTests().activity_frame()
        frame.loc[0, 'activity_type'] = 'Tax'
        frame.loc[0, 'net_cash_amount'] = -15
        frame.loc[1, 'activity_type'] = 'Tax'
        frame.loc[1, 'net_cash_amount'] = 5
        rows, errors = imports.process_csv_with_mapping(frame, imports.smart_header_mapping(frame.columns.tolist()))
        self.assertEqual(errors, [])
        self.assertEqual([(r['type'], r['amount']) for r in rows[:2]], [('TAX', 15), ('TAX', -5)])

    def test_tax_parser_aliases_and_signed_conventions(self):
        for label in ('TAX', 'Withholding Tax', 'Dividend Tax', '预扣税'):
            self.assertEqual(imports.parse_transaction_type(label), 'TAX')
        for flow, amount, expected in ((False, 15, 15), (False, -5, -5), (True, -15, 15), (True, 5, -5)):
            frame = pd.DataFrame([self.csv_row(type='Tax', amount=amount)])
            rows, errors = imports.process_csv_with_mapping(frame, imports.smart_header_mapping(frame.columns.tolist()), tax_cash_flow=flow)
            self.assertEqual(errors, [])
            self.assertEqual(rows[0]['amount'], expected)
            self.assertIsNone(rows[0]['stock'])

    def test_tax_invalid_amount_is_reported(self):
        for amount in ('', 'garbage', 'nan', 'inf'):
            frame = pd.DataFrame([self.csv_row(type='Tax', amount=amount)])
            rows, errors = imports.process_csv_with_mapping(frame, imports.smart_header_mapping(frame.columns.tolist()))
            self.assertEqual(rows, [])
            self.assertEqual(len(errors), 1)

    def test_tax_route_roundtrip_and_duplicates(self):
        result, status = self.import_rows([self.csv_row(type='Tax', amount=15), self.csv_row(type='Tax', amount=-5)])
        self.assertEqual(status, 200)
        self.assertEqual(result['created_count'], 2)
        self.assertEqual([float(t.amount) for t in Transaction.query.order_by(Transaction.id)], [15, -5])
        result, _ = self.import_rows([self.csv_row(type='Tax', amount=15), self.csv_row(type='Tax', amount=-5)])
        self.assertEqual(result['skipped_count'], 2)

    def test_net_dividend_is_restored_once_and_ambiguous_match_rejected(self):
        def row(kind, amount):
            return dict(trade_date=date.today(), stock='ABC', currency='USD', type=kind, amount=amount)
        rows, errors = imports._restore_gross_dividends([row('DIVIDEND', 85), row('TAX', 15)])
        self.assertEqual(errors, [])
        self.assertEqual(rows[0]['amount'], 100)
        _, errors = imports._restore_gross_dividends([row('DIVIDEND', 40), row('DIVIDEND', 45), row('TAX', 15)])
        self.assertEqual(len(errors), 1)
        _, errors = imports._restore_gross_dividends([row('TAX', 15)])
        self.assertEqual(len(errors), 1)

    def tax_asset_service(self):
        service = AssetValuationService.__new__(AssetValuationService)
        service.currency_service = SimpleNamespace(get_current_rate=lambda *a: Decimal('1.4'))
        return service

    def test_tax_cash_reversal_and_income_excludes_external_flows(self):
        today = date.today()
        self.add_transaction(trade_date=today, type='DIVIDEND', amount=100, currency='USD')
        self.add_transaction(trade_date=today, type='TAX', amount=15, currency='USD')
        self.add_transaction(trade_date=today, type='TAX', amount=-5, currency='USD')
        db.session.add(Cash(account_id=1, cad=0, usd=90)); db.session.commit()
        asset = self.tax_asset_service()
        self.assertEqual(asset.get_cash_balance(1, today)['usd'], 90)
        self.assertEqual(asset.get_cash_balance(1, today - timedelta(days=1))['usd'], 0)
        self.assertEqual(asset.get_withholding_tax_totals([1], today)['usd'], 10)
        self.assertEqual(asset.get_withholding_tax_totals([1], today, ownership_map={1: Decimal('.5')})['usd'], 5)
        service = PortfolioService.__new__(PortfolioService)
        with patch.object(service, 'get_portfolio_summary', return_value={'summary': {'total_current_value': 0}}):
            values, flows = service._fetch_time_series_from_db([1], [today], lambda _: Decimal(1), asset)
        self.assertEqual(flows[0][1], 0)
        self.assertEqual(values[0][1], 126)

    def test_tax_does_not_change_acb_or_capital_gains(self):
        from app.services.portfolio_service import PositionSnapshot
        position = PositionSnapshot('ABC', 1, date.today(), current_shares=Decimal(10), total_cost=Decimal(100), realized_gain=Decimal(20))
        service = PortfolioService.__new__(PortfolioService)
        service._process_transaction_acb(position, SimpleNamespace(type='TAX', amount=15))
        service._process_transaction_acb(position, SimpleNamespace(type='TAX', amount=-5))
        self.assertEqual((position.current_shares, position.total_cost, position.realized_gain), (10, 100, 20))
        self.assertEqual(position.to_dict()['withholding_tax'], 10)

    def test_tax_comprehensive_and_period_reports(self):
        today = date.today()
        for kind, amount in [('DIVIDEND', 100), ('TAX', 15), ('TAX', -5)]:
            self.add_transaction(trade_date=today, type=kind, amount=amount, currency='USD')
        db.session.add(Cash(account_id=1, cad=0, usd=90)); db.session.commit()
        asset = self.tax_asset_service()
        with patch.object(asset, '_calculate_account_metrics_by_currency', return_value=(Decimal(0),) * 6), patch.object(asset, '_calculate_daily_change', return_value=dict(cad=0, cad_only=0, usd_only=0)):
            metrics = asset.get_comprehensive_portfolio_metrics([1], today)
        self.assertEqual(metrics['dividends']['usd_only'], 100)
        self.assertEqual(metrics['withholding_tax']['usd_only'], 10)
        self.assertEqual(metrics['net_dividends']['usd_only'], 90)
        self.assertEqual(metrics['total_return']['cad'], 126)
        portfolio = dict(current_holdings=[], cleared_holdings=[], summary={'total_unrealized_gain': 0})
        service = PortfolioService.__new__(PortfolioService)
        with patch.object(service, 'get_portfolio_summary', return_value=portfolio), patch('app.services.portfolio_service.AssetValuationService', return_value=asset), patch('app.services.portfolio_service.currency_service.get_current_rate', return_value=1.4):
            monthly = service.get_monthly_analysis([1], months=1)['monthly_data'][0]
            quarterly = service.get_quarterly_analysis([1, 2], years=[today.year])['quarterly_data'][0]
        for prefix, data in [('monthly', monthly), ('quarterly', quarterly)]:
            self.assertEqual(data[prefix + '_withholding_tax'], 14)
            self.assertEqual(data[prefix + '_net_dividends'], 126)
            self.assertEqual(data['currency_breakdown']['withholding_tax_usd'], 10)
            self.assertEqual(data[prefix + '_return_percent'], 100)

    def test_daily_returns_include_net_dividend_and_refund(self):
        from app.services.daily_stats_service import DailyStatsService
        today = date.today()
        self.add_transaction(trade_date=today, type='DIVIDEND', amount=100, currency='USD')
        self.add_transaction(trade_date=today, type='TAX', amount=15, currency='USD')
        self.add_transaction(trade_date=today, type='TAX', amount=-5, currency='USD')
        db.session.add(Cash(account_id=1, cad=0, usd=90)); db.session.commit()
        daily = DailyStatsService.__new__(DailyStatsService)
        daily.asset_service = self.tax_asset_service()
        daily.portfolio_service = SimpleNamespace(get_portfolio_summary=lambda *a, **kw: dict(current_holdings=[], cleared_holdings=[]))
        with patch('app.services.daily_stats_service.currency_service.get_current_rate', return_value=1.4):
            snapshot = daily._get_combined_asset_snapshot([1], today)
        self.assertEqual(snapshot['total_assets'], 126)
        self.assertEqual(snapshot['total_return'], 126)

    def test_standalone_tax_refund_retains_negative_total(self):
        self.add_transaction(trade_date=date.today(), type='TAX', amount=-5, currency='USD')
        income = self.tax_asset_service().get_income_totals([1], date.today())
        self.assertEqual(income['withholding_tax'], -7)
        self.assertEqual(income['net_income'], 7)

    def test_wealthsimple_tax_invalid_amount_rejected(self):
        from test_csv_activity_types import ActivityCsvTests
        frame = ActivityCsvTests().activity_frame().iloc[:1].copy()
        frame['activity_type'] = 'Tax'
        frame['net_cash_amount'] = 'invalid'
        rows, errors = imports.process_csv_with_mapping(frame, {})
        self.assertEqual(rows, [])
        self.assertEqual(len(errors), 1)

    def test_tax_import_invalidates_historical_cache_atomically(self):
        db.session.add(PerformanceDailyCache(account_id=1, cache_date=date(2020, 1, 1), total_assets=100))
        db.session.add(ReportAnalysisCache(cache_type='annual', cache_key='tax', family_id=1,
                       account_ids_json='[1, 3]', params_json='{}', data_json='{}'))
        db.session.commit()
        result, status = self.import_rows([self.csv_row(type='Tax', amount=15)])
        self.assertEqual(status, 200)
        self.assertEqual(PerformanceDailyCache.query.count(), 0)
        self.assertEqual(ReportAnalysisCache.query.count(), 0)

    def test_security_transfer_only_import_does_not_overwrite_existing_data(self):
        self.add_transaction()
        result, status = self.import_rows([self.csv_row(type='SecurityTransfer', amount=-63579.32)],
                                         True, extend_to_full_month=True)
        self.assertEqual(status, 200)
        self.assertTrue(result['success'])
        self.assertEqual(result['ignored_count'], 1)
        self.assertEqual(result['created_count'], 0)
        self.assertEqual(result['deleted_count'], 0)
        self.assertEqual(Transaction.query.one().amount, 10)

    def test_ignored_transfer_does_not_extend_overwrite_range(self):
        self.add_transaction(trade_date=date(2025, 10, 13), amount=99)
        result, status = self.import_rows([self.csv_row(), self.csv_row(date='2025-10-13', type='SecurityTransfer', amount=-63579.32)], True)
        self.assertEqual(status, 200)
        self.assertEqual(result['ignored_count'], 1)
        self.assertEqual(result['created_count'], 1)
        self.assertEqual(result['failed_count'], 0)
        self.assertEqual(result['deleted_count'], 0)
        self.assertEqual(Transaction.query.filter_by(trade_date=date(2025, 10, 13)).one().amount, 99)

    def test_ignored_types_only_do_not_overwrite_existing_data(self):
        self.add_transaction()
        rows = [self.csv_row(type='ReturnOfCapital', amount=-.0904),
                self.csv_row(type='SecurityTransfer', amount=-63579.32)]
        result, status = self.import_rows(rows, True, extend_to_full_month=True)
        self.assertEqual(status, 200)
        self.assertEqual(result['ignored_count'], 2)
        self.assertEqual(result['created_count'], 0)
        self.assertEqual(result['deleted_count'], 0)
        self.assertEqual(result['failed_count'], 0)
        self.assertEqual(Transaction.query.one().amount, 10)

    def test_return_of_capital_does_not_block_mixed_import_or_extend_range(self):
        self.add_transaction(trade_date=date(2026, 1, 6), amount=99)
        rows = [self.csv_row(), self.csv_row(date='2026-01-06', type='ReturnOfCapital', amount=-.0904)]
        result, status = self.import_rows(rows, True)
        self.assertEqual(status, 200)
        self.assertEqual(result['ignored_count'], 1)
        self.assertEqual(result['created_count'], 1)
        self.assertEqual(result['failed_count'], 0)
        self.assertEqual(result['deleted_count'], 0)
        self.assertEqual(Transaction.query.filter_by(trade_date=date(2026, 1, 6)).one().amount, 99)


if __name__ == '__main__':
    unittest.main()
