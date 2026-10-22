"""Regression coverage for broker exports with two activity type columns."""
import io
import unittest
from contextlib import redirect_stdout

import pandas as pd

from app.api.imports import process_csv_with_mapping, smart_header_mapping


class ActivityCsvTests(unittest.TestCase):
    def activity_frame(self):
        return pd.read_csv(io.StringIO(
            'transaction_date,activity_type,activity_sub_type,symbol,quantity,unit_price,currency,net_cash_amount,commission,description\n'
            '2026-09-01,Trade,Buy,ABC,10,20,CAD,-201,1,Purchase\n'
            '2026-09-02,Trade,Sell,ABC,-2,30,CAD,59,1,Sale\n'
            '2026-09-03,Dividend,,ABC,0,0,CAD,5,0,Dividend\n'
            '2026-09-04,Interest,, ,0,0,CAD,2,0,Interest\n'
        ))

    def parse(self, frame, mapping):
        with redirect_stdout(io.StringIO()):
            return process_csv_with_mapping(frame, mapping)

    def test_full_broker_export_preserves_all_activity_and_amounts(self):
        frame = self.activity_frame()
        for type_column in ('activity_type', 'activity_sub_type'):
            with self.subTest(type_column=type_column):
                mapping = dict(smart_header_mapping(frame.columns.tolist()), type=type_column)
                rows, errors = self.parse(frame, mapping)
                self.assertEqual(errors, [])
                self.assertEqual([r['type'] for r in rows], ['BUY', 'SELL', 'DIVIDEND', 'INTEREST'])
                self.assertEqual([r['amount'] for r in rows], [201, 59, 5, 2])
                self.assertEqual([r['quantity'] for r in rows[:2]], [10, 2])
                self.assertEqual([r['fee'] for r in rows[:2]], [1, 1])

    def test_partial_export_uses_both_columns_with_either_mapping(self):
        frame = self.activity_frame().drop(columns=['commission', 'description'])
        for type_column in ('activity_type', 'activity_sub_type', None):
            with self.subTest(type_column=type_column):
                mapping = dict(smart_header_mapping(frame.columns.tolist()), type=type_column)
                rows, errors = self.parse(frame, mapping)
                self.assertEqual(errors, [])
                self.assertEqual([r['type'] for r in rows], ['BUY', 'SELL', 'DIVIDEND', 'INTEREST'])
                self.assertEqual(mapping['type'], type_column)

    def test_case_and_space_variations_in_headers_and_values(self):
        for full_export in (True, False):
            frame = self.activity_frame()
            if not full_export:
                frame = frame.drop(columns=['commission'])
            frame['activity_type'] = frame['activity_type'].str.lower().map(lambda value: f' {value} ')
            frame = frame.rename(columns=lambda name: ' ' + name.replace('_', ' ').title() + ' ')
            rows, errors = self.parse(frame, smart_header_mapping(frame.columns.tolist()))
            self.assertEqual(errors, [])
            self.assertEqual([r['type'] for r in rows], ['BUY', 'SELL', 'DIVIDEND', 'INTEREST'])

    def test_unknown_trade_subtype_reports_error_without_losing_other_rows(self):
        for full_export in (True, False):
            frame = self.activity_frame()
            if not full_export:
                frame = frame.drop(columns=['commission'])
            frame.loc[0, 'activity_sub_type'] = None
            # Non-trades must use the primary type even with a misleading subtype.
            frame.loc[2, 'activity_sub_type'] = 'Buy'
            rows, errors = self.parse(frame, smart_header_mapping(frame.columns.tolist()))
            self.assertEqual([r['type'] for r in rows], ['SELL', 'DIVIDEND', 'INTEREST'])
            self.assertEqual(len(errors), 1)
            self.assertIn('Row 2:', errors[0])

    def test_single_type_column_still_works(self):
        frame = self.activity_frame().drop(columns=['activity_sub_type', 'commission'])
        frame['activity_type'] = ['Buy', 'Sell', 'Dividend', 'Interest']
        mapping = dict(smart_header_mapping(frame.columns.tolist()), type='activity_type')
        rows, errors = self.parse(frame, mapping)
        self.assertEqual(errors, [])
        self.assertEqual([r['type'] for r in rows], ['BUY', 'SELL', 'DIVIDEND', 'INTEREST'])

    def test_preview_suggests_primary_even_when_subtype_comes_first(self):
        frame = self.activity_frame().drop(columns=['commission'])
        headers = ['activity_sub_type'] + [col for col in frame.columns if col != 'activity_sub_type']
        self.assertEqual(smart_header_mapping(headers)['type'], 'activity_type')

    def test_effective_date_broker_export_with_money_movements(self):
        # Same export layout as the supplied sample; account identifiers omitted.
        frame = pd.DataFrame([
            dict(effective_date='2025-11-05', effective_time='21:00:00', activity_type='Tax', activity_sub_type='NRT', description='Non-resident tax', symbol='', currency='USD', quantity=-.3, unit_price='', commission='', net_cash_amount=-.3),
            dict(effective_date='2025-11-05', effective_time='21:00:00', activity_type='Dividend', activity_sub_type='-', description='Cash dividend', symbol='TLT', currency='USD', quantity=1.98, unit_price='', commission='', net_cash_amount=1.98),
            dict(effective_date='2025-11-06', effective_time='12:04:15', activity_type='Trade', activity_sub_type='BUY', description='Bought shares', symbol='TLT', currency='USD', quantity=.0187, unit_price=89.69, commission=0, net_cash_amount=-1.68),
            dict(effective_date='2025-11-12', effective_time='08:27:31', activity_type='MoneyMovement', activity_sub_type='TRANSFER', description='Money transfer out of the account', symbol='', currency='CAD', quantity=-1907, unit_price='', commission='', net_cash_amount=-1907),
            dict(effective_date='2025-12-23', effective_time='16:20:43', activity_type='MoneyMovement', activity_sub_type='TRANSFER', description='Money transfer out of the account', symbol='', currency='USD', quantity=-92.52, unit_price='', commission='', net_cash_amount=-92.52),
        ])
        mapping = smart_header_mapping(frame.columns.tolist())
        self.assertEqual(mapping['date'], 'effective_date')
        rows, errors = self.parse(frame, mapping)
        self.assertEqual(errors, [])
        self.assertEqual([row['type'] for row in rows], ['TAX', 'DIVIDEND', 'BUY', 'WITHDRAWAL', 'WITHDRAWAL'])
        self.assertEqual([row['amount'] for row in rows], [.3, 1.98, 1.68, 1907, 92.52])
        self.assertEqual(rows[0]['trade_date'].isoformat(), '2025-11-05')

    def test_money_movement_works_without_full_broker_headers(self):
        frame = self.activity_frame().iloc[:1].copy().drop(columns=['commission'])
        frame['activity_type'] = 'MoneyMovement'
        frame['activity_sub_type'] = 'TRANSFER'
        for amount, expected in [(-1907, 'WITHDRAWAL'), (1907, 'DEPOSIT')]:
            frame['net_cash_amount'] = amount
            rows, errors = self.parse(frame, smart_header_mapping(frame.columns.tolist()))
            self.assertEqual(errors, [])
            self.assertEqual(rows[0]['type'], expected)
            self.assertEqual(rows[0]['amount'], abs(amount))

    def test_money_movement_unknown_or_conflicting_direction_is_not_guessed(self):
        frame = self.activity_frame().iloc[:1].copy()
        frame['activity_type'] = 'MoneyMovement'
        frame['description'] = 'Money movement'
        for subtype, amount in [('TRANSFER', 0), ('DEPOSIT', -50)]:
            frame['activity_sub_type'] = subtype
            frame['net_cash_amount'] = amount
            rows, errors = self.parse(frame, smart_header_mapping(frame.columns.tolist()))
            self.assertEqual(rows, [])
            self.assertEqual(len(errors), 1)

    def test_security_transfer_is_ignored_in_broker_and_generic_parsers(self):
        from app.api.imports import process_csv_with_mapping
        for full_export in (True, False):
            frame = self.activity_frame()
            frame.loc[0, 'activity_type'] = 'SecurityTransfer'
            frame.loc[0, 'activity_sub_type'] = '-'
            frame.loc[0, 'quantity'] = -323
            frame.loc[0, 'unit_price'] = float('nan')
            frame.loc[0, 'net_cash_amount'] = -63579.32
            frame.loc[0, 'description'] = 'Tax-free transfer of 323 shares out of the account'
            if not full_export:
                frame = frame.drop(columns=['commission'])
            ignored = []
            with redirect_stdout(io.StringIO()):
                rows, errors = process_csv_with_mapping(frame, smart_header_mapping(frame.columns.tolist()), ignored_rows=ignored)
            self.assertEqual(errors, [])
            self.assertEqual([row['type'] for row in rows], ['SELL', 'DIVIDEND', 'INTEREST'])
            self.assertEqual(ignored, [{'row_num': 2, 'type': 'SecurityTransfer'}])
            self.assertEqual(rows[0]['row_num'], 3)

    def test_direct_broker_parser_ignores_security_transfer(self):
        from app.api.imports import process_wealthsimple_activity_csv
        frame = self.activity_frame().iloc[:1].copy()
        frame['activity_type'] = 'SecurityTransfer'
        ignored = []
        with redirect_stdout(io.StringIO()):
            rows, errors = process_wealthsimple_activity_csv(frame, ignored_rows=ignored)
        self.assertEqual((rows, errors), ([], []))
        self.assertEqual(len(ignored), 1)

    def test_return_of_capital_is_ignored_with_normal_transactions(self):
        from app.api.imports import process_csv_with_mapping
        for full_export in (True, False):
            for label in ('ReturnOfCapital', 'return_of_capital', ' Return Of Capital '):
                with self.subTest(full_export=full_export, label=label):
                    frame = self.activity_frame()
                    frame.loc[0, 'activity_type'] = label
                    frame.loc[0, 'net_cash_amount'] = -.0904
                    frame.loc[0, 'description'] = 'VFV: Return of Capital (Adjusted Cost Base Entry Only)'
                    if not full_export:
                        frame = frame.drop(columns=['commission'])
                    ignored = []
                    with redirect_stdout(io.StringIO()):
                        rows, errors = process_csv_with_mapping(frame, smart_header_mapping(frame.columns.tolist()), ignored_rows=ignored)
                    self.assertEqual(errors, [])
                    self.assertEqual([r['type'] for r in rows], ['SELL', 'DIVIDEND', 'INTEREST'])
                    self.assertEqual(ignored, [{'row_num': 2, 'type': 'ReturnOfCapital'}])

    def test_direct_broker_parser_ignores_return_of_capital(self):
        from app.api.imports import process_wealthsimple_activity_csv
        frame = self.activity_frame().iloc[:1].copy()
        frame['activity_type'] = 'ReturnOfCapital'
        ignored = []
        rows, errors = process_wealthsimple_activity_csv(frame, ignored_rows=ignored)
        self.assertEqual((rows, errors), ([], []))
        self.assertEqual(ignored, [{'row_num': 2, 'type': 'ReturnOfCapital'}])


if __name__ == '__main__':
    unittest.main()
