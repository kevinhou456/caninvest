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


if __name__ == '__main__':
    unittest.main()
