"""Broker CSV prose must not become transactions or hide invalid data."""
import json
import tempfile
import unittest
from pathlib import Path

from app.api.imports import _parse_csv_bytes, _load_csv_dataframe


class CsvSurroundingNotesTests(unittest.TestCase):
    def parse(self, text):
        return _parse_csv_bytes(text.encode('utf-8'), ['utf-8'])['dataframe']

    def test_preamble_and_footer(self):
        text = ('Broker statement\nThis report is provided for your information\n\n'
                'date,type,symbol,quantity,price,currency,description\n'
                '2026-01-01,BUY,VFV,2,100,CAD,Purchase\n'
                '2026-01-02,SELL,VFV,1,110,CAD,Sale\n\n'
                'Notes: This report is for information only\n'
                'Please contact your broker for further details,,,,,,\n')
        df = self.parse(text)
        self.assertEqual(df['type'].tolist(), ['BUY', 'SELL'])

    def test_prose_does_not_determine_delimiter(self):
        text = ('Some prose,with commas,not a header\n' * 12 +
                'date;type;symbol;amount;currency\n'
                '2026-01-01;Dividend;VFV;12;CAD\n'
                'Notes: Please retain this report for your records\n')
        self.assertEqual(self.parse(text)['amount'].tolist(), [12])

    def test_multiline_preamble_and_transaction_description(self):
        text = ('"Broker introduction\ncontinued on another line"\n\n'
                'date,type,amount,currency,description\n'
                '2026-01-01,Dividend,12,CAD,"Cash dividend\nsecond line, with comma"\n'
                'Notes: End of statement\n')
        df = self.parse(text)
        self.assertEqual(len(df), 1)
        self.assertEqual(df.iloc[0]['description'], 'Cash dividend\nsecond line, with comma')

    def test_invalid_transaction_at_end_is_not_ignored(self):
        text = ('date,type,amount,currency\n2026-01-01,Dividend,12,CAD\n'
                'bad-date,UNKNOWN,invalid,CAD\nNotes: End of statement\n')
        df = self.parse(text)
        self.assertEqual(len(df), 2)
        self.assertEqual(df.iloc[-1]['type'], 'UNKNOWN')

    def test_incomplete_dated_row_is_not_ignored(self):
        df = self.parse('date,type,amount,currency\n2026-01-01,Dividend,12,CAD\n2026-02-01\n')
        self.assertEqual(len(df), 2)

    def test_final_import_uses_same_boundaries_with_old_metadata(self):
        text = ('Broker statement\ndate,type,amount,currency\n'
                '2026-01-01,Dividend,12,CAD\nNotes: End of statement\n')
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / 'session.csv'
            meta = Path(directory) / 'session.json'
            path.write_text(text)
            meta.write_text(json.dumps(dict(encoding='utf-8', delimiter=',', header_index=1)))
            loaded, _ = _load_csv_dataframe(str(path), str(meta))
            self.assertTrue(self.parse(text).equals(loaded))

    def test_tab_separated_and_chinese_notes(self):
        df = self.parse('账户说明\ndate\ttype\tamount\tcurrency\n2026-01-01\tDividend\t12\tCAD\n说明：此文件仅供参考\n')
        self.assertEqual(len(df), 1)

    def test_extra_transaction_fields_are_rejected_not_shifted(self):
        with self.assertRaises(ValueError):
            self.parse('date,type,amount,currency\n2026-01-01,Dividend,12,CAD,unexpected\n')


class CsvSeparatorRowTests(unittest.TestCase):
    TEXT = ('Transaction Type=ALL,Product Type=All,Symbol=,From=2026-08-20,To=2026-11-17,,,,,\n'
            'Transaction Date,Settlement Date,Activity Description,Description,Symbol,Quantity,Price,Price Currency,Total Amount,Total Currency\n'
            '----------------,---------------,--------------------,-----------,------,--------,-----,--------------,------------,--------------\n'
            '2026-11-11,2026-11-12,Buy,CI INVESCO NASDAQ 100 INDEX ET,QQC,81,48.21,CAD,-3905.01,CAD\n'
            '2026-11-04,2026-11-05,Sell,MARVELL TECHNOLOGY COM STOCK,MRVL,-11,159.46,USD,1754.04,USD\n'
            '2026-10-27,2026-10-27,Transfer in ,CSH TRANSFER C$ FDS,,0,0,,9519.66,CAD\n'
            '2026-10-27,2026-10-27,Transfer in ,MARVELL TECHNOLOGY COM STOCK,MRVL,11,0,,0,USD\n')

    def test_separator_row_is_dropped_and_rows_parse(self):
        import io
        from contextlib import redirect_stdout
        from app.api.imports import smart_header_mapping, process_csv_with_mapping
        with redirect_stdout(io.StringIO()):
            df = _parse_csv_bytes(self.TEXT.encode('utf-8'), ['utf-8'])['dataframe']
            mapping = smart_header_mapping(df.columns.tolist())
            ignored = []
            rows, errors = process_csv_with_mapping(df, mapping, ignored_rows=ignored)
        # In-kind security transfer is ignored; the cash transfer stays a deposit.
        self.assertEqual(ignored, [{'row_num': 5, 'type': 'SecurityTransfer'}])
        self.assertEqual(df['Symbol'].tolist()[:2], ['QQC', 'MRVL'])
        # 'action' in 'Transaction Date' must not claim the type field.
        self.assertEqual(mapping['type'], 'Activity Description')
        self.assertEqual(errors, [])
        self.assertEqual([(r['type'], r['currency']) for r in rows],
                         [('BUY', 'CAD'), ('SELL', 'USD'), ('DEPOSIT', 'CAD')])
