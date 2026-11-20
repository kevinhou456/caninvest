"""
卖超检测：卖出数量超过卖出前持有数量时，核心成本计算会记录一条 oversold_events（只记录，不改变计算）；
总览页把记录按 股票+币种 挂到表格行上。
"""
import unittest
from datetime import date
from decimal import Decimal
from types import SimpleNamespace

from app.main.routes import _attach_oversold_flags
from app.services.portfolio_service import PortfolioService, PositionSnapshot


def tx(tx_id, day, typ, qty, price=10):
    qty = Decimal(str(qty))
    return SimpleNamespace(id=tx_id, trade_date=date(2025, 1, day), type=typ, quantity=qty,
                           net_amount=qty * Decimal(str(price)), amount=None)


class OversoldDetectionTest(unittest.TestCase):
    def run_txs(self, txs):
        snapshot = PositionSnapshot('AAA', 1, date(2025, 2, 1))
        service = PortfolioService.__new__(PortfolioService)  # 只用到 _process_transaction_acb，无需数据库
        # 与核心查询相同的顺序：交易日、同日 BUY 优先、id
        for t in sorted(txs, key=lambda t: (t.trade_date, 0 if t.type == 'BUY' else 1, t.id)):
            service._process_transaction_acb(snapshot, t)
        return snapshot

    def test_normal_sell_all_is_not_flagged(self):
        s = self.run_txs([tx(1, 1, 'BUY', 10), tx(2, 2, 'SELL', 4), tx(3, 3, 'SELL', 6)])
        self.assertEqual(s.oversold_events, [])
        self.assertEqual(s.current_shares, 0)

    def test_same_day_sell_recorded_before_buy_is_not_flagged(self):
        s = self.run_txs([tx(1, 5, 'SELL', 3), tx(2, 5, 'BUY', 3)])
        self.assertEqual(s.oversold_events, [])

    def test_within_tolerance_is_not_flagged(self):
        s = self.run_txs([tx(1, 1, 'BUY', '10'), tx(2, 2, 'SELL', '10.00005')])
        self.assertEqual(s.oversold_events, [])

    def test_oversell_is_flagged_with_details_and_kept_after_buy_back(self):
        s = self.run_txs([tx(1, 1, 'BUY', 10), tx(2, 2, 'SELL', 11), tx(3, 3, 'BUY', 5)])
        self.assertEqual(len(s.oversold_events), 1)
        event = s.oversold_events[0]
        self.assertEqual(event['transaction_id'], 2)
        self.assertEqual(event['trade_date'], '2025-01-02')
        self.assertAlmostEqual(event['sell_quantity'], 11)
        self.assertAlmostEqual(event['shares_before'], 10)
        self.assertAlmostEqual(event['shortfall'], 1)
        self.assertEqual(s.current_shares, 4)  # 计算本身不变：10 - 11 + 5

    def test_sell_without_any_holding_is_flagged(self):
        s = self.run_txs([tx(1, 1, 'SELL', 2)])
        self.assertEqual([e['shares_before'] for e in s.oversold_events], [0.0])

    def test_flags_do_not_change_calculation(self):
        """有无卖超记录，成本/收益计算结果相同（与不带检测的同一套 ACB 运算比较）。"""
        s = self.run_txs([tx(1, 1, 'BUY', 10, 10), tx(2, 2, 'SELL', 12, 15), tx(3, 3, 'BUY', 4, 20)])
        # 手算：卖出前 10 股成本 100 → 均价 10 → 卖 12 股成本 120 → 剩余成本 -20，已实现 180-120=60
        self.assertEqual(s.realized_gain, Decimal('60'))
        self.assertEqual(s.total_cost, Decimal('60'))      # -20 + 80
        self.assertEqual(s.current_shares, Decimal('2'))

    def test_to_dict_contains_copies(self):
        s = self.run_txs([tx(1, 1, 'SELL', 2)])
        d = s.to_dict()
        d['oversold_events'][0]['shortfall'] = -1
        self.assertAlmostEqual(s.oversold_events[0]['shortfall'], 2)


class AttachOversoldFlagsTest(unittest.TestCase):
    def test_flags_attached_by_symbol_and_currency_across_tables(self):
        event = {'trade_date': '2025-01-02', 'transaction_id': 7, 'sell_quantity': 3.0,
                 'shares_before': 1.0, 'shortfall': 2.0}
        raw = [
            {'symbol': 'AAA', 'currency': 'USD', 'account_name': 'A1', 'oversold_events': []},
            {'symbol': 'AAA', 'currency': 'USD', 'account_name': 'A2', 'oversold_events': [event]},  # 已清仓的账户
            {'symbol': 'AAA', 'currency': 'CAD', 'account_name': 'A1', 'oversold_events': []},
            {'symbol': 'BBB', 'currency': 'USD', 'account_name': 'A1'},
        ]
        rows = [{'symbol': 'AAA', 'currency': 'USD'}, {'symbol': 'AAA', 'currency': 'CAD'},
                {'symbol': 'BBB', 'currency': 'USD'}]
        _attach_oversold_flags(rows, raw, lambda p: p['account_name'])
        self.assertEqual([r['account_name'] for r in rows[0]['oversold_flags']], ['A2'])
        self.assertEqual(rows[0]['oversold_flags'][0]['shortfall'], 2.0)
        self.assertEqual(rows[1]['oversold_flags'], [])
        self.assertEqual(rows[2]['oversold_flags'], [])
        self.assertNotIn('account_name', event)  # 不修改原始记录


if __name__ == '__main__':
    unittest.main()
