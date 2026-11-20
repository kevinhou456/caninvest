"""
旧接口清理的回归测试：
  - 依赖已删除旧持仓系统的报表接口已移除（404）
  - GET /members/<id> 不再调用不存在的方法
  - /daily-stats/calendar/current 不带参数时不再报 ownership_map 未定义
  - 旧的 /stocks/<symbol> 跳转到个股详情页 /stock/<symbol>
"""
import io
import unittest
from contextlib import redirect_stdout
from unittest.mock import patch

from app import create_app, db
from app.models.account import Account, AccountMember, AccountType
from app.models.family import Family
from app.models.member import Member
from app.models.stocks_cache import StocksCache


class LegacyEndpointsTest(unittest.TestCase):
    def setUp(self):
        self.output = redirect_stdout(io.StringIO())
        self.output.__enter__()
        self.app = create_app('testing')
        self.context = self.app.app_context()
        self.context.push()
        db.create_all()
        family = Family(name='F')
        db.session.add(family)
        db.session.flush()
        member = Member(name='M', family_id=family.id)
        acct_type = AccountType(name='Regular')
        db.session.add_all([member, acct_type])
        db.session.flush()
        account = Account(name='A', family_id=family.id, account_type_id=acct_type.id)
        db.session.add(account)
        db.session.flush()
        db.session.add(AccountMember(account_id=account.id, member_id=member.id, ownership_percentage=100))
        db.session.add(StocksCache(symbol='NVDA', currency='USD', name='NVIDIA'))
        db.session.commit()
        self.family_id, self.member_id, self.account_id = family.id, member.id, account.id
        self.client = self.app.test_client()

    def tearDown(self):
        db.session.remove()
        db.drop_all()
        self.context.pop()
        self.output.__exit__(None, None, None)

    def test_removed_legacy_report_endpoints(self):
        for url in (f'/api/v1/families/{self.family_id}/reports/portfolio',
                    f'/api/v1/accounts/{self.account_id}/reports/performance',
                    f'/api/v1/members/{self.member_id}/reports/contributions',
                    f'/api/v1/families/{self.family_id}/reports/tax-summary',
                    f'/api/v1/members/{self.member_id}/portfolio',
                    f'/api/v1/members/{self.member_id}/contributions'):
            self.assertEqual(self.client.get(url).status_code, 404, url)

    def test_get_member(self):
        resp = self.client.get(f'/api/v1/members/{self.member_id}')
        self.assertEqual(resp.status_code, 200)
        body = resp.get_json()
        self.assertEqual(body['name'], 'M')
        self.assertEqual([a['id'] for a in body['accounts']], [self.account_id])
        self.assertNotIn('portfolio_summary', body)

    def test_calendar_current_without_params(self):
        from app.api import daily_stats
        with patch.object(daily_stats.daily_stats_service, 'get_current_month_calendar') as calendar:
            calendar.return_value.to_dict.return_value = {}
            resp = self.client.get('/api/v1/daily-stats/calendar/current')
        self.assertEqual(resp.status_code, 200)
        self.assertTrue(resp.get_json()['success'])
        self.assertEqual(calendar.call_args.args[0], [self.account_id])
        self.assertIsNone(calendar.call_args.args[1])  # 不带成员参数时不按出资比例换算

    def test_old_stocks_page_redirects_to_stock_detail(self):
        resp = self.client.get('/stocks/NVDA?account_id=3')
        self.assertEqual(resp.status_code, 302)
        self.assertTrue(resp.headers['Location'].endswith('/stock/NVDA?account_id=3'))
        self.assertEqual(self.client.get('/stocks/UNKNOWN').status_code, 404)


if __name__ == '__main__':
    unittest.main()
