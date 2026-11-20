"""
导出计算基准结果（用于数据库查询优化前后的结果对比）。

安全保证：
  - 只读取 --db 指定的数据库文件，先复制到临时目录，所有计算都在临时副本上进行，
    原文件不会被写入（脚本结束后临时副本自动删除）。
  - 临时副本中会清空纯缓存表（performance_daily_cache / report_analysis_cache /
    overview_snapshots），保证每次都是真实计算，而不是读到上一次的缓存。
  - 屏蔽全部网络访问（yfinance / 汇率 API），价格和汇率只用数据库里已有的数据，
    保证结果可复现。

两个层级：
  stable : 只涉及 --as-of 及之前的历史日期，任何时候重跑结果都应完全一致，
           可以直接和保存下来的基准 JSON 对比。
  today  : 依赖“今天”的页面（当前持仓、月度/每日分析、收益对比等），
           只能在同一天、同一份数据库上，用旧代码和新代码先后各跑一次再对比。
  pages  : 其余页面（总览、持仓看板、个股详情、持仓接口、月度个股盈亏、按账户类型筛选等），
           同样需要同日新旧代码对比。

用法示例：
  venv/bin/python tests/baseline/export_baseline.py \
      --db ../canadian_family_investment_backups/app_20261119_141621.db \
      --as-of 2026-11-18 --tier stable --out /path/baseline_stable.json

  # 用旧版本代码（git worktree）导出，便于同日对比 today 层级
  venv/bin/python tests/baseline/export_baseline.py --code-dir /path/to/old_worktree ...
"""
import argparse
import contextlib
import io
import json
import logging
import os
import random
import shutil
import socket
import sqlite3
import sys
import tempfile
import time
from datetime import date, datetime, timedelta
from decimal import Decimal

CACHE_TABLES = ('performance_daily_cache', 'report_analysis_cache', 'overview_snapshots')


# ---------------------------------------------------------------- 环境准备

def block_network():
    """禁止任何网络连接，外部请求会立即抛错（业务代码会捕获并回退到数据库数据）。

    注意 yfinance 通过 curl_cffi（libcurl，C 层）发请求，不经过 Python socket，
    所以除了屏蔽 socket，还要直接替换 yfinance 的入口，并把代理指向不可达地址。
    运行结束后 main() 还会核对价格表行数没有变化，作为兜底检查。
    """
    def _blocked(*_a, **_k):
        raise OSError('network disabled by export_baseline')
    socket.socket.connect = _blocked
    socket.socket.connect_ex = _blocked
    socket.create_connection = _blocked
    socket.getaddrinfo = _blocked
    for var in ('HTTP_PROXY', 'HTTPS_PROXY', 'ALL_PROXY', 'http_proxy', 'https_proxy', 'all_proxy'):
        os.environ[var] = 'http://127.0.0.1:9'
    os.environ.pop('NO_PROXY', None)
    os.environ.pop('no_proxy', None)
    import yfinance
    yfinance.Ticker = _blocked
    yfinance.download = _blocked
    try:
        import curl_cffi.requests as _curl
        _curl.Session.request = _blocked
        _curl.request = _blocked
    except Exception:
        pass


FAKE_FETCH_CALLS = []


def install_fake_price_fetch(source_db: str):
    """模拟联网抓价格：对任意请求区间返回确定性的日线数据，并记录每一次请求。

    - 源库里已有的日期：返回源库里的真实收盘价（和真实 Yahoo 一样，不会改动已有价格）
    - 源库里没有的工作日：返回按 (代码, 日期) 生成的固定合成价格
    这样新旧代码面对的是完全相同、可复现的“网络”，可以比较它们的抓取行为和最终结果。
    """
    import zlib
    from app.services.stock_price_service import StockPriceService

    conn = sqlite3.connect(f'file:{os.path.abspath(source_db)}?mode=ro', uri=True)
    known = {(sym, cur, d): float(c) for sym, cur, d, c in
             conn.execute('SELECT symbol, currency, trade_date, close_price FROM stock_price_history')}
    conn.close()

    def fake_get_stock_history(self, symbol, start_date, end_date, currency=None):
        cur = (currency or 'USD').upper()
        FAKE_FETCH_CALLS.append([symbol, str(start_date), str(end_date), cur])
        end = min(end_date, date.today())
        ts, closes = [], []
        d = start_date
        while d <= end:
            if d.weekday() < 5:
                close = known.get((symbol.upper(), cur, d.isoformat()))
                if close is None:
                    close = 10 + (zlib.crc32(f'{symbol}{d}'.encode()) % 10000) / 100
                ts.append(int(datetime(d.year, d.month, d.day, 12).timestamp()))
                closes.append(close)
            d += timedelta(days=1)
        if not ts:
            return {}, {'error': 'no_data'}
        return {'timestamp': ts, 'open': closes, 'high': closes, 'low': closes,
                'close': closes, 'volume': [1000] * len(ts)}, {}

    StockPriceService.get_stock_history = fake_get_stock_history


WATCHED_TABLES = ('stock_price_history', 'stock_holiday_attempts', 'market_holidays', 'exchange_rates',
                  'stocks_cache', 'transactions', 'cash')


def table_fingerprint(db_path: str) -> dict:
    """被监视表的 行数 + 最大 updated/rowid，用于确认运行期间没有写入外部数据。"""
    conn = sqlite3.connect(f'file:{db_path}?mode=ro', uri=True)
    out = {}
    for t in WATCHED_TABLES:
        try:
            out[t] = list(conn.execute(f'SELECT count(*), max(rowid) FROM {t}').fetchone())
        except sqlite3.OperationalError:
            pass
    conn.close()
    return out


def prepare_working_copy(src_db: str) -> str:
    tmp_dir = tempfile.mkdtemp(prefix='baseline_db_')
    work_db = os.path.join(tmp_dir, 'work.db')
    # 用 SQLite 备份 API 复制，源库只以只读方式打开
    src = sqlite3.connect(f'file:{os.path.abspath(src_db)}?mode=ro', uri=True)
    dst = sqlite3.connect(work_db)
    src.backup(dst)
    src.close()
    for table in CACHE_TABLES:
        try:
            dst.execute(f'DELETE FROM {table}')
        except sqlite3.OperationalError:
            pass
    dst.commit()
    dst.close()
    return work_db


def to_jsonable(obj):
    if isinstance(obj, Decimal):
        return float(obj)
    if isinstance(obj, (date, datetime)):
        return obj.isoformat()
    if isinstance(obj, dict):
        return {str(k): to_jsonable(v) for k, v in obj.items()}
    if isinstance(obj, (list, tuple, set)):
        return [to_jsonable(v) for v in obj]
    if hasattr(obj, 'to_dict'):
        return to_jsonable(obj.to_dict())
    return obj


# ---------------------------------------------------------------- 日期选择

def quarter_ends(start: date, end: date):
    out = []
    for y in range(start.year, end.year + 1):
        for m, d in ((3, 31), (6, 30), (9, 30), (12, 31)):
            q = date(y, m, d)
            if start <= q <= end:
                out.append(q)
    return out


def month_ends(end: date, count: int):
    out = []
    first = end.replace(day=1)
    for _ in range(count + 1):
        last = first - timedelta(days=1)
        first = last.replace(day=1)
        out.append(last)
    return sorted(d for d in out if d <= end)


def stable_dates(first_trade: date, as_of: date):
    dates = set(quarter_ends(first_trade, as_of))
    dates.update(month_ends(as_of, 12))
    rng = random.Random(20261119)  # 固定种子，保证每次选出的日期相同
    span = (as_of - first_trade).days
    for _ in range(12):
        dates.add(first_trade + timedelta(days=rng.randint(0, span)))
    dates.add(as_of)
    return sorted(d for d in dates if first_trade <= d <= as_of)


# ---------------------------------------------------------------- 导出逻辑

class Exporter:
    def __init__(self, app, as_of: date):
        self.app = app
        self.as_of = as_of
        self.client = app.test_client()
        self.result = {}
        self.timings = {}

    def record(self, key, fn, clear_perf_cache=False):
        t0 = time.time()
        if clear_perf_cache:
            # 只在临时副本里清空收益对比缓存，保证这一项走“无缓存”的完整计算路径
            from app import db
            with self.app.app_context():
                db.session.execute(db.text('DELETE FROM performance_daily_cache'))
                db.session.commit()
        try:
            # 每一项在独立的 app context 里计算，模拟一次独立请求（请求级记忆不会跨项共享）
            with self.app.app_context(), contextlib.redirect_stdout(io.StringIO()):
                value = to_jsonable(fn())
        except Exception as exc:  # 出错也作为基准的一部分记录下来
            value = {'__error__': f'{type(exc).__name__}: {exc}'}
        self.result[key] = value
        self.timings[key] = round(time.time() - t0, 2)
        print(f'  [{self.timings[key]:7.2f}s] {key}', file=sys.stderr, flush=True)

    def api(self, url):
        def _call():
            resp = self.client.get(url)
            return {'status': resp.status_code, 'body': resp.get_json(silent=True)}
        return _call

    def context(self):
        from app.models import Account, Member, Family, Transaction
        from app import db
        fam = Family.query.order_by(Family.id).first()
        accounts = Account.query.order_by(Account.id).all()
        members = Member.query.order_by(Member.id).all()
        first_trade = db.session.query(db.func.min(Transaction.trade_date)).scalar()
        if isinstance(first_trade, datetime):
            first_trade = first_trade.date()
        return fam.id, [a.id for a in accounts], [m.id for m in members], first_trade

    def export_stable(self):
        from app.services.portfolio_service import PortfolioService, TimePeriod
        from app.services.asset_valuation_service import AssetValuationService
        fam_id, account_ids, member_ids, first_trade = self.context()
        dates = stable_dates(first_trade, self.as_of)
        years = sorted({d.year for d in dates if d.year < self.as_of.year})
        self.result['__meta_stable__'] = {
            'dates': [d.isoformat() for d in dates], 'account_ids': account_ids,
            'member_ids': member_ids, 'family_id': fam_id, 'years': years,
        }
        ps = PortfolioService()
        av = AssetValuationService()

        # 1. 每个账户、每个日期的持仓汇总与现金
        for d in dates:
            for acc in account_ids:
                self.record(f'stable/summary/acct{acc}/{d}',
                            lambda: ps.get_portfolio_summary([acc], TimePeriod.CUSTOM, end_date=d))
                self.record(f'stable/cash/acct{acc}/{d}', lambda: av.get_cash_balance(acc, d))

        # 2. 全部账户合并的汇总（年末 + as_of）
        for d in [date(y, 12, 31) for y in years] + [self.as_of]:
            self.record(f'stable/summary/all/{d}',
                        lambda: ps.get_portfolio_summary(account_ids, TimePeriod.CUSTOM, end_date=d))

        # 3. 报表接口（只取已完结的年份）
        years_param = ','.join(str(y) for y in years)
        scopes = [('family', '')] + [(f'member{m}', f'&member_id={m}') for m in member_ids]
        scopes += [(f'acct{a}', f'&account_id={a}') for a in account_ids]
        base = f'/api/v1/families/{fam_id}/reports'
        for name, q in scopes:
            self.record(f'stable/annual/{name}', self.api(f'{base}/annual-analysis?years={years_param}{q}'))
            self.record(f'stable/quarterly/{name}', self.api(f'{base}/quarterly-analysis?years={years_param}{q}'))

        # 4. T5008（每个成员、每年，包括当年：只依赖交易记录）
        for m in member_ids:
            for y in years + [self.as_of.year]:
                self.record(f'stable/t5008/member{m}/{y}',
                            self.api(f'{base}/annual-analysis/t5008?member_id={m}&year={y}'))

        # 5. 每日统计月历（几个已完结的月份）
        cal_months = sorted({(d.year, d.month) for d in month_ends(self.as_of, 3)})
        for y, mth in cal_months:
            self.record(f'stable/calendar/family/{y}-{mth:02d}',
                        self.api(f'/api/v1/daily-stats/calendar?year={y}&month={mth}'))
            for m in member_ids:
                self.record(f'stable/calendar/member{m}/{y}-{mth:02d}',
                            self.api(f'/api/v1/daily-stats/calendar?year={y}&month={mth}&member_id={m}'))

    def export_today(self):
        from app.services.portfolio_service import PortfolioService, TimePeriod
        from app.services.asset_valuation_service import AssetValuationService
        fam_id, account_ids, member_ids, _ = self.context()
        today = date.today()
        self.result['__meta_today__'] = {'today': today.isoformat(),
                                         'generated_at': datetime.now().isoformat()}
        ps = PortfolioService()
        av = AssetValuationService()
        self.record('today/summary/all', lambda: ps.get_portfolio_summary(account_ids, TimePeriod.CUSTOM, end_date=today))
        for acc in account_ids:
            self.record(f'today/summary/acct{acc}', lambda: ps.get_portfolio_summary([acc], TimePeriod.CUSTOM, end_date=today))
            self.record(f'today/cash/acct{acc}', lambda: av.get_cash_balance(acc, today))

        base = f'/api/v1/families/{fam_id}/reports'
        scopes = [('family', '')] + [(f'member{m}', f'&member_id={m}') for m in member_ids]
        for name, q in scopes:
            self.record(f'today/annual/{name}', self.api(f'{base}/annual-analysis?_=1{q}'))
            self.record(f'today/quarterly/{name}', self.api(f'{base}/quarterly-analysis?_=1{q}'))
            self.record(f'today/monthly/{name}', self.api(f'{base}/monthly-analysis?_=1{q}'))
            self.record(f'today/daily/{name}', self.api(f'{base}/daily-analysis?days=30{q}'))
            self.record(f'today/holdings_distribution/{name}', self.api(f'{base}/holdings-distribution?_=1{q}'))
            for rng in ('1m', 'ytd', 'all'):
                self.record(f'today/comparison/{rng}/{name}', self.api(f'{base}/comparison?range={rng}{q}'))
        self.record('today/calendar/family/current', self.api('/api/v1/daily-stats/calendar'))

        # 收益对比（无缓存冷启动）：每项之前清空 performance_daily_cache
        cold_scopes = scopes + [(f'acct{a}', f'&account_id={a}') for a in account_ids]
        for name, q in cold_scopes:
            for rng in ('all', 'ytd', '1m'):
                self.record(f'today/comparison_cold/{rng}/{name}', self.api(f'{base}/comparison?range={rng}{q}'),
                            clear_perf_cache=True)
            self.record(f'today/comparison_cold/all_twr/{name}',
                        self.api(f'{base}/comparison?range=all&return_type=twr{q}'), clear_perf_cache=True)

    def html(self, url):
        """渲染 HTML 页面，按行保存（去掉首尾空白和空行），便于逐行对比。"""
        def _call():
            resp = self.client.get(url)
            text = resp.get_data(as_text=True)
            return {'status': resp.status_code,
                    'lines': [ln.strip() for ln in text.splitlines() if ln.strip()]}
        return _call

    def export_holdings_board(self):
        """持仓看板接口：按页面的真实调用方式传 account_ids（每个选中账户一个）+ period + separate。"""
        from app.models.account import AccountMember, Account, AccountType
        _, account_ids, member_ids, _ = self.context()
        selections = {'all': account_ids}
        for m in member_ids:
            selections[f'member{m}'] = sorted(am.account_id for am in AccountMember.query.filter_by(member_id=m).all())
        for a in account_ids:
            selections[f'acct{a}'] = [a]
        type_rows = Account.query.join(AccountType).with_entities(Account.id, AccountType.name).all()
        for t in sorted({name for _, name in type_rows}):
            selections[f'type_{t}'] = sorted(i for i, name in type_rows if name == t)
        for name, ids in selections.items():
            ids_q = '&'.join(f'account_ids={i}' for i in ids)
            for period in ('1M', '3M', '6M', 'YTD', '1Y', 'ALL'):
                for sep in ('false', 'true'):
                    if len(ids) == 1 and sep == 'true':
                        continue
                    self.record(f'pages/holdings_board_api/{name}/{period}/sep_{sep}',
                                self.api(f'/api/holdings-board?{ids_q}&period={period}&separate={sep}'))
        # 强制刷新价格（断网时外部请求会失败，走原有的失败回退逻辑）
        ids_q = '&'.join(f'account_ids={i}' for i in account_ids)
        self.record('pages/holdings_board_api/all/1M/force_refresh',
                    self.api(f'/api/holdings-board?{ids_q}&period=1M&separate=false&force_refresh=true'))

    def export_pages(self):
        """其余页面与参数组合（依赖今天，需同日新旧代码对比）。"""
        from app import db
        from app.models import Transaction
        fam_id, account_ids, member_ids, _ = self.context()
        today = date.today()
        base = f'/api/v1/families/{fam_id}/reports'
        member_scopes = [('family', '')] + [(f'member{m}', f'&member_id={m}') for m in member_ids]
        type_scopes = [(f'type_{t}', f'&account_type={t}') for t in ('Regular', 'TFSA', 'RRSP')]
        member_type_scopes = [(f'member{m}_type_{t}', f'&member_id={m}&account_type={t}')
                              for m in member_ids for t in ('Regular', 'TFSA', 'RRSP')]

        # 总览页（full=1 才会计算数据）
        for name, q in member_scopes + type_scopes + [(f'acct{a}', f'&account_id={a}') for a in account_ids]:
            self.record(f'pages/overview/{name}', self.html(f'/overview?full=1{q}'))
        for period in ('ytd', 'last_month', 'last_quarter', 'last_365_days'):
            self.record(f'pages/overview/family/{period}', self.html(f'/overview?full=1&period={period}'))

        # 持仓看板
        self.export_holdings_board()
        self.record('pages/holdings_board_html', self.html('/holdings-board'))
        self.record('pages/holdings_analysis_html', self.html('/holdings-analysis'))

        # 持仓接口
        as_of_dates = [today.isoformat(), (today - timedelta(days=45)).isoformat(), '2025-12-31']
        for d in as_of_dates:
            self.record(f'pages/holdings_api/all/{d}', self.api(f'/api/v1/holdings?family_id={fam_id}&as_of_date={d}'))
            for a in account_ids:
                self.record(f'pages/holdings_snapshot/acct{a}/{d}',
                            self.api(f'/api/v1/holdings/snapshot/{a}?as_of_date={d}'))
            for m in member_ids:
                self.record(f'pages/holdings_member/member{m}/{d}',
                            self.api(f'/api/v1/holdings/member/{m}?as_of_date={d}'))
        self.record('pages/cash_data', self.api('/api/accounts/cash-data'))

        # 个股详情（持仓最多的几只 + 已清仓的一只）
        symbols = [s for (s,) in db.session.query(Transaction.stock).filter(
            Transaction.stock.isnot(None), Transaction.stock != '').group_by(Transaction.stock)
            .order_by(db.func.count(Transaction.id).desc()).limit(6).all()]
        symbols += [s for (s,) in db.session.query(Transaction.stock).filter(
            Transaction.stock.isnot(None), Transaction.stock != '').group_by(Transaction.stock)
            .order_by(db.func.max(Transaction.trade_date).asc()).limit(2).all()]
        for s in symbols:
            self.record(f'pages/stock_detail/{s}', self.html(f'/stock/{s}'))
            for m in member_ids:
                self.record(f'pages/stock_detail/{s}/member{m}', self.html(f'/stock/{s}?member_id={m}'))
            self.record(f'pages/stocks_symbol/{s}', self.html(f'/stocks/{s}'))
        self.record('pages/stocks_list', self.html('/stocks'))

        # 报表接口的其他参数组合
        years = sorted({d.year for d in [today.replace(month=1, day=1) - timedelta(days=1)]} | {today.year})
        for name, q in type_scopes + member_type_scopes:
            self.record(f'pages/annual/{name}', self.api(f'{base}/annual-analysis?_=1{q}'))
            self.record(f'pages/quarterly/{name}', self.api(f'{base}/quarterly-analysis?_=1{q}'))
            self.record(f'pages/monthly/{name}', self.api(f'{base}/monthly-analysis?_=1{q}'))
            self.record(f'pages/comparison/ytd/{name}', self.api(f'{base}/comparison?range=ytd{q}'))
            self.record(f'pages/holdings_distribution/{name}', self.api(f'{base}/holdings-distribution?_=1{q}'))
        for name, q in member_scopes:
            self.record(f'pages/annual_separate/{name}', self.api(f'{base}/annual-analysis?separate_accounts=1{q}'))
            for rng in ('3m', '6m', '1y'):
                self.record(f'pages/comparison/{rng}/{name}', self.api(f'{base}/comparison?range={rng}{q}'))
        for a in account_ids:
            self.record(f'pages/monthly/acct{a}', self.api(f'{base}/monthly-analysis?account_id={a}'))
            self.record(f'pages/comparison/1m/acct{a}', self.api(f'{base}/comparison?range=1m&account_id={a}'))
        for name, q in member_scopes:
            for (y, mth) in [(today.year, today.month), (today.year, max(1, today.month - 1)), (2025, 12)]:
                self.record(f'pages/monthly_stock_pnl/{name}/{y}-{mth:02d}',
                            self.api(f'{base}/monthly-stock-pnl?year={y}&month={mth}{q}'))
        for y in years:
            self.record(f'pages/tax_summary/{y}', self.api(f'{base}/tax-summary?year={y}'))
            for m in member_ids:
                self.record(f'pages/contributions/member{m}/{y}',
                            self.api(f'/api/v1/members/{m}/reports/contributions?year={y}'))
        self.record('pages/portfolio_report', self.api(f'{base}/portfolio'))
        for a in account_ids:
            self.record(f'pages/account_performance/acct{a}', self.api(f'/api/v1/accounts/{a}/reports/performance'))
            self.record(f'pages/t5008_acct/acct{a}/2025', self.api(f'{base}/annual-analysis/t5008?account_id={a}&year=2025'))

        # 每日统计的其他接口
        for d in (today, today - timedelta(days=1), today - timedelta(days=30)):
            self.record(f'pages/daily_day/family/{d}', self.api(f'/api/v1/daily-stats/day/{d.isoformat()}'))
        for name, q in member_scopes:
            self.record(f'pages/calendar_summary/{name}', self.api(f'/api/v1/daily-stats/calendar/summary?_=1{q}'))
            self.record(f'pages/calendar_current/{name}', self.api(f'/api/v1/daily-stats/calendar/current?_=1{q}'))

        # 其他统计页面的 HTML 外壳
        for path in ('/annual-stats', '/monthly-stats', '/quarterly-stats', '/daily-stats', '/comparison',
                     '/accounts', '/transactions'):
            self.record(f'pages/html{path}', self.html(path))


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('--db', required=True, help='冻结的数据库文件（只读，不会被修改）')
    parser.add_argument('--out', required=True, help='输出 JSON 路径')
    parser.add_argument('--as-of', required=True, help='stable 层级的截止日期 YYYY-MM-DD（必须早于今天）')
    parser.add_argument('--tier', choices=['stable', 'today', 'pages', 'holdings', 'all'], default='all')
    parser.add_argument('--code-dir', default=os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')),
                        help='要运行的代码根目录（默认当前仓库；可指向旧版本的 git worktree）')
    parser.add_argument('--network', action='store_true',
                        help='允许联网（仅用于排查联网时新旧代码的抓取差异；结果不可用于基准对比）')
    parser.add_argument('--fake-network', action='store_true',
                        help='模拟联网抓价格（确定性合成数据 + 记录请求），用于比较新旧代码的抓取行为')
    parser.add_argument('--keep-db', help='运行结束后把临时数据库副本保存到这个路径（便于检查写入了什么）')
    args = parser.parse_args()

    as_of = date.fromisoformat(args.as_of)
    if as_of >= date.today():
        parser.error('--as-of 必须早于今天，否则 stable 结果会随时间变化')

    work_db = prepare_working_copy(args.db)
    os.environ['DATABASE_URL'] = 'sqlite:///' + work_db
    if not args.network:
        block_network()
    fingerprint_before = table_fingerprint(work_db)
    sys.path.insert(0, os.path.abspath(args.code_dir))
    logging.disable(logging.WARNING)
    import warnings
    warnings.filterwarnings('ignore')

    from app import create_app
    if args.fake_network:
        install_fake_price_fetch(args.db)
    app = create_app()
    assert app.config['SQLALCHEMY_DATABASE_URI'].endswith(work_db), '数据库没有指向临时副本，已中止'

    try:
        with app.app_context():
            exporter = Exporter(app, as_of)
            t0 = time.time()
            if args.tier in ('stable', 'all'):
                exporter.export_stable()
            if args.tier in ('today', 'all'):
                exporter.export_today()
            if args.tier in ('pages', 'all'):
                exporter.export_pages()
            if args.tier == 'holdings':
                exporter.export_holdings_board()
            payload = {
                'manifest': {
                    'source_db': os.path.abspath(args.db), 'as_of': as_of.isoformat(),
                    'tier': args.tier, 'code_dir': os.path.abspath(args.code_dir),
                    'git_commit': os.popen(f'git -C "{args.code_dir}" rev-parse HEAD').read().strip(),
                    'generated_at': datetime.now().isoformat(),
                    'total_seconds': round(time.time() - t0, 1),
                },
                'timings': exporter.timings,
                'results': exporter.result,
            }
        from app import db
        with app.app_context():
            db.session.remove()
            db.engine.dispose()
        fingerprint_after = table_fingerprint(work_db)
        data_changed = fingerprint_before != fingerprint_after
        payload['manifest'].update({
            'network': 'fake' if args.fake_network else args.network, 'data_changed': data_changed,
            'fake_fetch_calls': FAKE_FETCH_CALLS,
            'fingerprint_before': fingerprint_before, 'fingerprint_after': fingerprint_after,
        })
        if args.keep_db:
            dst = sqlite3.connect(args.keep_db)
            src = sqlite3.connect(work_db)
            src.backup(dst)
            src.close()
            dst.close()
        with open(args.out, 'w', encoding='utf-8') as f:
            json.dump(payload, f, ensure_ascii=False, indent=1, sort_keys=True)
        print(f'written {args.out}: {len(exporter.result)} entries, {payload["manifest"]["total_seconds"]}s',
              file=sys.stderr)
        if data_changed:
            changed = {t: (fingerprint_before.get(t), fingerprint_after.get(t)) for t in fingerprint_after
                       if fingerprint_before.get(t) != fingerprint_after.get(t)}
            print(f'!!! 运行期间数据库被写入了外部数据，本次结果不能作为基准对比: {changed}', file=sys.stderr)
            if not args.network and not args.fake_network:
                sys.exit(2)
    finally:
        shutil.rmtree(os.path.dirname(work_db), ignore_errors=True)


if __name__ == '__main__':
    main()
