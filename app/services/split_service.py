"""
拆股/合股的唯一实现。

数据：stock_splits 表（按 代码+币种 登记，所有账户共用）。
约定：
  - 拆股日 S 当天开盘生效：S 当天及以后的交易数量、价格都是拆股后的。
  - 数据库里的历史价格一律存“当天实际成交价”（Yahoo 返回的是按此后所有拆股调整过的价格，
    保存前用 to_actual() 还原）。
  - 持仓计算：在交易流里插入拆股事件（with_split_events），由 PortfolioService._process_transaction_acb
    处理：股数 × 比例，总成本不变。T5008 使用同一个事件流。
  - 需要连续可比价格的地方（拆股当天的“昨收”、基准指数收益率）用 to_basis() 把实际价换算到同一股数口径。
"""
from dataclasses import dataclass
from datetime import date
from decimal import Decimal
from typing import Dict, Iterable, List, Optional, Sequence, Tuple

from app import db
from app.models.stock_split import StockSplit

ONE = Decimal('1')


@dataclass
class SplitEvent:
    """插入交易流的拆股事件，字段与 Transaction 对齐，便于和交易一起排序、处理。"""
    stock: str
    currency: str
    trade_date: date
    ratio: Decimal
    type: str = 'SPLIT'
    id: Optional[int] = None
    account_id: Optional[int] = None
    quantity: Decimal = Decimal('0')
    price: Decimal = Decimal('0')
    fee: Decimal = Decimal('0')
    amount: Decimal = Decimal('0')


def _all_splits() -> Dict[Tuple[str, str], Tuple[Tuple[date, Decimal], ...]]:
    """{(代码, 币种): ((拆股日, 比例), ...) 按日期升序}；同一请求内只读一次，有写入时自动重读。"""
    from app.utils.query_memo import memo

    def _load():
        result: Dict[Tuple[str, str], List[Tuple[date, Decimal]]] = {}
        rows = db.session.query(StockSplit.symbol, StockSplit.currency, StockSplit.split_date, StockSplit.ratio) \
            .order_by(StockSplit.split_date.asc()).all()
        for symbol, currency, split_date, ratio in rows:
            result.setdefault((symbol.upper(), currency.upper()), []).append((split_date, Decimal(str(ratio))))
        return {k: tuple(v) for k, v in result.items()}

    return memo('stock_splits', 'all', _load)


def get_splits(symbol: Optional[str], currency: Optional[str]) -> Tuple[Tuple[date, Decimal], ...]:
    if not symbol or not currency:
        return ()
    return _all_splits().get((symbol.upper(), currency.upper()), ())


def factor_between(symbol: str, currency: str, after_date: date, through_date: Optional[date] = None) -> Decimal:
    """after_date < 拆股日 <= through_date（None 表示不设上限）的所有拆股比例之积。"""
    factor = ONE
    for split_date, ratio in get_splits(symbol, currency):
        if split_date > after_date and (through_date is None or split_date <= through_date):
            factor *= ratio
    return factor


def to_actual(adjusted_price, symbol: str, currency: str, price_date: date):
    """Yahoo 调整后价格 → price_date 当天的实际成交价。传入 float 返回 float，传入 Decimal 返回 Decimal。"""
    factor = factor_between(symbol, currency, price_date)
    if factor == ONE or adjusted_price is None:
        return adjusted_price
    if isinstance(adjusted_price, Decimal):
        return adjusted_price * factor
    return float(Decimal(str(adjusted_price)) * factor)


def to_basis(actual_price, symbol: str, currency: str, price_date: date, basis_date: Optional[date] = None):
    """price_date 的实际价 → basis_date 的股数口径（中间每拆 1 拆 N，价格除以 N）。basis_date=None 表示最新口径。"""
    factor = factor_between(symbol, currency, price_date, basis_date)
    if factor == ONE or actual_price is None:
        return actual_price
    if isinstance(actual_price, Decimal):
        return actual_price / factor
    return float(Decimal(str(actual_price)) / factor)


def with_split_events(transactions: Sequence, through_date: Optional[date] = None) -> List:
    """把拆股事件插入一只股票的有序交易列表（可含多个账户）。

    只插入 第一笔交易日 < 拆股日 <= through_date 的拆股（之前没有持仓，拆了也没有影响）；
    拆股事件排在拆股日当天所有交易之前。交易本身的相对顺序不变。
    """
    if not transactions or any(isinstance(tx, SplitEvent) for tx in transactions):
        return list(transactions)
    first = transactions[0]
    symbol, currency = first.stock, (first.currency or '')
    splits = [(d, r) for d, r in get_splits(symbol, currency)
              if d > first.trade_date and (through_date is None or d <= through_date)]
    if not splits:
        return list(transactions)
    merged: List = []
    pending = list(splits)
    for tx in transactions:
        while pending and pending[0][0] <= tx.trade_date:
            split_date, ratio = pending.pop(0)
            merged.append(SplitEvent(stock=symbol, currency=currency, trade_date=split_date, ratio=ratio))
        merged.append(tx)
    for split_date, ratio in pending:
        merged.append(SplitEvent(stock=symbol, currency=currency, trade_date=split_date, ratio=ratio))
    return merged


def record_split(symbol: str, currency: str, split_date: date, ratio, source: str = 'yahoo') -> bool:
    """登记一次拆股；已存在（同代码、币种、日期）时不改动。返回是否新增。调用方负责提交事务。"""
    symbol, currency = symbol.upper(), currency.upper()
    ratio = Decimal(str(ratio))
    if ratio <= 0 or ratio == ONE:
        return False
    existing = StockSplit.query.filter_by(symbol=symbol, currency=currency, split_date=split_date).first()
    if existing:
        return False
    db.session.add(StockSplit(symbol=symbol, currency=currency, split_date=split_date, ratio=ratio, source=source))
    db.session.flush()
    return True


# 比例与 1 相差不到 10% 的不当作拆股：加拿大 ETF 年底“再投资分配 + 合并”在 Yahoo 上显示为
# 0.993、1.021 之类的拆股，但持有份额实际不变（只增加 ACB），不能按比例换算股数
MIN_SPLIT_DEVIATION = Decimal('0.1')


def parse_yahoo_splits(items: Iterable[Tuple[date, float]]) -> List[Tuple[date, Decimal]]:
    """Yahoo 的拆股数据 → [(日期, 比例)]，过滤 0、无效值和接近 1 的伪拆股；比例保留 8 位小数（合股如 1/3）。"""
    result = []
    for split_date, value in items:
        try:
            ratio = Decimal(str(value)).quantize(Decimal('0.00000001'))
        except Exception:
            continue
        if ratio > 0 and abs(ratio - ONE) >= MIN_SPLIT_DEVIATION:
            result.append((split_date, ratio))
    return result
