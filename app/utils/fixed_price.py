"""
固定价格的证券（如银行投资储蓄账户 ISA：每份净值恒定，不在交易所交易，yfinance 没有数据）。

这些代码的当前价和历史价一律返回固定值，不访问外部接口、不写入历史价格表。
价格查询的两个入口（StockPriceService 当前价、StockHistoryCacheService 历史价）都先查这里。

新增同类产品时只需在 FIXED_PRICES 里加一行：(代码, 币种) -> 每份价格。
"""
from datetime import date, timedelta
from decimal import Decimal
from typing import Dict, List, Optional

FIXED_PRICES: Dict[tuple, Decimal] = {
    # RBC Investment Savings Account Series A（利息每月以份额再投资，每份 $10）
    ('RBF2010.TO', 'CAD'): Decimal('10'),
}


def get_fixed_price(symbol: Optional[str], currency: Optional[str] = None) -> Optional[Decimal]:
    """返回固定价格；不是固定价格证券时返回 None。未给币种时按代码匹配。"""
    if not symbol:
        return None
    symbol = symbol.strip().upper()
    if currency:
        return FIXED_PRICES.get((symbol, currency.strip().upper()))
    for (sym, _cur), price in FIXED_PRICES.items():
        if sym == symbol:
            return price
    return None


def get_fixed_currency(symbol: Optional[str]) -> Optional[str]:
    """固定价格证券的币种；不是固定价格证券时返回 None。"""
    if not symbol:
        return None
    symbol = symbol.strip().upper()
    for sym, cur in FIXED_PRICES:
        if sym == symbol:
            return cur
    return None


def fixed_price_history(symbol: str, currency: str, start_date: date, end_date: date) -> List[Dict]:
    """生成与历史价格表行格式相同的记录：区间内每个工作日一行，开高低收都是固定价格。"""
    price = get_fixed_price(symbol, currency)
    if price is None:
        return []
    value = float(price)
    symbol = symbol.strip().upper()
    currency = currency.strip().upper()
    rows = []
    day = start_date
    while day <= end_date:
        if day.weekday() < 5:
            iso = day.isoformat()
            rows.append({
                'id': None, 'symbol': symbol, 'currency': currency,
                'trade_date': iso, 'date': iso,
                'open_price': value, 'open': value,
                'high_price': value, 'high': value,
                'low_price': value, 'low': value,
                'close_price': value, 'close': value,
                'adjusted_close': value, 'adj_close': value,
                'volume': 0, 'data_source': 'fixed_price',
                'created_at': None, 'updated_at': None,
            })
        day += timedelta(days=1)
    return rows


REINVEST_NOTE_PREFIX = '利息再投资（按每份 ${price} 买入）：'


def add_reinvestment_buy(transaction) -> bool:
    """固定价格证券的利息以份额再投资：导入一条 INTEREST 时补上同日 BUY（份额 @ 固定价格），现金净额为 0。

    只处理 INTEREST + 固定价格证券；同账户同日已有相同份额的 BUY（数据库里或本次导入中）时不重复添加。
    份额取利息记录的 quantity，没有时用 金额 ÷ 固定价格。调用方负责提交事务。返回是否新增。
    """
    from app import db
    from app.models.transaction import Transaction

    if (transaction.type or '').upper() != 'INTEREST':
        return False
    price = get_fixed_price(transaction.stock, transaction.currency)
    if price is None:
        return False
    units = Decimal(str(transaction.quantity or 0))
    if units <= 0 and transaction.amount:
        units = (Decimal(str(transaction.amount)) / price).quantize(Decimal('0.0001'))
    if units <= 0:
        return False
    symbol = transaction.stock.strip().upper()
    currency = transaction.currency.strip().upper()
    with db.session.no_autoflush:
        pending = [obj for obj in db.session.new if isinstance(obj, Transaction) and obj is not transaction]
    candidates = pending + Transaction.query.filter_by(
        account_id=transaction.account_id, trade_date=transaction.trade_date, type='BUY',
        stock=symbol, currency=currency).all()
    for tx in candidates:
        if (tx.account_id == transaction.account_id and tx.trade_date == transaction.trade_date
                and (tx.type or '').upper() == 'BUY' and (tx.stock or '').upper() == symbol
                and abs(Decimal(str(tx.quantity or 0)) - units) < Decimal('0.0001')):
            return False
    price_text = format(price.normalize(), 'f')
    db.session.add(Transaction(
        account_id=transaction.account_id, trade_date=transaction.trade_date, type='BUY', stock=symbol,
        quantity=units, price=price, amount=(units * price).quantize(Decimal('0.01')), fee=0,
        currency=currency,
        notes=REINVEST_NOTE_PREFIX.format(price=price_text) + (transaction.notes or ''),
    ))
    return True
