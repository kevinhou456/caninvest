"""
请求级查询记忆（request-scoped memo）

用途：同一个请求（Flask app context）里，相同的只读查询只执行一次。
例如一个页面要算几百个“账户×股票×日期”的持仓，节假日表、价格历史、股票信息
会被反复查询成百上千次，结果却完全一样。

正确性保证：
  - 记忆只活在当前 app context（flask.g）里，请求结束即丢弃，不跨请求、不跨进程。
  - 会话里发生任何写操作（flush / commit / rollback / ORM 批量 update/delete/insert）
    就清空全部记忆，下一次查询重新读库。所以不会读到“本请求自己刚改过之前”的旧数据。
  - 没有 app context 时（纯脚本环境）直接执行查询，不做记忆。

只用于结果可以安全复用的“读”，调用方拿到的值不要原地修改。
"""
from typing import Any, Callable, Hashable

from flask import g, has_app_context
from sqlalchemy import event
from sqlalchemy.orm import Session

_G_KEY = '_query_memo_store'


def memo(namespace: str, key: Hashable, loader: Callable[[], Any]) -> Any:
    """返回 namespace/key 对应的记忆值；没有则调用 loader() 并记住结果。"""
    if not has_app_context():
        return loader()
    store = g.get(_G_KEY)
    if store is None:
        store = {}
        setattr(g, _G_KEY, store)
    bucket = store.setdefault(namespace, {})
    if key in bucket:
        return bucket[key]
    value = loader()
    bucket[key] = value
    return value


def bump(namespace: str, key: Hashable) -> int:
    """计数器：返回本请求内 namespace/key 被调用的次数（含本次）。无 app context 时恒为 1。"""
    if not has_app_context():
        return 1
    counts = memo(namespace, '__counts__', dict)
    counts[key] = counts.get(key, 0) + 1
    return counts[key]


def peek(namespace: str, key: Hashable, default: Any = None) -> Any:
    """只读取已有的记忆值，不触发加载。"""
    if not has_app_context():
        return default
    store = g.get(_G_KEY) or {}
    return store.get(namespace, {}).get(key, default)


def clear_all() -> None:
    if has_app_context() and g.get(_G_KEY):
        setattr(g, _G_KEY, {})


# ---- 写操作时清空记忆 -------------------------------------------------------

@event.listens_for(Session, 'after_flush')
def _on_flush(session, _flush_context):
    if session.new or session.dirty or session.deleted:
        clear_all()


@event.listens_for(Session, 'after_commit')
def _on_commit(_session):
    clear_all()


@event.listens_for(Session, 'after_soft_rollback')
def _on_rollback(_session, _previous_transaction):
    clear_all()


@event.listens_for(Session, 'do_orm_execute')
def _on_orm_execute(orm_execute_state):
    if orm_execute_state.is_insert or orm_execute_state.is_update or orm_execute_state.is_delete:
        clear_all()
