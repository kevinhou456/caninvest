"""
对比两份 export_baseline.py 的输出，逐字段列出差异。

  venv/bin/python tests/baseline/compare_baseline.py old.json new.json [--prefix stable/] [--tol 1e-6]

规则：
  - 数值允许 tol 的绝对误差（浮点舍入），其余必须完全相同
  - 列表按顺序比较（顺序变化也算差异）
  - 自动忽略时间戳类字段（见 IGNORED_KEYS）
退出码：0 = 完全一致，1 = 有差异
"""
import argparse
import json
import math
import sys

IGNORED_KEYS = {
    'generated_at', 'cached_at', 'created_at', 'updated_at', 'last_updated', 'price_updated_at',
    'timestamp', 'request_time', 'last_updated_at', 'from_cache', 'cache_hit', 'cached', 'computed_at', 'elapsed', 'duration',
}


def diff(a, b, path, tol, out):
    if isinstance(a, bool) or isinstance(b, bool):
        if a != b:
            out.append((path, a, b))
        return
    if isinstance(a, (int, float)) and isinstance(b, (int, float)):
        if not (math.isclose(a, b, abs_tol=tol, rel_tol=1e-12) or (a != a and b != b)):
            out.append((path, a, b))
        return
    if isinstance(a, dict) and isinstance(b, dict):
        for k in sorted(set(a) | set(b)):
            if k in IGNORED_KEYS:
                continue
            if k not in a:
                out.append((f'{path}.{k}', '<缺失>', b[k]))
            elif k not in b:
                out.append((f'{path}.{k}', a[k], '<缺失>'))
            else:
                diff(a[k], b[k], f'{path}.{k}', tol, out)
        return
    if isinstance(a, list) and isinstance(b, list):
        if len(a) != len(b):
            out.append((f'{path}[len]', len(a), len(b)))
        for i, (x, y) in enumerate(zip(a, b)):
            diff(x, y, f'{path}[{i}]', tol, out)
        return
    if a != b:
        out.append((path, a, b))


def short(v, n=80):
    s = json.dumps(v, ensure_ascii=False) if not isinstance(v, str) else v
    return s if len(s) <= n else s[:n] + '…'


def main():
    p = argparse.ArgumentParser()
    p.add_argument('old')
    p.add_argument('new')
    p.add_argument('--prefix', default='', help='只比较以此开头的条目，如 stable/')
    p.add_argument('--tol', type=float, default=1e-6)
    p.add_argument('--max', type=int, default=200, help='最多打印多少条差异')
    p.add_argument('--ignore-key', action='append', default=[], help='额外忽略的字段名（可多次指定）')
    args = p.parse_args()
    IGNORED_KEYS.update(args.ignore_key)

    old_doc = json.load(open(args.old, encoding='utf-8'))
    new_doc = json.load(open(args.new, encoding='utf-8'))
    for name, doc in (('旧', old_doc), ('新', new_doc)):
        m = doc.get('manifest', {})
        if m.get('network') == 'fake' and m.get('data_changed') is not None:
            continue  # 模拟联网：数据变化是预期的，新旧双方面对同一个确定性“网络”
        if m.get('network') or m.get('data_changed', None) is not False:
            print(f'!!! {name}文件不是“断网且数据未变”的导出（network={m.get("network")}, '
                  f'data_changed={m.get("data_changed")}），不能作为可信对比。')
            sys.exit(3)
    old, new = old_doc['results'], new_doc['results']
    keys = sorted(k for k in set(old) | set(new) if k.startswith(args.prefix) and not k.startswith('__meta_today'))

    diffs = []
    errors = 0
    for k in keys:
        if k not in old or k not in new:
            diffs.append((k, '<缺失>' if k not in old else '存在', '<缺失>' if k not in new else '存在'))
            continue
        for side in (old[k], new[k]):
            if isinstance(side, dict) and '__error__' in side:
                errors += 1
        diff(old[k], new[k], k, args.tol, diffs)

    print(f'比较条目: {len(keys)}，差异字段: {len(diffs)}，含错误记录的条目: {errors}')
    for path, a, b in diffs[:args.max]:
        print(f'  {path}\n      旧: {short(a)}\n      新: {short(b)}')
    if len(diffs) > args.max:
        print(f'  ……另有 {len(diffs) - args.max} 处差异未显示')
    sys.exit(1 if diffs else 0)


if __name__ == '__main__':
    main()
