#!/usr/bin/env bash
# check-links.sh — README/docs/openspec 主规格的 Markdown 相对链接可达性检查。
# 用法：bash scripts/check-links.sh   （退出码 0 = 无死链）
set -u
root=$(git rev-parse --show-toplevel 2>/dev/null || pwd)
cd "$root" || exit 1
python3 - <<'PY'
import re, os, glob, urllib.parse, sys
bad = 0
files = ['README.md', 'README_CN.md'] \
    + glob.glob('docs/**/*.md', recursive=True) \
    + glob.glob('openspec/specs/**/*.md', recursive=True)
for f in files:
    base = os.path.dirname(f)
    text = open(f, encoding='utf-8').read()
    for m in re.finditer(r'\]\(([^)#]+)(?:#[^)]*)?\)', text):
        t = urllib.parse.unquote(m.group(1))
        if t.startswith(('http://', 'https://', 'mailto:')):
            continue
        p = os.path.normpath(os.path.join(base, t))
        if not os.path.exists(p):
            print(f'BROKEN: {f} -> {t}')
            bad += 1
print('死链数:', bad)
sys.exit(1 if bad else 0)
PY
