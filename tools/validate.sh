#!/usr/bin/env bash
# Единая точка локальной валидации проекта. Запускай перед коммитом:
#   bash tools/validate.sh
# 1) php -l на серверные скрипты.
# 2) node --check на извлечённые inline-скрипты из index.html.
set -e

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

echo "== php -l =="
php -l leads-proxy.php
php -l cron_update.php

echo "== extracting inline scripts from index.html =="
python3 - <<'PY'
import re
html = open('index.html').read()
scripts = re.findall(r'<script[^>]*>(.*?)</script>', html, flags=re.DOTALL)
open('/tmp/leads-inline.js', 'w').write('\n;\n'.join(s for s in scripts if s.strip()))
print('inline scripts:', len([s for s in scripts if s.strip()]))
PY

echo "== node --check /tmp/leads-inline.js =="
node --check /tmp/leads-inline.js

echo "OK"
