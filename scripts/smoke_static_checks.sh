#!/bin/bash
set -euo pipefail
cd "$(dirname "$0")/.."

echo "[smoke] python compileall"
python3 -m compileall python

echo "[smoke] dashboard html present"
test -f QQQ_0DTE_Dashboard.html

echo "[smoke] docker compose commands wired"
python3 - <<'PY'
from pathlib import Path
text = Path("docker-compose.yml").read_text(encoding="utf-8")
required = [
    'command: ["python", "python/longbridge_gateway/main.py"]',
    'command: ["python", "python/dashboard_bridge/main.py"]',
    'command: ["python", "python/longbridge_executor/main.py"]',
    '/healthz',
]
missing = [item for item in required if item not in text]
if missing:
    raise SystemExit(f"missing compose wiring: {missing}")
PY

echo "[smoke] config example includes qqq fields"
python3 - <<'PY'
from pathlib import Path
text = Path("configs/runtime.env.example").read_text(encoding="utf-8")
required = [
    "EXECUTION_MODE=paper",
    "DAILY_BARS_FILE=data/daily_bars_qqq.json",
    "DASHBOARD_HTML=./QQQ_0DTE_Dashboard.html",
    "ORDER_INTENT_SUBJECT=order.intent.option.>",
]
missing = [item for item in required if item not in text]
if missing:
    raise SystemExit(f"missing env fields: {missing}")
PY

echo "[smoke] subject docs aligned"
python3 - <<'PY'
from pathlib import Path
import re
targets = [
    Path("README.md"),
    Path("docs/INTERFACES.md"),
    Path("python/longbridge_gateway/main.py"),
]
for path in targets:
    text = path.read_text(encoding="utf-8")
    if re.search(r"quote\.option\.qqq(?:[^a-zs]|$)", text):
        raise SystemExit(f"stale subject doc in {path}")
PY

echo "[smoke] ok"
