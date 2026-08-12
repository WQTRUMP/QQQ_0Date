#!/usr/bin/env bash
# [INPUT]: 依赖仓库 Python 环境、私有 .env.binance 与 python.binance_trading.main
# [OUTPUT]: 对外提供默认 paper、仅允许 paper/testnet 的单一 Binance 启动命令
# [POS]: 项目唯一进程入口；不分流其他产品，不创建宽权限运行文件
# [PROTOCOL]: 变更时更新此头部，然后检查 CLAUDE.md

set -euo pipefail
umask 077

PROJECT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$PROJECT_DIR"

usage() {
    printf 'Usage: %s [paper|testnet]\n' "$0"
}

if [ "$#" -gt 1 ]; then
    usage >&2
    exit 2
fi

MODE="${1:-paper}"
case "$MODE" in
    paper|testnet)
        ;;
    -h|--help)
        usage
        exit 0
        ;;
    *)
        printf 'Unsupported mode: %s\n' "$MODE" >&2
        usage >&2
        exit 2
        ;;
esac

if [ -x "$PROJECT_DIR/.venv/bin/python" ]; then
    PYTHON_BIN="$PROJECT_DIR/.venv/bin/python"
else
    PYTHON_BIN="${PYTHON_BIN:-python3}"
fi

export PYTHONDONTWRITEBYTECODE=1
export PYTHONPATH="$PROJECT_DIR${PYTHONPATH:+:$PYTHONPATH}"

exec "$PYTHON_BIN" -m python.binance_trading.main "$MODE" --env-file "$PROJECT_DIR/.env.binance"
