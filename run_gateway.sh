#!/bin/bash
# Longbridge Gateway wrapper
set -euo pipefail
PROJECT_DIR="$(cd "$(dirname "$0")" && pwd)"
if [ ! -f "${PROJECT_DIR}/.env.longbridge" ]; then
    echo ".env.longbridge not found under ${PROJECT_DIR}" >&2
    exit 1
fi
set -a
source "${PROJECT_DIR}/.env.longbridge"
set +a
exec "${PROJECT_DIR}/target/longbridge_gateway"
