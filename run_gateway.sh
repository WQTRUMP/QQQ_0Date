#!/bin/bash
# Longbridge Gateway wrapper
PROJECT_DIR="/Users/xncool/Desktop/QQQ_Single"
set -a
source "${PROJECT_DIR}/.env.longbridge"
set +a
exec "${PROJECT_DIR}/target/longbridge_gateway"
