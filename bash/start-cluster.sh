#!/usr/bin/env bash
# ============================================================
#  EdgeAuth 集群启动（artisan 方式）
#  被 systemd ExecStart 调用，也可手动 / 应急使用
# ============================================================
set -Eeuo pipefail

PHP_BIN="/xp/server/php/php-8.4/bin/php"
BASE_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"
APP_NAME="edge-auth"
LOG_DIR="/var/log/${APP_NAME}"
PID_DIR="/var/run/${APP_NAME}"
BASE_PORT=9503
POOL_SIZE=256
INSTANCE_COUNT=${1:-4}

# ★ 对齐 deploy.sh 的环境变量（可被外部覆盖）
REDIS_HOST="${REDIS_HOST:-127.0.0.1}"
REDIS_PORT="${REDIS_PORT:-16379}"
REDIS_PASS="${REDIS_PASS:-uApDVQD7}"
REDIS_DB="${REDIS_DB:-2}"

echo "========================================"
echo " EdgeAuth — 启动 (artisan 模式)"
echo " 实例: ${INSTANCE_COUNT}, 端口: ${BASE_PORT}~$((BASE_PORT+INSTANCE_COUNT-1))"
echo " Redis: ${REDIS_HOST}:${REDIS_PORT} db=${REDIS_DB}"
echo "========================================"

mkdir -p "$LOG_DIR" "$PID_DIR"

# 先停旧
for i in $(seq 0 $((INSTANCE_COUNT-1))); do
    port=$((BASE_PORT+i))
    pf="${PID_DIR}/${APP_NAME}.${port}.pid"
    if [[ -f "$pf" ]] && kill -0 "$(cat "$pf")" 2>/dev/null; then
        kill -TERM "$(cat "$pf")" 2>/dev/null || true
        echo "  停止旧 :${port}"
    fi
    fuser -k "${port}/tcp" 2>/dev/null || true
done
sleep 1

# 启动新
cd "${BASE_DIR}"
for i in $(seq 0 $((INSTANCE_COUNT-1))); do
    port=$((BASE_PORT+i))
    echo "  启动 :${port} …"
    AUTH_PORT="${port}" \
    REDIS_HOST="${REDIS_HOST}" \
    REDIS_PORT="${REDIS_PORT}" \
    REDIS_PASS="${REDIS_PASS}" \
    REDIS_DB="${REDIS_DB}" \
    MAX_CONCURRENCY=10000 \
    REDIS_POOL_SIZE="${POOL_SIZE}" \
    nohup "${PHP_BIN}" artisan edge:auth-swow \
        >> "${LOG_DIR}/${APP_NAME}.${port}.log" \
        2>> "${LOG_DIR}/${APP_NAME}.${port}.err" &

    echo $! > "${PID_DIR}/${APP_NAME}.${port}.pid"
    echo "    PID: $!, 日志: ${LOG_DIR}/${APP_NAME}.${port}.log"
    sleep 0.3
done
cd - >/dev/null

sleep 2
echo ""
echo "端口状态:"
for i in $(seq 0 $((INSTANCE_COUNT-1))); do
    port=$((BASE_PORT+i))
    ss -tlnp 2>/dev/null | grep -q ":${port} " \
        && echo "  ✅ :${port} LISTEN" \
        || echo "  ❌ :${port} NOT LISTENING"
done
echo "完成。"
