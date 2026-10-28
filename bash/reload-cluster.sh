#!/usr/bin/env bash
# ============================================================
#  EdgeAuth 集群滚动重启（artisan 方式）
#  用法：
#    reload-cluster.sh              # 默认 4 实例，端口 9503~9506，间隔 3s
#    reload-cluster.sh 4 5          # 4 实例，间隔 5s
#    reload-cluster.sh 9503 9504    # 只滚动重载指定端口
# ============================================================
set -Eeuo pipefail

PHP_BIN="/xp/server/php/php-8.4/bin/php"
BASE_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"
APP_NAME="edge-auth"
LOG_DIR="/var/log/${APP_NAME}"
PID_DIR="/var/run/${APP_NAME}"
POOL_SIZE=256

# ★ 对齐 deploy.sh 的环境变量（可被外部覆盖）
REDIS_HOST="${REDIS_HOST:-127.0.0.1}"
REDIS_PORT="${REDIS_PORT:-16379}"
REDIS_PASS="${REDIS_PASS:-uApDVQD7}"
REDIS_DB="${REDIS_DB:-2}"

# 解析参数：reload-cluster.sh [count [interval]]  或  reload-cluster.sh port1 port2 ...
if [[ $# -ge 1 && "$1" =~ ^[0-9]+$ ]] && { [[ $# -eq 1 ]] || [[ "$2" =~ ^[0-9]+$ ]]; }; then
    INSTANCE_COUNT=${1:-4}
    SLEEP_BETWEEN=${2:-3}
    BASE_PORT=9503
    PORTS=()
    for i in $(seq 0 $((INSTANCE_COUNT-1))); do PORTS+=($((BASE_PORT+i))); done
else
    PORTS=("$@")
    SLEEP_BETWEEN=3
fi

echo "========================================"
echo " EdgeAuth — 滚动重启 (artisan 模式)"
echo " 端口: ${PORTS[*]}"
echo " Redis: ${REDIS_HOST}:${REDIS_PORT} db=${REDIS_DB}"
echo "========================================"

health_check() {
    local port=$1
    for i in $(seq 1 10); do
        ss -tlnp 2>/dev/null | grep -q ":${port} " || { sleep 0.5; continue; }
        local resp
        resp=$(curl -s -o /dev/null -w "%{http_code}" --max-time 2 \
            -X POST http://127.0.0.1:${port}/health 2>/dev/null)
        [[ "$resp" == "200" ]] && return 0
        sleep 0.5
    done
    return 1
}

for port in "${PORTS[@]}"; do
    echo ""
    echo "━━ 端口 ${port} ━━"

    # 停旧
    pf="${PID_DIR}/${APP_NAME}.${port}.pid"
    if [[ -f "$pf" ]] && kill -0 "$(cat "$pf")" 2>/dev/null; then
        pid=$(cat "$pf")
        echo "  SIGTERM PID ${pid}"
        kill -TERM "$pid" 2>/dev/null
        for w in $(seq 1 8); do
            kill -0 "$pid" 2>/dev/null || { echo "  ✅ 已退出"; break; }
            sleep 1
        done
        kill -0 "$pid" 2>/dev/null && { echo "  ⚠️ SIGKILL"; kill -KILL "$pid" 2>/dev/null; }
        rm -f "$pf"
    else
        echo "  (无运行实例，直接启动)"
        fuser -k "${port}/tcp" 2>/dev/null || true
    fi

    # 启新（★ 补齐 Redis 环境变量）
    echo "  启动新实例…"
    cd "${BASE_DIR}"
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
    cd - >/dev/null

    # 等待就绪
    if health_check "$port"; then
        echo "  ✅ :${port} 就绪"
    else
        echo "  ❌ :${port} 健康检查失败！"
        tail -5 "${LOG_DIR}/${APP_NAME}.${port}.err" 2>/dev/null || true
        read -r -p "  继续下一个？(y/n) " ans
        [[ "$ans" =~ ^[Yy]$ ]] || { echo "停止。"; exit 1; }
    fi

    [[ "$port" != "${PORTS[-1]}" ]] && { echo "  → 等 ${SLEEP_BETWEEN}s…"; sleep "$SLEEP_BETWEEN"; }
done

echo ""
echo "========================================"
echo " 滚动重启完成"
echo "========================================"
for port in "${PORTS[@]}"; do
    health_check "$port" \
        && echo "  ✅ :${port} OK" \
        || echo "  ❌ :${port} FAIL"
done

command -v nginx &>/dev/null && pgrep nginx &>/dev/null && { nginx -s reload 2>/dev/null && echo "✅ nginx reloaded" || true; }
echo "完成。"
