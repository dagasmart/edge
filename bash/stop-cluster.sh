#!/usr/bin/env bash
# ============================================================
#  EdgeAuth 集群停止（artisan 方式）
#  被 systemd ExecStop 调用，也可手动 / 应急使用
# ============================================================
set -Eeuo pipefail

APP_NAME="edge-auth"
PID_DIR="/var/run/${APP_NAME}"
BASE_PORT=9503
INSTANCE_COUNT=${1:-4}

echo "========================================"
echo " EdgeAuth — 停止"
echo "========================================"

for i in $(seq 0 $((INSTANCE_COUNT-1))); do
    port=$((BASE_PORT+i))
    pf="${PID_DIR}/${APP_NAME}.${port}.pid"

    if [[ -f "$pf" ]] && kill -0 "$(cat "$pf")" 2>/dev/null; then
        pid=$(cat "$pf")
        echo "  :${port} → SIGTERM PID ${pid}"
        kill -TERM "$pid" 2>/dev/null || true
        for w in $(seq 1 10); do
            kill -0 "$pid" 2>/dev/null || { echo "    ✅ 已退出"; break; }
            sleep 0.5
        done
        if kill -0 "$pid" 2>/dev/null; then
            echo "    ⚠️ SIGKILL ${pid}"
            kill -KILL "$pid" 2>/dev/null || true
        fi
        rm -f "$pf"
    else
        # 兜底：按端口清理可能残留的孤儿进程
        echo "  :${port} → 无 pid 文件，尝试按端口清理"
        fuser -k "${port}/tcp" 2>/dev/null || true
        rm -f "$pf"
    fi
done

echo "完成。"
