#!/usr/bin/env bash
# EdgeAuth 集群停止（artisan 方式）
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
        echo "  :${port} → PID ${pid} → SIGTERM"
        kill -TERM "$pid" 2>/dev/null || true
    else
        # 兜底：从端口找
        pid=$(ss -tlnp 2>/dev/null | grep ":${port} " | grep -oP 'pid=\K[0-9]+' | head -1)
        if [[ -n "$pid" ]]; then
            echo "  :${port} → PID ${pid} (无pid文件) → SIGTERM"
            kill -TERM "$pid" 2>/dev/null || true
        else
            echo "  :${port} → 未运行"
            fuser -k "${port}/tcp" 2>/dev/null || true
        fi
    fi
done

# 等待退出
echo "等待进程退出…"
for w in $(seq 1 10); do
    remaining=0
    for i in $(seq 0 $((INSTANCE_COUNT-1))); do
        port=$((BASE_PORT+i))
        pf="${PID_DIR}/${APP_NAME}.${port}.pid"
        [[ -f "$pf" ]] && kill -0 "$(cat "$pf")" 2>/dev/null && remaining=$((remaining+1))
    done
    [[ $remaining -eq 0 ]] && { echo "✅ 全部停止"; break; }
    echo "  ${remaining} 个仍在退出… (${w}/10)"
    sleep 1
done

# SIGKILL 兜底
for i in $(seq 0 $((INSTANCE_COUNT-1))); do
    port=$((BASE_PORT+i))
    pf="${PID_DIR}/${APP_NAME}.${port}.pid"
    if [[ -f "$pf" ]] && kill -0 "$(cat "$pf")" 2>/dev/null; then
        pid=$(cat "$pf")
        echo "  ⚠️ :${port} → SIGKILL"
        kill -KILL "$pid" 2>/dev/null || true
        fuser -k -9 "${port}/tcp" 2>/dev/null || true
    fi
    rm -f "$pf"
done

# 清理孤儿 artisan 进程
ORPHAN=$(ps aux | grep 'edge:auth-swow' | grep -v grep | awk '{print $2}')
if [[ -n "$ORPHAN" ]]; then
    echo "清理孤儿 artisan 进程…"
    echo "$ORPHAN" | xargs kill -TERM 2>/dev/null || true
    sleep 1
    ORPHAN=$(ps aux | grep 'edge:auth-swow' | grep -v grep | awk '{print $2}')
    [[ -n "$ORPHAN" ]] && echo "$ORPHAN" | xargs kill -9 2>/dev/null || true
fi

echo "✅ 集群已停止"
