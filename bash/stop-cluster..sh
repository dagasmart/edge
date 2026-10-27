#!/bin/bash
# EdgeAuth Swow 集群停止脚本
# 用法: bash extensions/edge/bin/stop-cluster.sh [实例数，默认4]

INSTANCE_COUNT=${1:-4}
BASE_PORT=9503

echo "========================================"
echo " EdgeAuth Swow Cluster — 停止"
echo " 实例数: ${INSTANCE_COUNT}"
echo " 端口范围: ${BASE_PORT} ~ $((BASE_PORT + INSTANCE_COUNT - 1))"
echo "========================================"

# 先尝试优雅停止（SIGTERM）
for port in $(seq $BASE_PORT $((BASE_PORT + INSTANCE_COUNT - 1))); do
    PID=$(ss -tlnp 2>/dev/null | grep ":${port} " | grep -oP 'pid=\K[0-9]+' | head -1)
    if [ -n "$PID" ]; then
        echo "端口 ${port} → PID ${PID} → 发送 SIGTERM"
        kill -15 $PID 2>/dev/null
    else
        echo "端口 ${port} → 没有进程在监听"
        # 兜底：fuser 强杀
        fuser -k ${port}/tcp 2>/dev/null
    fi
done

# 等待最多 10 秒让进程自己退出
echo "等待进程退出..."
for i in $(seq 1 10); do
    REMAINING=0
    for port in $(seq $BASE_PORT $((BASE_PORT + INSTANCE_COUNT - 1))); do
        if ss -tlnp 2>/dev/null | grep -q ":${port} "; then
            REMAINING=$((REMAINING + 1))
        fi
    done
    if [ $REMAINING -eq 0 ]; then
        echo "✅ 所有实例已停止"
        break
    fi
    echo "  还有 ${REMAINING} 个实例在退出中... (${i}/10)"
    sleep 1
done

# 10 秒后还没退出，强制 SIGKILL
REMAINING=0
for port in $(seq $BASE_PORT $((BASE_PORT + INSTANCE_COUNT - 1))); do
    if ss -tlnp 2>/dev/null | grep -q ":${port} "; then
        REMAINING=$((REMAINING + 1))
        PID=$(ss -tlnp 2>/dev/null | grep ":${port} " | grep -oP 'pid=\K[0-9]+' | head -1)
        if [ -n "$PID" ]; then
            echo "⚠️ 端口 ${port} (PID ${PID}) 超时未退出 → SIGKILL"
            kill -9 $PID 2>/dev/null
        fi
        fuser -k -9 ${port}/tcp 2>/dev/null
    fi
done

if [ $REMAINING -eq 0 ]; then
    echo "✅ 集群已完全停止"
else
    echo "❌ 仍有实例未停止，请手动检查"
fi

# 清理可能残留的 PHP 进程
ORPHAN=$(ps aux | grep 'edge:auth-swow' | grep -v grep | awk '{print $2}')
if [ -n "$ORPHAN" ]; then
    echo "清理残留进程: $ORPHAN"
    echo "$ORPHAN" | xargs kill -9 2>/dev/null
fi

echo "完成。"
