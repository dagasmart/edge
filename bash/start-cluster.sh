#!/bin/bash
# EdgeAuth Swow 多实例集群启动脚本
# 用法: bash extensions/edge/bin/start-cluster.sh [实例数，默认4]

INSTANCE_COUNT=${1:-4}
BASE_PORT=9503
PHP_BIN=$(which php)
ARTISAN="php artisan edge:auth-swow"

echo "========================================"
echo " EdgeAuth Swow Cluster"
echo " 实例数: ${INSTANCE_COUNT}"
echo " 端口范围: ${BASE_PORT} ~ $((BASE_PORT + INSTANCE_COUNT - 1))"
echo "========================================"

# 停掉旧进程
echo "停止旧实例..."
for port in $(seq $BASE_PORT $((BASE_PORT + INSTANCE_COUNT - 1))); do
    fuser -k ${port}/tcp 2>/dev/null
done
sleep 1

# 启动新实例
for i in $(seq 0 $((INSTANCE_COUNT - 1))); do
    PORT=$((BASE_PORT + i))
    echo "启动实例 $((i+1))/${INSTANCE_COUNT} → 端口 ${PORT}"

    # 每个实例用独立的环境变量区分
    AUTH_PORT=${PORT} \
    REDIS_DB=2 \
    MAX_CONCURRENCY=10000 \
    REDIS_POOL_SIZE=256 \
    $PHP_BIN artisan edge:auth-swow \
        >> /var/log/edge-auth/instance-${PORT}.log 2>&1 &

    PID=$!
    echo "  PID: ${PID}, 日志: /var/log/edge-auth/instance-${PORT}.log"
    sleep 0.3  # 错开启动，避免 Redis 连接风暴
done

# 等一下确认端口都起来了
sleep 2
echo ""
echo "端口监听状态:"
for port in $(seq $BASE_PORT $((BASE_PORT + INSTANCE_COUNT - 1))); do
    if ss -tlnp | grep -q ":${port} "; then
        echo "  ✅ :${port} — LISTEN"
    else
        echo "  ❌ :${port} — NOT LISTENING"
    fi
done

echo ""
echo "集群启动完成。查看日志: tail -f /var/log/edge-auth/instance-*.log"
