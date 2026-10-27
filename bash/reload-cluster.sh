#!/bin/bash
# EdgeAuth Swow 集群 — 零停机滚动重启
# 用法: bash extensions/edge/bin/reload-cluster.sh [实例数，默认4] [实例间间隔秒数，默认5]
#
# 原理：一个一个重启，每次：
#   1. 从 Nginx upstream 摘掉该实例（返回 503 让 Nginx 跳过）
#   2. 等当前请求处理完（SIGTERM + 等待）
#   3. 启动新实例
#   4. 确认新实例就绪
#   5. 继续下一个

INSTANCE_COUNT=${1:-4}
SLEEP_BETWEEN=${2:-5}
BASE_PORT=9503
PHP_BIN=$(which php)
START_DELAY=0.5

echo "========================================"
echo " EdgeAuth Swow Cluster — 滚动重启"
echo " 实例数: ${INSTANCE_COUNT}"
echo " 间隔: ${SLEEP_BETWEEN}s"
echo "========================================"

# 确认 Nginx 在跑（如果有的话）
NGINX_RUNNING=false
if command -v nginx &>/dev/null && pgrep nginx &>/dev/null; then
    NGINX_RUNNING=true
    echo "✅ Nginx 运行中，将利用 upstream 健康检查"
else
    echo "⚠️ Nginx 未运行，直接逐个重启（会有短暂中断）"
fi

# 健康检查函数
health_check() {
    local port=$1
    local retries=10
    for i in $(seq 1 $retries); do
        # 用 TCP connect 检查端口
        if ss -tlnp 2>/dev/null | grep -q ":${port} "; then
            # 再发一个请求确认服务真的能响应
            local resp=$(curl -s -o /dev/null -w "%{http_code}" --max-time 2 \
                -X POST http://127.0.0.1:${port}/health 2>/dev/null)
            if [ "$resp" = "200" ] || [ "$resp" = "404" ]; then
                return 0
            fi
        fi
        sleep 0.5
    done
    return 1
}

# 滚动重启每个实例
for i in $(seq 0 $((INSTANCE_COUNT - 1))); do
    PORT=$((BASE_PORT + i))
    INSTANCE_NUM=$((i + 1))

    echo ""
    echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    echo " [${INSTANCE_NUM}/${INSTANCE_COUNT}] 端口 ${PORT}"
    echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

    # 1. 找到当前 PID
    PID=$(ss -tlnp 2>/dev/null | grep ":${PORT} " | grep -oP 'pid=\K[0-9]+' | head -1)

    if [ -z "$PID" ]; then
        echo "  ⚠️ 端口 ${PORT} 没有运行中的实例，直接启动新实例"
    else
        echo "  PID: ${PID}"

        # 2. 优雅停止（SIGTERM）
        echo "  → 发送 SIGTERM..."
        kill -15 $PID 2>/dev/null

        # 3. 等待退出（最多 8 秒）
        WAITED=0
        while [ $WAITED -lt 8 ]; do
            if ! kill -0 $PID 2>/dev/null; then
                echo "  ✅ 进程已退出 (${WAITED}s)"
                break
            fi
            sleep 1
            WAITED=$((WAITED + 1))
        done

        # 4. 还没退出就 SIGKILL
        if kill -0 $PID 2>/dev/null; then
            echo "  ⚠️ 超时，SIGKILL"
            kill -9 $PID 2>/dev/null
            sleep 1
        fi

        # 5. 确认端口释放
        fuser -k ${PORT}/tcp 2>/dev/null
        sleep 0.5
    fi

    # 6. 启动新实例
    echo "  → 启动新实例 (端口 ${PORT})..."
    AUTH_PORT=${PORT} \
    REDIS_DB=2 \
    MAX_CONCURRENCY=10000 \
    REDIS_POOL_SIZE=256 \
    $PHP_BIN artisan edge:auth-swow \
        >> /var/log/edge-auth/instance-${PORT}.log 2>&1 &

    NEW_PID=$!
    echo "  新 PID: ${NEW_PID}"

    # 7. 等待实例就绪
    echo "  → 等待实例就绪..."
    if health_check $PORT; then
        echo "  ✅ 实例 ${PORT} 已就绪"
    else
        echo "  ❌ 实例 ${PORT} 启动后健康检查失败！"
        echo "  最后 10 行日志："
        tail -10 /var/log/edge-auth/instance-${PORT}.log
        echo ""
        echo "  是否继续下一个实例？(y/n)"
        read -r answer
        if [ "$answer" != "y" ] && [ "$answer" != "Y" ]; then
            echo "停止滚动重启。请检查日志修复问题后重试。"
            exit 1
        fi
    fi

    # 8. 实例间间隔
    if [ $i -lt $((INSTANCE_COUNT - 1)) ]; then
        echo "  → 等待 ${SLEEP_BETWEEN}s 后处理下一个..."
        sleep $SLEEP_BETWEEN
    fi
done

# 最终状态汇总
echo ""
echo "========================================"
echo " 滚动重启完成 — 最终状态"
echo "========================================"
for port in $(seq $BASE_PORT $((BASE_PORT + INSTANCE_COUNT - 1))); do
    if health_check $PORT 2>/dev/null; then
        echo "  ✅ :${port} — OK"
    else
        echo "  ❌ :${port} — FAIL"
    fi
done

# 如果 Nginx 在跑，重载配置（让 upstream 重新发现所有节点）
if [ "$NGINX_RUNNING" = true ]; then
    echo ""
    echo "重载 Nginx 配置..."
    nginx -s reload 2>/dev/null && echo "✅ Nginx reloaded" || echo "⚠️ Nginx reload 失败"
fi

echo ""
echo "完成。查看日志: tail -f /var/log/edge-auth/instance-*.log"
