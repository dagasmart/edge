#!/usr/bin/env bash
#
# EdgeAuth Swow Health Check & Alert Script
# 用法: ./edge-health-check.sh [--daemon] [--interval 5]
#

set -euo pipefail

# =========================================================
# 配置区
# =========================================================
HEALTH_URL="http://127.0.0.1:9503/admin-api/extension/edge/health"
METRICS_URL="http://127.0.0.1:9503/admin-api/extension/edge/metrics"
PORT=9503
TIMEOUT=3
INTERVAL=5
LOG_DIR="/var/log/edge-auth"
LOG_FILE="${LOG_DIR}/health-check.log"
STATE_FILE="${LOG_DIR}/health-check.state"

# 告警阈值
ALERT_CONSECUTIVE_FAIL=3          # 连续失败 N 次才告警（避免瞬时抖动）
ALERT_REDIS_POOL_LOW_PCT=20       # Redis 池空闲低于 20% 告警
ALERT_DEVICE_EXCEEDED_THRESHOLD=10 # 设备限流触发次数阈值（单次检查增量）

# 告警通道开关
ALERT_SYSLOG=true
ALERT_STDERR=true
ALERT_WEBHOOK=false
WEBHOOK_URL="${WEBHOOK_URL:-}"

# 恢复通知
NOTIFY_RECOVERY=true

# =========================================================
# 初始化
# =========================================================
mkdir -p "$LOG_DIR"

# 日志函数（带轮转：>50MB 自动截断）
log() {
    local level="$1"
    shift
    local msg="[$(date '+%F %T')] [$level] $*"
    echo "$msg" >> "$LOG_FILE"

    # 日志大小检查
    if [ -f "$LOG_FILE" ] && [ "$(stat -c%s "$LOG_FILE" 2>/dev/null || echo 0)" -gt 52428800 ]; then
        mv "$LOG_FILE" "${LOG_FILE}.old"
        touch "$LOG_FILE"
    fi

    if [ "$ALERT_STDERR" = true ]; then
        echo "$msg" >&2
    fi
}

# 状态持久化
load_state() {
    if [ -f "$STATE_FILE" ]; then
        # shellcheck source=/dev/null
        source "$STATE_FILE"
    else
        CONSECUTIVE_FAILS=0
        LAST_ALERT_TIME=0
        LAST_REDIS_FREE=-1
        LAST_DEVICE_EXCEEDED=-1
        HEALTH_OK_SINCE=""
    fi
}

save_state() {
    cat > "$STATE_FILE" <<EOF
CONSECUTIVE_FAILS=$CONSECUTIVE_FAILS
LAST_ALERT_TIME=$LAST_ALERT_TIME
LAST_REDIS_FREE=$LAST_REDIS_FREE
LAST_DEVICE_EXCEEDED=$LAST_DEVICE_EXCEEDED
HEALTH_OK_SINCE=$HEALTH_OK_SINCE
EOF
}

# =========================================================
# 告警函数
# =========================================================
send_alert() {
    local title="$1"
    local body="$2"
    local severity="${3:-WARNING}"

    local full_msg="${title} | ${body}"

    # syslog
    if [ "$ALERT_SYSLOG" = true ]; then
        logger -t "edge-health" -p "user.${severity,,}" "$full_msg" 2>/dev/null || true
    fi

    # webhook（可选）
    if [ "$ALERT_WEBHOOK" = true ] && [ -n "$WEBHOOK_URL" ]; then
        curl -s -X POST "$WEBHOOK_URL" \
            -H "Content-Type: application/json" \
            --max-time 5 \
            -d "{\"title\":\"${title}\",\"body\":\"${body}\",\"severity\":\"${severity}\",\"host\":\"$(hostname)\",\"time\":\"$(date -Iseconds)\"}" \
            >/dev/null 2>&1 || true
    fi

    LAST_ALERT_TIME=$(date +%s)
}

send_recovery() {
    local body="$1"
    send_alert "EdgeAuth RECOVERED" "$body" "INFO"
    log "INFO" "Service recovered: $body"
}

# =========================================================
# 核心检查逻辑
# =========================================================
check_health() {
    local http_code body exit_code=0

    # ---- 1. HTTP 可达性 ----
    http_code=$(curl -s -o /tmp/edge_health_body.$$ -w '%{http_code}' \
        --max-time "$TIMEOUT" "$HEALTH_URL" 2>/dev/null) || true

    if [ "$http_code" != "200" ]; then
        body="health endpoint returned HTTP $http_code (expected 200)"
        log "ERROR" "$body"
        return 1
    fi

    # 解析 JSON
    local status redis_total redis_free cache_size
    status=$(jq -r '.status // empty' /tmp/edge_health_body.$$ 2>/dev/null || echo "")
    redis_total=$(jq -r '.redis.total // empty' /tmp/edge_health_body.$$ 2>/dev/null || echo "0")
    redis_free=$(jq -r '.redis.free // empty' /tmp/edge_health_body.$$ 2>/dev/null || echo "0")
    cache_size=$(jq -r '.cache.size // empty' /tmp/edge_health_body.$$ 2>/dev/null || echo "0")

    rm -f /tmp/edge_health_body.$$

    # ---- 2. 业务状态 ----
    if [ "$status" != "ok" ]; then
        body="health.status='$status' (expected 'ok')"
        log "ERROR" "$body"
        return 1
    fi

    # ---- 3. Redis 池水位告警 ----
    if [ "$redis_total" -gt 0 ] 2>/dev/null; then
        local free_pct=$(( redis_free * 100 / redis_total ))
        LAST_REDIS_FREE=$redis_free

        if [ "$free_pct" -lt "$ALERT_REDIS_POOL_LOW_PCT" ]; then
            body="Redis pool low: free=${redis_free}/${redis_total} (${free_pct}%)"
            log "WARN" "$body"
            send_alert "EdgeAuth Redis Pool Low" "$body" "WARNING"
        fi
    fi

    # ---- 4. 端口存活兜底检查 ----
    if ! ss -tlnp 2>/dev/null | grep -q ":${PORT} "; then
        body="Port ${PORT} NOT listening (process may have crashed)"
        log "ERROR" "$body"
        return 1
    fi

    # ---- 5. Metrics 异常指标（非阻断，仅告警）----
    local metrics_body
    metrics_body=$(curl -s --max-time "$TIMEOUT" "$METRICS_URL" 2>/dev/null) || true

    if [ -n "$metrics_body" ]; then
        local device_exceeded redis_exhausted auth_fail
        device_exceeded=$(echo "$metrics_body" | jq -r '.device_concurrency_exceeded // 0' 2>/dev/null || echo "0")
        redis_exhausted=$(echo "$metrics_body" | jq -r '.redis_pool_exhausted // 0' 2>/dev/null || echo "0")
        auth_fail=$(echo "$metrics_body" | jq -r '.auth_fail // 0' 2>/dev/null || echo "0")

        # 设备限流突增检测
        if [ "$LAST_DEVICE_EXCEEDED" -ge 0 ] 2>/dev/null; then
            local delta=$(( device_exceeded - LAST_DEVICE_EXCEEDED ))
            if [ "$delta" -ge "$ALERT_DEVICE_EXCEEDED_THRESHOLD" ]; then
                body="Device concurrency exceeded spike: +${delta} (total=${device_exceeded})"
                log "WARN" "$body"
                send_alert "EdgeAuth Device Limit Spike" "$body" "WARNING"
            fi
        fi
        LAST_DEVICE_EXCEEDED=$device_exceeded

        # Redis 池枯竭
        if [ "$redis_exhausted" -gt 0 ] 2>/dev/null && [ "$redis_exhausted" -ne "$LAST_REDIS_EXHAUSTED" ] 2>/dev/null; then
            body="Redis pool exhausted count > 0 (${redis_exhausted})"
            log "WARN" "$body"
            send_alert "EdgeAuth Redis Exhausted" "$body" "WARNING"
        fi
    fi

    log "INFO" "health=ok redis_free=${redis_free}/${redis_total} cache=${cache_size}"
    return 0
}

# =========================================================
# 主循环
# =========================================================
main() {
    local daemon_mode=false

    while [[ $# -gt 0 ]]; do
        case "$1" in
            --daemon)   daemon_mode=true; shift ;;
            --interval) INTERVAL="$2"; shift 2 ;;
            *)          shift ;;
        esac
    done

    load_state

    log "INFO" "=== EdgeAuth Health Check started (daemon=$daemon_mode, interval=${INTERVAL}s) ==="

    while true; do
        if check_health; then
            # 成功
            if [ "$CONSECUTIVE_FAILS" -ge "$ALERT_CONSECUTIVE_FAIL" ]; then
                # 从故障中恢复
                if [ "$NOTIFY_RECOVERY" = true ]; then
                    send_recovery "Service back to normal after ${CONSECUTIVE_FAILS} failures"
                fi
            fi
            CONSECUTIVE_FAILS=0
            HEALTH_OK_SINCE=$(date +%s)
        else
            # 失败
            CONSECUTIVE_FAILS=$((CONSECUTIVE_FAILS + 1))
            log "WARN" "Consecutive failures: ${CONSECUTIVE_FAILS}/${ALERT_CONSECUTIVE_FAIL}"

            if [ "$CONSECUTIVE_FAILS" -eq "$ALERT_CONSECUTIVE_FAIL" ]; then
                # 首次达到阈值，发告警
                local alert_body="EdgeAuth health FAILED ${CONSECUTIVE_FAILS} times consecutively"
                if [ "$(date +%s)" -gt "$((LAST_ALERT_TIME + 300))" ]; then
                    # 5 分钟内不重复告警
                    send_alert "EdgeAuth Health CRITICAL" "$alert_body" "CRITICAL"
                fi
            fi

            # 连续失败过多，建议重启
            if [ "$CONSECUTIVE_FAILS" -ge 10 ]; then
                log "CRIT" "10+ consecutive failures, suggesting worker restart"
                send_alert "EdgeAuth 10+ FAILS" "10 consecutive health failures, manual intervention needed" "CRITICAL"
            fi
        fi

        save_state

        if [ "$daemon_mode" = false ]; then
            # 单次运行模式
            break
        fi

        sleep "$INTERVAL"
    done
}

main "$@"
