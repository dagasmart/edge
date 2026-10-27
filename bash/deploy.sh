#!/bin/bash
# EdgeAuth Swow 一键部署脚本
# 用法:
#   bash extensions/edge/bin/deploy.sh              # 默认 4 实例
#   bash extensions/edge/bin/deploy.sh 8           # 8 实例
#   bash extensions/edge/bin/deploy.sh 4 /opt/app  # 指定项目根目录
#
# CI/CD 调用:
#   bash deploy.sh 4 /var/www/ccs --skip-git
#
# 退出码: 0=成功, 1=失败（CI 可据此判断）

set -euo pipefail

# ============================================================
# 参数解析
# ============================================================
INSTANCE_COUNT=${1:-4}
PROJECT_ROOT=${2:-$(pwd)}
SKIP_GIT=false
SKIP_COMPOSER=false

for arg in "$@"; do
    case $arg in
        --skip-git) SKIP_GIT=true ;;
        --skip-composer) SKIP_COMPOSER=true ;;
    esac
done

BASE_PORT=9503
PHP_BIN=$(which php)
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
LOG_DIR="/var/log/edge-auth"
TIMESTAMP=$(date +%Y%m%d_%H%M%S)
DEPLOY_LOG="${LOG_DIR}/deploy-${TIMESTAMP}.log"

# 颜色输出
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
CYAN='\033[0;36m'
NC='\033[0m'

log()   { echo -e "${GREEN}[$(date +%H:%M:%S)]${NC} $*"; }
warn()  { echo -e "${YELLOW}[$(date +%H:%M:%S)] ⚠️${NC} $*"; }
err()   { echo -e "${RED}[$(date +%H:%M:%S)] ❌${NC} $*" >&2; }
step()  { echo -e "\n${CYAN}━━━ $* ━━━${NC}"; }

# ============================================================
# 前置检查
# ============================================================
step "前置检查"

# 日志目录
mkdir -p "$LOG_DIR"
exec > >(tee -a "$DEPLOY_LOG") 2>&1

# 确认项目根目录
if [ ! -f "${PROJECT_ROOT}/artisan" ]; then
    err "找不到 artisan 文件: ${PROJECT_ROOT}/artisan"
    err "请确认 PROJECT_ROOT 参数正确"
    exit 1
fi
log "✅ 项目根目录: ${PROJECT_ROOT}"

# 确认 PHP
if ! command -v php &>/dev/null; then
    err "PHP 未安装"
    exit 1
fi
log "✅ PHP: $(php -r 'echo PHP_VERSION;')"

# 确认端口未被其他进程占用（非本服务）
for port in $(seq $BASE_PORT $((BASE_PORT + INSTANCE_COUNT - 1))); do
    OWNER=$(ss -tlnp 2>/dev/null | grep ":${port} " | grep -oP 'users:\(\("\K[^"]+' || echo "")
    if [ -n "$OWNER" ] && [[ "$OWNER" != "php" ]]; then
        warn "端口 ${port} 被非 PHP 进程占用 (${OWNER})"
    fi
done

# 确认 Redis 可达
REDIS_HOST="127.0.0.1"
REDIS_PORT="16379"
if ! redis-cli -h $REDIS_HOST -p $REDIS_PORT -a uApDVQD7 PING 2>/dev/null | grep -q PONG; then
    warn "Redis 不可达 (${REDIS_HOST}:${REDIS_PORT})，服务将以 DEGRADED 模式启动"
else
    log "✅ Redis 可达"
fi

# 系统参数快速检查
ULIMIT_N=$(ulimit -n)
if [ "$ULIMIT_N" -lt 65535 ]; then
    warn "ulimit -n = ${ULIMIT_N}，建议 ≥ 65535"
    warn "可运行: sysctl --system && ulimit -n 65535"
fi

# ============================================================
# 1. 拉代码
# ============================================================
step "1/6 更新代码"

cd "$PROJECT_ROOT"

if [ "$SKIP_GIT" = false ]; then
    if [ -d .git ]; then
        log "拉取最新代码..."
        git fetch --all
        CURRENT_BRANCH=$(git rev-parse --abbrev-ref HEAD)
        git reset --hard origin/${CURRENT_BRANCH}
        NEW_COMMIT=$(git rev-parse --short HEAD)
        log "✅ 代码已更新到 ${NEW_COMMIT}"
    else
        warn "非 git 仓库，跳过代码更新"
    fi
else
    log "跳过 git 更新 (--skip-git)"
fi

# ============================================================
# 2. Composer 依赖
# ============================================================
step "2/6 安装依赖"

if [ "$SKIP_COMPOSER" = false ]; then
    if [ -f composer.json ]; then
        log "composer install --no-dev --optimize-autoloader..."
        composer install --no-dev --optimize-autoloader --no-interaction 2>&1 | tail -3
        log "✅ Composer 完成"
    else
        warn "没有 composer.json，跳过"
    fi
else
    log "跳过 composer (--skip-composer)"
fi

# ============================================================
# 3. 预热 Redis 数据
# ============================================================
step "3/6 预热数据"

# 确保 bench 测试 key 存在
redis-cli -h $REDIS_HOST -p $REDIS_PORT -a uApDVQD7 -n 2 SET \
    "device:secret:bench_client:bench_user" "bench_secret" &>/dev/null && \
    log "✅ bench secret 已就绪" || warn "bench secret 写入失败"

# 如果有批量预热脚本
if [ -f "${SCRIPT_DIR}/test/gen_request.php" ]; then
    log "生成签名测试..."
    php "${SCRIPT_DIR}/test/gen_request.php" >/dev/null 2>&1 || true
fi

# ============================================================
# 4. 零停机滚动重启
# ============================================================
step "4/6 滚动重启 ${INSTANCE_COUNT} 个实例"

# 调用 reload-cluster.sh
if [ -x "${SCRIPT_DIR}/reload-cluster.sh" ]; then
    bash "${SCRIPT_DIR}/reload-cluster.sh" $INSTANCE_COUNT 5
else
    warn "reload-cluster.sh 不存在或不可执行，使用 stop + start"
    bash "${SCRIPT_DIR}/stop-cluster.sh" $INSTANCE_COUNT
    sleep 2
    bash "${SCRIPT_DIR}/start-cluster.sh" $INSTANCE_COUNT
fi

# ============================================================
# 5. 健康检查
# ============================================================
step "5/6 部署后健康检查"

FAILED=0
for port in $(seq $BASE_PORT $((BASE_PORT + INSTANCE_COUNT - 1))); do
    # TCP 连通
    if ! ss -tlnp 2>/dev/null | grep -q ":${port} "; then
        err "端口 ${port} 未监听"
        FAILED=$((FAILED + 1))
        continue
    fi

    # HTTP 健康
    HTTP_CODE=$(curl -s -o /dev/null -w "%{http_code}" --max-time 3 \
        http://127.0.0.1:${port}/health 2>/dev/null || echo "000")

    if [ "$HTTP_CODE" = "200" ] || [ "$HTTP_CODE" = "404" ]; then
        log "✅ :${port} → HTTP ${HTTP_CODE}"
    else
        err "❌ :${port} → HTTP ${HTTP_CODE}"
        FAILED=$((FAILED + 1))
    fi
done

if [ $FAILED -gt 0 ]; then
    err "${FAILED} 个实例健康检查失败"
    warn "查看日志: tail -f ${LOG_DIR}/instance-*.log"
fi

# Nginx 重载
if command -v nginx &>/dev/null && pgrep nginx &>/dev/null; then
    nginx -t 2>/dev/null && nginx -s reload 2>/dev/null
    log "✅ Nginx 已重载"
fi

# ============================================================
# 6. 汇总
# ============================================================
step "6/6 部署完成"

TOTAL_QPS_ESTIMATE=$((INSTANCE_COUNT * 30000))

echo ""
echo "=========================================="
echo "  部署结果汇总"
echo "=========================================="
echo "  项目目录:    ${PROJECT_ROOT}"
echo "  实例数:      ${INSTANCE_COUNT}"
echo "  端口范围:    ${BASE_PORT} ~ $((BASE_PORT + INSTANCE_COUNT - 1))"
echo "  预估 QPS:    ~${TOTAL_QPS_ESTIMATE}/s"
echo "  部署日志:    ${DEPLOY_LOG}"
echo "  实例日志:    ${LOG_DIR}/instance-*.log"
echo "=========================================="

if [ $FAILED -eq 0 ]; then
    echo -e "  ${GREEN}✅ 全部实例健康，部署成功${NC}"
    echo ""
    echo "  快速验证:"
    echo "    curl http://127.0.0.1/mqtt/auth  (通过 Nginx)"
    echo "    curl http://127.0.0.1:9503/health (直连实例)"
    echo "    ss -tlnp | grep 950[3-9]"
    echo ""
    exit 0
else
    echo -e "  ${RED}❌ ${FAILED} 个实例异常，请检查${NC}"
    echo ""
    echo "  排查命令:"
    echo "    tail -50 ${LOG_DIR}/instance-9503.log"
    echo "    ss -tlnp | grep 9503"
    echo "    php artisan edge:auth-swow (前台调试)"
    echo ""
    exit 1
fi
