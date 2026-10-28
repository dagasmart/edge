#!/usr/bin/env bash
# ============================================================
#  EdgeAuth 生产部署脚本 (最终版)
#  ----------------------------------------------------------
#  目录约定:
#    extensions/edge/bash/       本脚本 + start/stop-cluster.sh
#    extensions/edge/config/     secrets.php / env.sh
#    extensions/edge/test/       verify_simple.php 等
#    server.php 位置由 BIN_DIR 指定（默认同 bash/ 目录，可改）
#
#  包含: 环境检查 · Redis 预热 · 多实例启停 · 健康检查
#        · verify 验证 · systemd · Nginx upstream 自动生成
#        · Nginx 限流 · SSL 终止 · 日志轮转
#
#  用法: ./deploy.sh [deploy|rollback|status|stop|restart] [选项]
#        ./deploy.sh                    # 标准部署 + 验证
#        ./deploy.sh --dry-run         # 预演
#        ./deploy.sh --no-ssl          # 不做 SSL（内网/测试）
#        ./deploy.sh rollback
#        ./deploy.sh status
# ============================================================

set -Eeuo pipefail
IFS=$'\n\t'

# ===================== 配置区（按环境修改） =====================

APP_NAME="edge-auth"

# 项目根目录：本脚本位于 extensions/edge/bash/deploy.sh，上推 3 级即项目根
BASE_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"
_SD="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"   # 本脚本所在目录 (bash/)

# 脚本目录（bash/）：deploy.sh / start-cluster.sh / stop-cluster.sh 都在这里

# 配置 / 测试目录
CONF_DIR="${BASE_DIR}/extensions/edge/config"
TEST_DIR="${BASE_DIR}/extensions/edge/test"

# server.php 位置（★重点★）：默认与脚本同目录，若在别处请改成绝对路径
#   例: BIN_DIR="${BASE_DIR}/extensions/edge/bin/server.php"
BIN_DIR="${BASE_DIR}/extensions/edge/bin/server.php"   # server.php 所在目录，可改为 extensions/edge/bin

LOG_DIR="/var/log/${APP_NAME}"
PID_DIR="/var/run/${APP_NAME}"

REDIS_HOST="127.0.0.1"
REDIS_PORT="16379"
REDIS_PASS="uApDVQD7"
REDIS_DB="2"

# 多实例监听端口（改这里，upstream / 限流 / systemd 全跟着变）
PORTS=(9503 9504 9505 9506)

POOL_SIZE=256
RUN_USER=""
RUN_GROUP=""

# --- Nginx ---
NGINX_CONF_DIR="/etc/nginx/conf.d"
NGINX_UPSTREAM_FILE="${NGINX_CONF_DIR}/edge-auth-upstream.conf"
NGINX_LIMIT_FILE="${NGINX_CONF_DIR}/edge-auth-limit.conf"
NGINX_SSL_FILE="${NGINX_CONF_DIR}/edge-auth-ssl.conf"
NGINX_SITE_FILE="${NGINX_CONF_DIR}/edge-auth.conf"

RATE_AUTH="100r/s"
RATE_OBSERVE="10r/s"
RATE_BURST="200"

# --- SSL ---
SSL_ENABLED=1
SSL_CERT_PATH="/etc/ssl/certs/edge-auth.crt"
SSL_KEY_PATH="/etc/ssl/private/edge-auth.key"
SSL_CHAIN_PATH="/etc/ssl/certs/edge-auth-chain.crt"
SSL_PORT="443"
SSL_PROTOCOLS="TLSv1.2 TLSv1.3"
SSL_CIPHERS="ECDHE-ECDSA-AES128-GCM-SHA256:ECDHE-RSA-AES128-GCM-SHA256:ECDHE-ECDSA-AES256-GCM-SHA384:ECDHE-RSA-AES256-GCM-SHA384"
SSL_DHPARAM="/etc/ssl/certs/dhparam.pem"

# ===================== 颜色 / 日志 =====================

RED='\033[0;31m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'
BLUE='\033[0;34m'; BOLD='\033[1m'; RESET='\033[0m'

log()  { echo -e "${GREEN}[$(date +%T)] ✅ ${1}${RESET}"; }
warn() { echo -e "${YELLOW}[$(date +%T)] ⚠️  ${1}${RESET}"; }
err()  { echo -e "${RED}[$(date +%T)] ❌ ${1}${RESET}" >&2; }
info() { echo -e "${BLUE}[$(date +%T)] ℹ️  ${1}${RESET}"; }
die()  { err "${1}"; exit 1; }

# ===================== 参数解析 =====================

DRY_RUN=0
VERIFY=1
ACTION="deploy"

usage() {
    cat <<EOF
用法: ${0##*/} [命令] [选项]

命令:
  deploy     (默认) 构建·预热·启动·检查·验证·生成配置
  rollback   回滚（重启实例，代码需手动 git checkout）
  status     查看实例状态
  stop       停止所有实例
  restart    重启所有实例

选项:
  -n, --dry-run     只打印动作，不执行
  -s, --no-verify   部署后跳过 verify
  --no-ssl          不生成 SSL 配置（纯内网/测试）
  -h, --help        帮助
EOF
}

while [[ $# -gt 0 ]]; do
    case "$1" in
        deploy|rollback|status|stop|restart) ACTION="$1"; shift ;;
        -n|--dry-run)   DRY_RUN=1; shift ;;
        -s|--no-verify) VERIFY=0; shift ;;
        --no-ssl)       SSL_ENABLED=0; shift ;;
        -h|--help)      usage; exit 0 ;;
        *) die "未知参数: $1 (见 --help)" ;;
    esac
done

# ===================== 环境检查 =====================

check_env() {
    info "检查运行环境…"
    [[ "$(id -u)" -eq 0 ]] || warn "非 root：systemd / nginx / logrotate 安装将被跳过"

    command -v php >/dev/null 2>&1 || die "php 未找到，需 PHP >= 8.1"
    PHP_VER=$(php -r 'echo PHP_MAJOR_VERSION.".".PHP_MINOR_VERSION;')
    [[ "${PHP_VER%%.*}" -ge 8 ]] || die "需要 PHP >= 8.1，当前 ${PHP_VER}"

    php -r "extension_loaded('swow') || exit(1);" 2>/dev/null \
        || die "Swow 扩展未加载: composer require swow/swow"
    log "Swow 已加载"

    if php -r "class_exists('Swow\Atomic') || exit(1);" 2>/dev/null; then
        log "Swow\\Atomic 可用 → 并发限制生效"
    else
        warn "Swow\\Atomic 不可用 → 并发限制禁用（生产不推荐）"
    fi

    php -r "extension_loaded('pcntl') || exit(1);" 2>/dev/null \
        && log "pcntl 可用 → 优雅退出生效" \
        || warn "pcntl 不可用 → 信号处理器不生效"

    # server.php 必须存在
    [[ -f "$BIN_DIR" ]] || die "server.php 未找到: ${BIN_DIR}（请修改脚本顶部的 BIN_DIR）"

    if command -v redis-cli >/dev/null 2>&1; then
        redis-cli -h "$REDIS_HOST" -p "$REDIS_PORT" -a "$REDIS_PASS" -n "$REDIS_DB" \
            ping >/dev/null 2>&1 || die "Redis 不可达 ${REDIS_HOST}:${REDIS_PORT} db=${REDIS_DB}"
    else
        php -r "
            \$r=new Redis();
            \$r->connect('${REDIS_HOST}',${REDIS_PORT},2) or exit(1);
            \$r->auth('${REDIS_PASS}') or exit(1);
            \$r->select(${REDIS_DB}) or exit(1);
            exit(\$r->ping()===true?0:1);
        " || die "Redis 不可达 (PHP 内联检测失败)"
    fi
    log "Redis 正常 ${REDIS_HOST}:${REDIS_PORT} db=${REDIS_DB}"
}

# ===================== 目录 / 权限 =====================

setup_dirs() {
    info "初始化目录与权限…"
    for d in "$LOG_DIR" "$PID_DIR"; do
        if [[ ! -d "$d" ]]; then
            [[ $DRY_RUN -eq 1 ]] && info "(dry) mkdir -p $d" && continue
            mkdir -p "$d"
        fi
    done
    if [[ -n "$RUN_USER" ]]; then
        chown -R "${RUN_USER}:${RUN_GROUP:-$RUN_USER}" "$LOG_DIR" "$PID_DIR" 2>/dev/null \
            || warn "chown 失败，请手动设置权限"
    fi
    # bash/ 目录下的 php 脚本一并赋可执行（verify 在 test/ 下，单独处理）
    # 给 bash/ 下的部署脚本赋权（_SD = 脚本目录）
    [[ $DRY_RUN -eq 1 ]] && info "(dry) chmod +x ${_SD}/*.sh" && return
    chmod +x "${_SD}"/*.sh 2>/dev/null || true
    # 测试脚本也需要可执行
    chmod +x "${TEST_DIR}"/*.php 2>/dev/null || true
    chmod +x "${TEST_DIR}"/*.php 2>/dev/null || true
}

# ===================== Redis 预热 =====================

seed_redis() {
    info "Redis 预热：写入设备密钥…"
    local pairs=()
    local secret_file="${CONF_DIR}/secrets.php"

    if [[ -f "$secret_file" ]]; then
        # 输出 shell 可 eval 的赋值：pairs[key]='secret'
        while IFS= read -r line; do pairs+=("$line"); done < <(php -r "
            \$cfg=include('${secret_file}');
            foreach(\$cfg as \$k=>\$v) echo \"pairs[\$k]='\$v'\n\";
        ")
        eval "${pairs[*]}"
    else
        warn "未找到 ${secret_file}，使用内置测试密钥 bench_secret"
        pairs=("pairs[bench_client:bench_user]=bench_secret")
        eval "${pairs[*]}"
    fi

    for key in "${!pairs[@]}"; do
        local client_id="${key%%:*}" user="${key#*:}" secret="${pairs[$key]}"
        local rk="device:secret:${client_id}:${user}"

        if [[ $DRY_RUN -eq 1 ]]; then info "(dry) SET ${rk}"; continue; fi

        if command -v redis-cli >/dev/null 2>&1; then
            redis-cli -h "$REDIS_HOST" -p "$REDIS_PORT" -a "$REDIS_PASS" -n "$REDIS_DB" \
                --no-auth-warning SET "$rk" "$secret" >/dev/null \
                || die "Redis SET 失败: ${rk}"
        else
            php -r "
                \$r=new Redis();
                \$r->connect('${REDIS_HOST}',${REDIS_PORT},2) or exit(1);
                \$r->auth('${REDIS_PASS}') or exit(1);
                \$r->select(${REDIS_DB}) or exit(1);
                \$r->set('${rk}','${secret}') or exit(1); echo 'OK';
            " | grep -q OK || die "Redis SET 失败(php): ${rk}"
        fi
        log "seeded: ${rk} ($(echo -n "$secret" | wc -c) bytes)"
    done
}

# ===================== 构建 =====================

build() {
    info "执行构建…"
    [[ $DRY_RUN -eq 1 ]] && info "(dry) composer install --no-dev" && return
    if [[ -f "${BASE_DIR}/composer.json" ]]; then
        (cd "$BASE_DIR" && composer install --no-dev --optimize-autoloader --quiet) \
            || warn "composer install 失败，继续"
    fi
    date +%s > "${BASE_DIR}/.deploy_snapshot"
    log "版本快照: $(cat "${BASE_DIR}/.deploy_snapshot")"
}

# ===================== 实例管理 =====================

pid_file() { echo "${PID_DIR}/${APP_NAME}.${1}.pid"; }

is_running() {
    local pf; pf="$(pid_file "$1")"
    [[ -f "$pf" ]] && kill -0 "$(cat "$pf")" 2>/dev/null
}

start_instance() {
    local port="$1"
    if is_running "$port"; then
        warn "实例 :${port} 已在运行 (pid=$(cat "$(pid_file "$port")"))"
        return 0
    fi
    info "启动实例 :${port} …"
    [[ $DRY_RUN -eq 1 ]] && info "(dry) php ${BIN_DIR} --port=${port} --pool=${POOL_SIZE}" && return

    local logf="${LOG_DIR}/${APP_NAME}.${port}.log"
    local errf="${LOG_DIR}/${APP_NAME}.${port}.err"
    nohup php "${BIN_DIR}" --port="${port}" --pool="${POOL_SIZE}" \
        >>"$logf" 2>>"$errf" &
    echo "$!" > "$(pid_file "$port")"

    local w=0
    while [[ $w -lt 5 ]]; do
        php -r "\$fp=@fsockopen('127.0.0.1',${port},\$e,\$s,1);if(\$fp){fclose(\$fp);exit(0);}exit(1);" 2>/dev/null \
            && { log "实例 :${port} 已监听 (pid=$!)"; return 0; }
        sleep 0.5; (( w += 1 ))
    done
    err "实例 :${port} 启动超时，查看 ${errf}"; return 1
}

stop_instance() {
    local port="$1"; local pf; pf="$(pid_file "$port")"
    if ! is_running "$port"; then
        rm -f "$pf"; return 0
    fi
    local pid; pid="$(cat "$pf")"
    info "停止实例 :${port} (pid=${pid}) …"
    [[ $DRY_RUN -eq 1 ]] && info "(dry) kill -TERM ${pid}" && return

    kill -TERM "$pid" 2>/dev/null || true
    local w=0
    while [[ $w -lt 10 ]] && kill -0 "$pid" 2>/dev/null; do sleep 0.5; (( w += 1 )); done
    if kill -0 "$pid" 2>/dev/null; then
        warn "实例 :${port} 未响应 SIGTERM，发 SIGKILL"
        kill -KILL "$pid" 2>/dev/null || true
    fi
    rm -f "$pf"
    log "实例 :${port} 已停止"
}

start_all() { for p in "${PORTS[@]}"; do start_instance "$p"; done; }
stop_all()  { for p in "${PORTS[@]}"; do stop_instance "$p"; done; }

# ===================== 健康检查 =====================

health_check() {
    info "健康检查 (${#PORTS[@]} 实例)…"
    local failed=0

    for p in "${PORTS[@]}"; do
        if ! php -r "\$fp=@fsockopen('127.0.0.1',${p},\$e,\$s,2);if(\$fp){fclose(\$fp);exit(0);}exit(1);" 2>/dev/null; then
            err "实例 :${p} 端口不可达"; (( failed += 1 )); continue
        fi
        local code
        code=$(php -r "
            \$ctx=stream_context_create(['http'=>['timeout'=>3,'ignore_errors'=>true]]);
            @file_get_contents('http://127.0.0.1:${p}/health',false,\$ctx);
            if(isset(\$http_response_header))foreach(\$http_response_header as \$h)
                if(preg_match('/HTTP\/\d\.\d\s+(\d+)/',\$h,\$m)){echo \$m[1];break;}
        " 2>/dev/null)
        [[ "$code" == "200" ]] && log "实例 :${p} /health → 200" \
            || { err "实例 :${p} /health → ${code:-无响应}"; (( failed += 1 )); }
    done

    # 认证链路存活校验（空字段应 401）
    local ac
    ac=$(php -r "
        \$ctx=stream_context_create(['http'=>['method'=>'POST','header'=>'Content-Type: application/x-www-form-urlencoded','content'=>'clientid=&username=&password=','timeout'=>3,'ignore_errors'=>true]]);
        @file_get_contents('http://127.0.0.1:${PORTS[0]}/mqtt/auth',false,\$ctx);
        if(isset(\$http_response_header))foreach(\$http_response_header as \$h)
            if(preg_match('/HTTP\/\d\.\d\s+(\d+)/',\$h,\$m)){echo \$m[1];break;}
    " 2>/dev/null)
    [[ "$ac" == "401" ]] && log "认证链路正常 (/mqtt/auth 空字段 → 401)" \
        || { err "认证链路异常 (/mqtt/auth → ${ac:-无响应})"; (( failed += 1 )); }

    [[ $failed -eq 0 ]] || die "健康检查失败: ${failed} 个异常"
    log "全部实例健康"
}

# ===================== verify =====================

run_verify() {
    [[ $VERIFY -eq 0 ]] && { info "已跳过 verify (--no-verify)"; return; }
    [[ -x "${TEST_DIR}/verify_simple.php" ]] || { warn "verify_simple.php 不可执行，跳过"; return; }
    info "运行 verify_simple.php …"
    [[ $DRY_RUN -eq 1 ]] && info "(dry) php ${TEST_DIR}/verify_simple.php" && return

    if php "${TEST_DIR}/verify_simple.php" 2>&1 | tee "${LOG_DIR}/verify.log" | grep -q "ALL PASSED"; then
        log "verify 全部通过 🎯"
    else
        err "verify 存在失败项，详见 ${LOG_DIR}/verify.log"
    fi
}

# ===================== systemd =====================

install_systemd() {
    [[ "$(id -u)" -ne 0 ]] && { warn "非 root，跳过 systemd"; return; }
    command -v systemctl >/dev/null 2>&1 || { warn "systemctl 未找到，跳过"; return; }
    info "安装 systemd 服务…"
    [[ $DRY_RUN -eq 1 ]] && info "(dry) 写入 /etc/systemd/system/${APP_NAME}.service" && return

    cat > "/etc/systemd/system/${APP_NAME}.service" <<EOF
[Unit]
Description=EdgeAuth MQTT Auth Server
After=network.target redis.service

[Service]
Type=forking
User=${RUN_USER:-root}
Group=${RUN_GROUP:-root}
WorkingDirectory=${BASE_DIR}
ExecStart=${BIN_DIR}/start-cluster.sh
ExecStop=${BIN_DIR}/stop-cluster.sh
Restart=on-failure
RestartSec=5
LimitNOFILE=65535

[Install]
WantedBy=multi-user.target
EOF
    systemctl daemon-reload
    systemctl enable "${APP_NAME}.service" >/dev/null 2>&1 || warn "systemctl enable 失败"
    log "systemd 服务已安装: ${APP_NAME}.service"
}

# ===================== Nginx upstream =====================

generate_nginx_conf() {
    command -v nginx >/dev/null 2>&1 || { warn "nginx 未安装，跳过"; return; }
    [[ "$(id -u)" -ne 0 ]] && { warn "非 root，跳过 nginx 配置"; return; }
    info "生成 Nginx upstream 配置…"
    [[ $DRY_RUN -eq 1 ]] && info "(dry) 写 ${NGINX_UPSTREAM_FILE}" && return

    local conf="## 自动生成 by deploy.sh @ $(date '+%F %T') — 勿手动修改\n\nupstream edge_auth_backend {\n    least_conn;\n"
    for p in "${PORTS[@]}"; do
        conf+="    server 127.0.0.1:${p} max_fails=3 fail_timeout=10s;\n"
    done
    conf+="    keepalive 128;\n}\n"
    echo -e "$conf" > "$NGINX_UPSTREAM_FILE"
    log "upstream 已写入: ${NGINX_UPSTREAM_FILE}"

    # 不带 SSL 时的纯 HTTP server 块（SSL 模式下由 ssl 文件接管，此文件仅作备份）
    if [[ $SSL_ENABLED -eq 0 ]]; then
        if [[ -f "$NGINX_SITE_FILE" ]]; then
            mv "$NGINX_SITE_FILE" "${NGINX_SITE_FILE}.bak" 2>/dev/null || true
        fi
        local site="## 自动生成 by deploy.sh @ $(date '+%F %T')\nserver {\n    listen 80;\n    server_name _;\n\n    location /mqtt/auth {\n        limit_req zone=edge_auth_limit burst=${RATE_BURST} nodelay;\n        limit_req_status 429;\n        proxy_pass http://edge_auth_backend;\n        proxy_set_header Host \$host;\n        proxy_set_header X-Real-IP \$remote_addr;\n        proxy_set_header X-Forwarded-For \$proxy_add_x_forwarded_for;\n        proxy_buffering off; proxy_request_buffering off;\n        proxy_http_version 1.1; proxy_set_header Connection '';\n        proxy_connect_timeout 5s; proxy_send_timeout 10s; proxy_read_timeout 10s;\n    }\n\n    location /health {\n        limit_req zone=edge_observe_limit burst=20 nodelay;\n        limit_req_status 429;\n        proxy_pass http://edge_auth_backend; proxy_buffering off; access_log off;\n    }\n\n    location /metrics {\n        limit_req zone=edge_observe_limit burst=20 nodelay;\n        limit_req_status 429;\n        proxy_pass http://edge_auth_backend; proxy_buffering off; access_log off;\n    }\n\n    location / { return 404; }\n}\n"
        echo -e "$site" > "$NGINX_SITE_FILE"
        log "HTTP server 块已写入: ${NGINX_SITE_FILE}"
    fi
}

# ===================== Nginx 限流 =====================

generate_nginx_limit() {
    command -v nginx >/dev/null 2>&1 || return
    [[ "$(id -u)" -ne 0 ]] && return
    info "生成 Nginx 限流配置…"
    [[ $DRY_RUN -eq 1 ]] && info "(dry) 写 ${NGINX_LIMIT_FILE}" && return

    local conf="## 自动生成 by deploy.sh @ $(date '+%F %T')\n\n# /mqtt/auth: 签名校验限速，防暴力\nlimit_req_zone \$binary_remote_addr zone=edge_auth_limit:10m rate=${RATE_AUTH};\n\n# /health /metrics: 观察端点限速\nlimit_req_zone \$binary_remote_addr zone=edge_observe_limit:1m rate=${RATE_OBSERVE};\n"
    echo -e "$conf" > "$NGINX_LIMIT_FILE"
    log "限流 zone 已写入: ${NGINX_LIMIT_FILE}"
}

# ===================== SSL 终止 =====================

generate_nginx_ssl() {
    command -v nginx >/dev/null 2>&1 || return
    [[ "$(id -u)" -ne 0 ]] && { warn "非 root，跳过 SSL"; return; }
    [[ $SSL_ENABLED -eq 0 ]] && { info "SSL 已禁用 (--no-ssl)，跳过"; return; }

    if [[ ! -f "$SSL_CERT_PATH" ]] || [[ ! -f "$SSL_KEY_PATH" ]]; then
        warn "SSL 证书缺失 (${SSL_CERT_PATH})，跳过 SSL 生成"
        return
    fi
    info "生成 Nginx SSL 配置…"
    [[ $DRY_RUN -eq 1 ]] && info "(dry) 写 ${NGINX_SSL_FILE}" && return

    [[ -f "$SSL_DHPARAM" ]] || openssl dhparam -out "$SSL_DHPARAM" 2048 2>/dev/null \
        || warn "dhparam 生成失败，SSL 将不含 dhparam"

    local conf="## 自动生成 by deploy.sh @ $(date '+%F %T')\n\n# HTTP → HTTPS\nserver {\n    listen 80;\n    server_name _;\n    return 301 https://\$host\$request_uri;\n}\n\nserver {\n    listen ${SSL_PORT} ssl http2;\n    server_name _;\n\n    ssl_certificate     ${SSL_CERT_PATH};\n    ssl_certificate_key ${SSL_KEY_PATH};\n"
    [[ -f "$SSL_CHAIN_PATH" ]] && conf+="    ssl_trusted_certificate ${SSL_CHAIN_PATH};\n"
    conf+="\n    ssl_protocols ${SSL_PROTOCOLS};\n    ssl_ciphers '${SSL_CIPHERS}';\n    ssl_prefer_server_ciphers on;\n"
    [[ -f "$SSL_DHPARAM" ]] && conf+="    ssl_dhparam ${SSL_DHPARAM};\n"
    conf+="    ssl_session_cache shared:SSL:10m;\n    ssl_session_timeout 10m;\n    ssl_session_tickets off;\n\n    add_header X-Frame-Options DENY always;\n    add_header X-Content-Type-Options nosniff always;\n    add_header X-XSS-Protection \"1; mode=block\" always;\n\n    location /mqtt/auth {\n        limit_req zone=edge_auth_limit burst=${RATE_BURST} nodelay;\n        limit_req_status 429;\n        proxy_pass http://edge_auth_backend;\n        proxy_set_header Host \$host;\n        proxy_set_header X-Real-IP \$remote_addr;\n        proxy_set_header X-Forwarded-For \$proxy_add_x_forwarded_for;\n        proxy_set_header X-Forwarded-Proto https;\n        proxy_buffering off; proxy_request_buffering off;\n        proxy_http_version 1.1; proxy_set_header Connection '';\n        proxy_connect_timeout 5s; proxy_send_timeout 10s; proxy_read_timeout 10s;\n    }\n\n    location /health {\n        limit_req zone=edge_observe_limit burst=20 nodelay;\n        limit_req_status 429;\n        proxy_pass http://edge_auth_backend; proxy_buffering off; access_log off;\n    }\n\n    location /metrics {\n        limit_req zone=edge_observe_limit burst=20 nodelay;\n        limit_req_status 429;\n        proxy_pass http://edge_auth_backend; proxy_buffering off; access_log off;\n    }\n\n    location / { return 404; }\n}\n"
    echo -e "$conf" > "$NGINX_SSL_FILE"
    log "SSL 配置已写入: ${NGINX_SSL_FILE}"

    # SSL 模式下，原 HTTP server 块备份
    if [[ -f "$NGINX_SITE_FILE" ]]; then
        mv "$NGINX_SITE_FILE" "${NGINX_SITE_FILE}.bak" 2>/dev/null || true
        log "原 HTTP server 块已备份为 ${NGINX_SITE_FILE}.bak"
    fi
}

# ===================== 日志轮转 =====================

generate_logrotate() {
    [[ "$(id -u)" -ne 0 ]] && { warn "非 root，跳过 logrotate"; return; }
    info "生成 logrotate 配置…"
    [[ $DRY_RUN -eq 1 ]] && info "(dry) 写 /etc/logrotate.d/${APP_NAME}" && return

    cat > "/etc/logrotate.d/${APP_NAME}" <<EOF
${LOG_DIR}/*.log
${LOG_DIR}/*.err
{
    daily
    missingok
    rotate 30
    compress
    delaycompress
    notifempty
    create 0640 root adm
    sharedscripts
}
EOF
    log "logrotate 已写入: /etc/logrotate.d/${APP_NAME}"
    logrotate -d "/etc/logrotate.d/${APP_NAME}" >/dev/null 2>&1 \
        && log "logrotate 语法检查通过" \
        || warn "logrotate 配置可能有问题，请运行 logrotate -d 检查"
}

# ===================== Nginx 配置校验 & reload =====================

nginx_reload() {
    command -v nginx >/dev/null 2>&1 || return
    [[ "$(id -u)" -ne 0 ]] && return
    if nginx -t 2>&1 | grep -q "test is successful"; then
        log "nginx 配置语法通过"
        systemctl reload nginx 2>/dev/null && log "nginx reloaded ✅" \
            || warn "nginx reload 失败（可能未使用 systemd）"
    else
        err "nginx 配置语法错误，请手动运行 nginx -t"; nginx -t
    fi
}

# ===================== 回滚 / 状态 =====================

do_rollback() {
    info "执行回滚…"
    stop_all
    warn "代码回滚需手动执行 (如 git checkout <prev>)，本脚本仅重启实例"
    start_all
    health_check
    log "回滚完成（实例已重启）"
}

do_status() {
    echo -e "${BOLD}=== EdgeAuth 实例状态 ===${RESET}"
    printf "%-8s %-10s %-8s %s\n" "PORT" "PID" "STATUS" "UPTIME"
    for p in "${PORTS[@]}"; do
        local pf; pf="$(pid_file "$p")"
        if is_running "$p"; then
            local pid; pid="$(cat "$pf")"
            local up; up=$(ps -o etime= -p "$pid" 2>/dev/null | tr -d ' ' || echo '-')
            printf "%-8s %-10s \033[0;32m%-8s\033[0m %s\n" "$p" "$pid" "RUNNING" "$up"
        else
            printf "%-8s %-10s \033[0;31m%-8s\033[0m %s\n" "$p" "-" "STOPPED" "-"
        fi
    done
}

# ===================== 主流程 =====================

main() {
    case "$ACTION" in
        deploy)
            echo -e "${BOLD}🚀 EdgeAuth Deploy${RESET} (dry=$([[ $DRY_RUN -eq 1 ]] && echo on || echo off))"
            check_env
            setup_dirs
            build
            seed_redis
            stop_all
            start_all
            health_check
            run_verify
            install_systemd
            generate_nginx_conf
            generate_nginx_limit
            generate_nginx_ssl
            nginx_reload
            generate_logrotate
            echo
            log "🎯 部署完成 — ${#PORTS[@]} 实例运行中"
            echo -e "   端口:     ${PORTS[*]}"
            echo -e "   server:   ${BIN_DIR}"
            echo -e "   upstream: ${NGINX_UPSTREAM_FILE}"
            echo -e "   限流:     ${NGINX_LIMIT_FILE}"
            echo -e "   SSL:      ${SSL_ENABLED}-${NGINX_SSL_FILE}"
            echo -e "   日志:     ${LOG_DIR}/  (logrotate: /etc/logrotate.d/${APP_NAME})"
            echo -e "   验证:     tail -1 ${LOG_DIR}/verify.log"
            ;;
        rollback) do_rollback ;;
        status)   do_status ;;
        stop)     stop_all ;;
        restart)  stop_all; start_all; health_check ;;
        *)        die "未知动作: $ACTION" ;;
    esac
}

main
