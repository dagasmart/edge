#!/usr/bin/env bash
# ============================================================
#  EdgeAuth 生产部署脚本 (方案B: php artisan 方式)
#  ----------------------------------------------------------
#  全链路统一使用: php artisan edge:auth-swow
#  目录约定:
#    extensions/edge/bash/       本脚本 + start/stop/reload-cluster.sh
#    extensions/edge/bin/        server.php (仅做存在性提示，不用于启动)
#    extensions/edge/config/     secrets.php / env.sh
#    extensions/edge/test/       verify_simple.php
# ============================================================

set -Eeuo pipefail
IFS=$'\n\t'

# ===================== 配置区 =====================

APP_NAME="edge-auth"
PHP_BIN="/xp/server/php/php-8.4/bin/php"

# 项目根：脚本在 extensions/edge/bash/，上推 3 级
BASE_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"
_SD="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

CONF_DIR="${BASE_DIR}/extensions/edge/config"
TEST_DIR="${BASE_DIR}/extensions/edge/test"

# server.php 路径（仅用于 check_env 提示，启动不走这个）
BIN_DIR="${BASE_DIR}/extensions/edge/bin/server.php"

LOG_DIR="/var/log/${APP_NAME}"
PID_DIR="/var/run/${APP_NAME}"

REDIS_HOST="127.0.0.1"
REDIS_PORT="16379"
REDIS_PASS="uApDVQD7"
REDIS_DB="2"

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
warn(){ echo -e "${YELLOW}[$(date +%T)] ⚠️  ${1}${RESET}"; }
err()  { echo -e "${RED}[$(date +%T)] ❌ ${1}${RESET}" >&2; }
info(){ echo -e "${BLUE}[$(date +%T)] ℹ️  ${1}${RESET}"; }
die()  { err "${1}"; exit 1; }

# ===================== 参数解析 =====================

DRY_RUN=0; VERIFY=1; ACTION="deploy"
while [[ $# -gt 0 ]]; do
    case "$1" in
        deploy|rollback|status|stop|restart) ACTION="$1"; shift ;;
        -n|--dry-run)   DRY_RUN=1; shift ;;
        -s|--no-verify) VERIFY=0; shift ;;
        --no-ssl)       SSL_ENABLED=0; shift ;;
        -h|--help)      sed -n '/^sed/,/^EOF/p' "${BASH_SOURCE[0]}" 2>/dev/null; exit 0 ;;
        *) die "未知参数: $1" ;;
    esac
done

# ===================== 环境检查 =====================

check_env() {
    info "检查运行环境…"
    [[ "$(id -u)" -eq 0 ]] || warn "非 root：systemd / nginx / logrotate 将被跳过"

    [[ -x "$PHP_BIN" ]] || die "PHP_BIN 不可执行: ${PHP_BIN}"
    log "PHP: $(${PHP_BIN} -r 'echo PHP_VERSION;') (${PHP_BIN})"

    "${PHP_BIN}" -r "extension_loaded('swow') || exit(1);" || die "Swow 未加载"
    log "Swow 已加载"

    "${PHP_BIN}" -r "class_exists('Swow\Atomic') || exit(1);" \
        && log "Swow\\Atomic 可用" || warn "Swow\\Atomic 不可用"

    "${PHP_BIN}" -r "extension_loaded('pcntl') || exit(1);" \
        && log "pcntl 可用" || warn "pcntl 不可用"

    # artisan 文件必须存在
    [[ -f "${BASE_DIR}/artisan" ]] || die "artisan 不存在: ${BASE_DIR}/artisan（Laravel 根目录不对？）"
    log "artisan 存在: ${BASE_DIR}/artisan"

    # server.php 提示（不阻断）
    [[ -f "$BIN_DIR" ]] && log "server.php 存在（仅提示，启动走 artisan）" \
        || warn "server.php 未找到: ${BIN_DIR}（不影响 artisan 启动）"

    # Redis
    if command -v redis-cli >/dev/null 2>&1; then
        redis-cli -h "$REDIS_HOST" -p "$REDIS_PORT" -a "$REDIS_PASS" -n "$REDIS_DB" \
            ping >/dev/null 2>&1 || die "Redis 不可达"
    else
        "${PHP_BIN}" -r "
            \$r=new Redis();\$r->connect('${REDIS_HOST}',${REDIS_PORT},2) or exit(1);
            \$r->auth('${REDIS_PASS}') or exit(1);\$r->select(${REDIS_DB}) or exit(1);
            exit(\$r->ping()===true?0:1);" || die "Redis 不可达"
    fi
    log "Redis 正常"
}

# ===================== 目录 =====================

setup_dirs() {
    info "初始化目录…"
    for d in "$LOG_DIR" "$PID_DIR"; do
        [[ ! -d "$d" ]] && { [[ $DRY_RUN -eq 1 ]] && info "(dry) mkdir $d" && continue; mkdir -p "$d"; }
    done
    [[ -n "$RUN_USER" ]] && chown -R "${RUN_USER}:${RUN_GROUP:-$RUN_USER}" "$LOG_DIR" "$PID_DIR" 2>/dev/null || true
    [[ $DRY_RUN -eq 1 ]] && return
    chmod +x "${_SD}"/*.sh 2>/dev/null || true
    chmod +x "${TEST_DIR}"/*.php 2>/dev/null || true
}

# ===================== Redis 预热 =====================

seed_redis() {
    info "Redis 预热…"
    local pairs=() _PAIR_KEY=() _PAIR_VAL=()
    local secret_file="${CONF_DIR}/secrets.php"

    if [[ -f "$secret_file" ]]; then
        while IFS= read -r line; do pairs+=("$line"); done < <("${PHP_BIN}" -r "
            \$cfg=include('${secret_file}');
            foreach(\$cfg as \$k=>\$v) echo \$k.'='.\$v.\"\\n\";
        ")
        [[ ${#pairs[@]} -gt 0 ]] || die "secrets.php 返回空"
    else
        warn "未找到 secrets.php，使用内置 bench_secret"
        pairs+=("bench_client:bench_user=bench_secret")
    fi

    local _kv _k _v
    for _kv in "${pairs[@]}"; do
        _PAIR_KEY+=("${_kv%%=*}"); _PAIR_VAL+=("${_kv#*=}")
    done

    for i in "${!_PAIR_KEY[@]}"; do
        local key="${_PAIR_KEY[$i]}" secret="${_PAIR_VAL[$i]}"
        local rk="device:secret:${key/:/-}"
        [[ $DRY_RUN -eq 1 ]] && { info "(dry) SET ${rk}"; continue; }
        if command -v redis-cli >/dev/null 2>&1; then
            redis-cli -h "$REDIS_HOST" -p "$REDIS_PORT" -a "$REDIS_PASS" -n "$REDIS_DB" \
                --no-auth-warning SET "$rk" "$secret" >/dev/null || die "Redis SET 失败"
        else
            "${PHP_BIN}" -r "
                \$r=new Redis();\$r->connect('${REDIS_HOST}',${REDIS_PORT},2) or exit(1);
                \$r->auth('${REDIS_PASS}') or exit(1);\$r->select(${REDIS_DB}) or exit(1);
                \$r->set('${rk}','${secret}') or exit(1);" || die "Redis SET 失败"
        fi
        log "seeded: ${rk}"
    done
}

# ===================== 构建 =====================

build() {
    info "构建…"
    [[ $DRY_RUN -eq 1 ]] && return
    [[ -f "${BASE_DIR}/composer.json" ]] && (cd "$BASE_DIR" && composer install --no-dev --optimize-autoloader --quiet) || true
    date +%s > "${BASE_DIR}/.deploy_snapshot"
    log "版本快照已记录"
}

# ===================== 实例管理（artisan 方式）=====================

pid_file() { echo "${PID_DIR}/${APP_NAME}.${1}.pid"; }
is_running() { local pf="$(pid_file "$1")"; [[ -f "$pf" ]] && kill -0 "$(cat "$pf")" 2>/dev/null; }

start_instance() {
    local port="$1"
    if is_running "$port"; then warn "实例 :${port} 已在运行"; return 0; fi
    info "启动实例 :${port} …"
    [[ $DRY_RUN -eq 1 ]] && { info "(dry) cd ${BASE_DIR} && ${PHP_BIN} artisan edge:auth-swow (PORT=${port})"; return; }

    local logf="${LOG_DIR}/${APP_NAME}.${port}.log"
    local errf="${LOG_DIR}/${APP_NAME}.${port}.err"

    cd "${BASE_DIR}"
    AUTH_PORT="${port}" \
    REDIS_DB="${REDIS_DB}" \
    MAX_CONCURRENCY=10000 \
    REDIS_POOL_SIZE="${POOL_SIZE}" \
    nohup "${PHP_BIN}" artisan edge:auth-swow >>"$logf" 2>>"$errf" &

    local pid=$!
    echo "$pid" > "$(pid_file "$port")"
    cd - >/dev/null

    # 等待端口监听
    local w=0
    while [[ $w -lt 8 ]]; do
        "${PHP_BIN}" -r "\$fp=@fsockopen('127.0.0.1',${port},\$e,\$s,1);if(\$fp){fclose(\$fp);exit(0);}exit(1);" 2>/dev/null \
            && { log "实例 :${port} 已监听 (pid=${pid})"; return 0; }
        sleep 0.5; (( w += 1 ))
    done
    err "实例 :${port} 启动超时"; return 1
}

stop_instance() {
    local port="$1"; local pf="$(pid_file "$port")"
    if ! is_running "$port"; then rm -f "$pf"; return 0; fi
    local pid="$(cat "$pf")"
    info "停止 :${port} (pid=${pid})"
    [[ $DRY_RUN -eq 1 ]] && return
    kill -TERM "$pid" 2>/dev/null || true
    local w=0
    while [[ $w -lt 10 ]] && kill -0 "$pid" 2>/dev/null; do sleep 0.5; (( w += 1 )); done
    kill -0 "$pid" 2>/dev/null && { warn "SIGKILL :${port}"; kill -KILL "$pid" 2>/dev/null; }
    rm -f "$pf"
    log "实例 :${port} 已停止"
}

start_all() { for p in "${PORTS[@]}"; do start_instance "$p"; done; }
stop_all()  { for p in "${PORTS[@]}"; do stop_instance "$p"; done; }

# ===================== 健康检查 =====================

health_check() {
    info "健康检查…"
    local failed=0
    for p in "${PORTS[@]}"; do
        "${PHP_BIN}" -r "\$fp=@fsockopen('127.0.0.1',${p},\$e,\$s,2);if(\$fp){fclose(\$fp);exit(0);}exit(1);" 2>/dev/null \
            || { err ":${p} 端口不可达"; (( failed += 1 )); continue; }
        local code
        code=$("${PHP_BIN}" -r "
            \$ctx=stream_context_create(['http'=>['timeout'=>3,'ignore_errors'=>true]]);
            @file_get_contents('http://127.0.0.1:${p}/health',false,\$ctx);
            if(isset(\$http_response_header))foreach(\$http_response_header as \$h)
                if(preg_match('/HTTP\/\d\.\d\s+(\d+)/',\$h,\$m)){echo \$m[1];break;}
        " 2>/dev/null)
        [[ "$code" == "200" ]] && log ":${p} /health → 200" || { err ":${p} /health → ${code:-无}"; (( failed += 1 )); }
    done

    local ac
    ac=$("${PHP_BIN}" -r "
        \$ctx=stream_context_create(['http'=>['method'=>'POST','header'=>'Content-Type: application/x-www-form-urlencoded','content'=>'clientid=&username=&password=','timeout'=>3,'ignore_errors'=>true]]);
        @file_get_contents('http://127.0.0.1:${PORTS[0]}/mqtt/auth',false,\$ctx);
        if(isset(\$http_response_header))foreach(\$http_response_header as \$h)
            if(preg_match('/HTTP\/\d\.\d\s+(\d+)/',\$h,\$m)){echo \$m[1];break;}
    " 2>/dev/null)
    [[ "$ac" == "401" ]] && log "认证链路正常 (/mqtt/auth → 401)" || { err "/mqtt/auth → ${ac:-无}"; (( failed += 1 )); }

    [[ $failed -eq 0 ]] || die "健康检查失败: ${failed} 个异常"
    log "全部实例健康 ✅"
}

# ===================== verify =====================

run_verify() {
    [[ $VERIFY -eq 0 ]] && return
    [[ -x "${TEST_DIR}/verify_simple.php" ]] || { warn "verify 不可执行，跳过"; return; }
    info "运行 verify…"
    [[ $DRY_RUN -eq 1 ]] && return
    "${PHP_BIN}" "${TEST_DIR}/verify_simple.php" 2>&1 | tee "${LOG_DIR}/verify.log" | grep -q "ALL PASSED" \
        && log "verify 全部通过 🎯" || warn "verify 有失败项"
}

# ===================== systemd =====================

install_systemd() {
    [[ "$(id -u)" -ne 0 ]] && { warn "非 root，跳过 systemd"; return; }
    command -v systemctl >/dev/null 2>&1 || return
    info "安装 systemd 服务…"
    [[ $DRY_RUN -eq 1 ]] && return
    cat > "/etc/systemd/system/${APP_NAME}.service" <<EOF
[Unit]
Description=EdgeAuth MQTT Auth Server
After=network.target redis.service

[Service]
Type=forking
WorkingDirectory=${BASE_DIR}
ExecStart=${_SD}/start-cluster.sh
ExecStop=${_SD}/stop-cluster.sh
Restart=on-failure
RestartSec=5
LimitNOFILE=65535

[Install]
WantedBy=multi-user.target
EOF
    systemctl daemon-reload
    systemctl enable "${APP_NAME}.service" >/dev/null 2>&1 || true
    log "systemd 服务已安装"
}

# ===================== Nginx / logrotate（与之前一致，不变）=====================

generate_nginx_conf() {
    command -v nginx >/dev/null 2>&1 || { warn "nginx 未安装，跳过"; return; }
    [[ "$(id -u)" -ne 0 ]] && { warn "非 root，跳过"; return; }
    info "生成 Nginx 配置…"
    [[ $DRY_RUN -eq 1 ]] && return

    local conf="upstream edge_auth_backend {\n    least_conn;\n"
    for p in "${PORTS[@]}"; do conf+="    server 127.0.0.1:${p} max_fails=3 fail_timeout=10s;\n"; done
    conf+="    keepalive 128;\n}\n"
    echo -e "$conf" > "$NGINX_UPSTREAM_FILE"
    log "upstream: ${NGINX_UPSTREAM_FILE}"

    cat > "$NGINX_LIMIT_FILE" <<EOF
limit_req_zone \$binary_remote_addr zone=edge_auth_limit:10m rate=${RATE_AUTH};
limit_req_zone \$binary_remote_addr zone=edge_observe_limit:1m rate=${RATE_OBSERVE};
EOF

    if [[ $SSL_ENABLED -eq 1 && -f "$SSL_CERT_PATH" && -f "$SSL_KEY_PATH" ]]; then
        cat > "$NGINX_SSL_FILE" <<EOF
server { listen 80; server_name _; return 301 https://\$host\$request_uri; }
server {
    listen ${SSL_PORT} ssl http2; server_name _;
    ssl_certificate ${SSL_CERT_PATH}; ssl_certificate_key ${SSL_KEY_PATH};
    ssl_protocols ${SSL_PROTOCOLS}; ssl_ciphers '${SSL_CIPHERS}';
    ssl_prefer_server_ciphers on; ssl_session_cache shared:SSL:10m; ssl_session_timeout 10m;
    add_header X-Frame-Options DENY always;
    add_header X-Content-Type-Options nosniff always;
    location /mqtt/auth {
        limit_req zone=edge_auth_limit burst=${RATE_BURST} nodelay; limit_req_status 429;
        proxy_pass http://edge_auth_backend; proxy_buffering off; proxy_http_version 1.1;
        proxy_set_header Connection ''; proxy_set_header X-Real-IP \$remote_addr;
    }
    location /health { limit_req zone=edge_observe_limit burst=20 nodelay; proxy_pass http://edge_auth_backend; access_log off; }
    location /metrics { limit_req zone=edge_observe_limit burst=20 nodelay; proxy_pass http://edge_auth_backend; access_log off; }
    location / { return 404; }
}
EOF
        log "SSL: ${NGINX_SSL_FILE}"
    else
        cat > "$NGINX_SITE_FILE" <<EOF
server {
    listen 80; server_name _;
    location /mqtt/auth {
        limit_req zone=edge_auth_limit burst=${RATE_BURST} nodelay; limit_req_status 429;
        proxy_pass http://edge_auth_backend; proxy_buffering off; proxy_http_version 1.1;
        proxy_set_header Connection '';
    }
    location /health { limit_req zone=edge_observe_limit burst=20 nodelay; proxy_pass http://edge_auth_backend; access_log off; }
    location /metrics { limit_req zone=edge_observe_limit burst=20 nodelay; proxy_pass http://edge_auth_backend; access_log off; }
    location / { return 404; }
}
EOF
        log "HTTP: ${NGINX_SITE_FILE}"
    fi

    nginx -t && systemctl reload nginx && log "nginx reloaded ✅" || warn "nginx reload 失败"
}

generate_logrotate() {
    [[ "$(id -u)" -ne 0 ]] && return
    [[ $DRY_RUN -eq 1 ]] && return
    cat > "/etc/logrotate.d/${APP_NAME}" <<EOF
${LOG_DIR}/*.log ${LOG_DIR}/*.err { daily missingok rotate 30 compress delaycompress notifempty create 0640 root adm }
EOF
    log "logrotate 已写入"
}

# ===================== 状态 / 回滚 =====================

do_status() {
    echo -e "${BOLD}=== EdgeAuth 状态 ===${RESET}"
    for p in "${PORTS[@]}"; do
        if is_running "$p"; then echo -e ":${p}\tPID=$(cat "$(pid_file "$p")")\t\033[0;32mRUNNING\033[0m"
        else echo -e ":${p}\tSTOPPED\t\033[0;31mDOWN\033[0m"; fi
    done
}

do_rollback() { stop_all; start_all; health_check; log "回滚完成"; }

# ===================== 主流程 =====================

main() {
    case "$ACTION" in
        deploy)
            echo -e "${BOLD}🚀 EdgeAuth Deploy (artisan 模式, dry=$([[ $DRY_RUN -eq 1 ]] && echo on || echo off))${RESET}"
            check_env; setup_dirs; build; seed_redis
            stop_all; start_all; health_check; run_verify
            install_systemd; generate_nginx_conf; generate_logrotate
            echo; log "🎯 部署完成 — ${#PORTS[@]} 实例 (artisan edge:auth-swow)"
            ;;
        rollback) do_rollback ;;
        status)   do_status ;;
        stop)     stop_all ;;
        restart)  stop_all; start_all; health_check ;;
        *)        die "未知动作" ;;
    esac
}

main
