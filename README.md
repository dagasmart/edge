# EdgeAuth + NanoMQ 实操部署手册

> 适用环境：Laravel 13 / PHP 8.4 / Swow / PostgreSQL / Docker NanoMQ  
> 认证链路：**本地直连形态**（`http://127.0.0.1:9503/admin-api/extension/edge/*`）  
> 生成时间：2026-10-29

---

## 目录

- [一、EdgeAuth 部署](#一edgeauth-部署)
  - [1.1 目录与权限](#11-目录与权限)
  - [1.2 环境预检](#12-环境预检)
  - [1.3 Redis 密钥注入](#13-redis-密钥注入)
  - [1.4 启动实例（artisan 方式）](#14-启动实例artisan-方式)
  - [1.5 停止实例](#15-停止实例)
  - [1.6 健康检查](#16-健康检查)
  - [1.7 回归测试](#17-回归测试)
  - [1.8 systemd 服务](#18-systemd-服务)
  - [1.9 Nginx 配置（可选，本地直连可省略）](#19-nginx-配置可选本地直连可省略)
  - [1.10 logrotate](#110-logrotate)
- [二、NanoMQ MQTT 部署](#二nanomq-mqtt-部署)
  - [2.1 证书生成](#21-证书生成)
  - [2.2 配置文件](#22-配置文件)
  - [2.3 Docker Compose 启动](#23-docker-compose-启动)
  - [2.4 客户端验证](#24-客户端验证)
  - [2.5 iptables 保护明文端口](#25-iptables-保护明文端口)
  - [2.6 常用运维](#26-常用运维)
  - [2.7 参数调优与生产建议](#27-参数调优与生产建议)
- [三、数据库急救（PostgreSQL 外键死锁）](#三数据库急救postgresql-外键死锁)
- [四、迁移回滚（三张表）](#四迁移回滚三张表)
- [五、EdgeAuth 源码级实操补充](#五edgeauth-源码级实操补充)
  - [5.1 artisan 命令完整参数](#51-artisan-命令完整参数)
  - [5.2 环境变量 / 常量清单](#52-环境变量--常量清单)
  - [5.3 路由端点清单](#53-路由端点清单)
  - [5.4 seed_redis.php 密钥注入工具](#54-seed_redisphp-密钥注入工具)
  - [5.5 verify_simple.php 回归测试](#55-verify_simplephp-回归测试)
  - [5.6 故障排查速查](#56-故障排查速查)
  - [5.7 文件结构速览](#57-文件结构速览)
- [六、EdgeAuth Prometheus 指标字段解释](#六edgeauth-prometheus-指标字段解释)
  - [6.1 指标完整结构](#61-指标完整结构)
  - [6.2 字段逐条解释](#62-字段逐条解释)
  - [6.3 Prometheus 抓取配置](#63-prometheus-抓取配置)
  - [6.4 Grafana 关键查询](#64-grafana-关键查询)
  - [6.5 health 与 metrics 端点差异](#65-health-与-metrics-端点差异)
- [七、版本与兼容性备忘](#七版本与兼容性备忘)
- [八、快速联调顺序（一步到位）](#八快速联调顺序一步到位)
- [附录 A：完整文件结构](#附录-a完整文件结构)

---

## 一、EdgeAuth 部署

### 1.1 目录与权限

```bash
mkdir -p /var/log/edge-auth /var/run/edge-auth

# 脚本可执行
chmod +x extensions/edge/bash/*.sh
chmod +x extensions/edge/test/*.php
```

### 1.2 环境预检

```bash
# PHP & Swow
/xp/server/php/php-8.4/bin/php -v
/xp/server/php/php-8.4/bin/php -r 'extension_loaded("swow") || exit(1);'
/xp/server/php/php-8.4/bin/php -r 'class_exists("Swow\Atomic") || exit(1);'
/xp/server/php/php-8.4/bin/php -r 'extension_loaded("pcntl") || exit(1);'

# artisan 存在性
ls -l artisan

# Redis 连通
redis-cli -h 127.0.0.1 -p 16379 -a uApDVQD7 -n 2 ping

# 或 PHP 方式
/xp/server/php/php-8.4/bin/php -r '
    $r=new Redis();$r->connect("127.0.0.1",16379,2) or exit(1);
    $r->auth("uApDVQD7") or exit(1);$r->select(2) or exit(1);
    exit($r->ping()===true?0:1);
'
```

### 1.3 Redis 密钥注入

```bash
# 方式 A：从 secrets.php 读取
/xp/server/php/php-8.4/bin/php -r "
    \$cfg=include('extensions/edge/config/secrets.php');
    foreach(\$cfg as \$k=>\$v) echo \$k.'='.\$v.\"\n\";
" | while IFS= read -r line; do
    rk="device:secret:${line%%=*}"
    rv="${line#*=}"
    redis-cli -h 127.0.0.1 -p 16379 -a uApDVQD7 -n 2 --no-auth-warning SET "$rk" "$rv"
    echo "seeded: $rk"
done

# 方式 B：无 secrets.php，注入 bench 密钥
redis-cli -h 127.0.0.1 -p 16379 -a uApDVQD7 -n 2 --no-auth-warning \
    SET "device:secret:bench_client:bench_user" "bench_secret"
```

### 1.4 启动实例（artisan 方式）

```bash
cd /path/to/laravel-root

# 单实例
AUTH_PORT=9503 REDIS_DB=2 MAX_CONCURRENCY=10000 REDIS_POOL_SIZE=256 \
nohup /xp/server/php/php-8.4/bin/php artisan edge:auth-swow \
    >> /var/log/edge-auth/edge-auth.9503.log \
    2>> /var/log/edge-auth/edge-auth.9503.err &

# 批量 4 实例
for port in 9503 9504 9505 9506; do
    AUTH_PORT=$port REDIS_DB=2 MAX_CONCURRENCY=10000 REDIS_POOL_SIZE=256 \
    nohup /xp/server/php/php-8.4/bin/php artisan edge:auth-swow \
        >> /var/log/edge-auth/edge-auth.$port.log \
        2>> /var/log/edge-auth/edge-auth.$port.err &
    echo "started :$port pid=$!"
    sleep 0.5
done
```

### 1.5 停止实例

```bash
for port in 9503 9504 9505 9506; do
    pf="/var/run/edge-auth/edge-auth.$port.pid"
    if [[ -f "$pf" ]]; then
        pid=$(cat "$pf")
        kill -TERM "$pid" 2>/dev/null || true
        rm -f "$pf"
        echo "stopped :$port (pid=$pid)"
    fi
done
```

### 1.6 健康检查

```bash
# 端口监听
for p in 9503 9504 9505 9506; do
    /xp/server/php/php-8.4/bin/php -r "\$fp=@fsockopen('127.0.0.1',$p,\$e,\$s,2);if(\$fp){fclose(\$fp);echo ':$p OK\n';}else{echo ':$p FAIL\n';}"
done

# /health 端点
curl -s -o /dev/null -w "%{http_code}" http://127.0.0.1:9503/admin-api/extension/edge/health

# /auth 认证链路（预期 401）
curl -s -o /dev/null -w "%{http_code}" \
    -X POST -d "clientid=&username=&password=" \
    http://127.0.0.1:9503/admin-api/extension/edge/auth
```

### 1.7 回归测试

```bash
/xp/server/php/php-8.4/bin/php extensions/edge/test/verify_simple.php \
    2>&1 | tee /var/log/edge-auth/verify.log

grep "ALL PASSED" /var/log/edge-auth/verify.log && echo "✅ verify passed"
```

### 1.8 systemd 服务

```bash
cat > /etc/systemd/system/edge-auth.service <<'EOF'
[Unit]
Description=EdgeAuth MQTT Auth Server
After=network.target redis.service

[Service]
Type=forking
WorkingDirectory=/path/to/laravel-root
ExecStart=/path/to/laravel-root/extensions/edge/bash/start-cluster.sh
ExecStop=/path/to/laravel-root/extensions/edge/bash/stop-cluster.sh
Restart=on-failure
RestartSec=5
LimitNOFILE=65535

[Install]
WantedBy=multi-user.target
EOF

systemctl daemon-reload
systemctl enable edge-auth.service
systemctl start edge-auth
```

### 1.9 Nginx 配置（可选，本地直连可省略）

> 本地联调时 **NanoMQ 直连 `127.0.0.1:9503`，无需 Nginx**。  
> 如需保留 Nginx 做 TLS 终结，配置如下：

```bash
# upstream
cat > /etc/nginx/conf.d/edge-auth-upstream.conf <<'EOF'
upstream edge_auth_backend {
    least_conn;
    server 127.0.0.1:9503 max_fails=3 fail_timeout=10s;
    server 127.0.0.1:9504 max_fails=3 fail_timeout=10s;
    server 127.0.0.1:9505 max_fails=3 fail_timeout=10s;
    server 127.0.0.1:9506 max_fails=3 fail_timeout=10s;
    keepalive 128;
}
EOF

# 限流
cat > /etc/nginx/conf.d/edge-auth-limit.conf <<'EOF'
limit_req_zone $binary_remote_addr zone=edge_auth_limit:10m rate=100r/s;
limit_req_zone $binary_remote_addr zone=edge_observe_limit:1m rate=10r/s;
EOF

# SSL server（含 /mqtt → /admin-api/extension/edge 重写）
cat > /etc/nginx/conf.d/edge-auth-ssl.conf <<'EOF'
server { listen 80; server_name _; return 301 https://$host$request_uri; }
server {
    listen 443 ssl http2; server_name _;
    ssl_certificate /etc/ssl/certs/edge-auth.crt;
    ssl_certificate_key /etc/ssl/private/edge-auth.key;
    ssl_protocols TLSv1.2 TLSv1.3;
    ssl_ciphers 'ECDHE-ECDSA-AES128-GCM-SHA256:ECDHE-RSA-AES128-GCM-SHA256:ECDHE-ECDSA-AES256-GCM-SHA384:ECDHE-RSA-AES256-GCM-SHA384';
    ssl_prefer_server_ciphers on; ssl_session_cache shared:SSL:10m; ssl_session_timeout 10m;
    add_header X-Frame-Options DENY always;
    add_header X-Content-Type-Options nosniff always;

    location = /mqtt/auth {
        limit_req zone=edge_auth_limit burst=200 nodelay; limit_req_status 429;
        proxy_pass http://edge_auth_backend/admin-api/extension/edge/auth;
        proxy_buffering off; proxy_http_version 1.1;
        proxy_set_header Connection ''; proxy_set_header X-Real-IP $remote_addr;
    }
    location = /mqtt/super {
        limit_req zone=edge_auth_limit burst=200 nodelay; limit_req_status 429;
        proxy_pass http://edge_auth_backend/admin-api/extension/edge/super;
        proxy_buffering off; proxy_http_version 1.1;
        proxy_set_header Connection ''; proxy_set_header X-Real-IP $remote_addr;
    }
    location = /mqtt/acl {
        limit_req zone=edge_auth_limit burst=200 nodelay; limit_req_status 429;
        proxy_pass http://edge_auth_backend/admin-api/extension/edge/acl;
        proxy_buffering off; proxy_http_version 1.1;
        proxy_set_header Connection ''; proxy_set_header X-Real-IP $remote_addr;
    }
    location = /health {
        limit_req zone=edge_observe_limit burst=20 nodelay;
        proxy_pass http://edge_auth_backend/admin-api/extension/edge/health;
        access_log off;
    }
    location = /metrics {
        limit_req zone=edge_observe_limit burst=20 nodelay;
        proxy_pass http://edge_auth_backend/admin-api/extension/edge/metrics;
        access_log off;
    }
    location / { return 404; }
}
EOF

nginx -t && systemctl reload nginx
```

### 1.10 logrotate

```bash
cat > /etc/logrotate.d/edge-auth <<'EOF'
/var/log/edge-auth/*.log /var/log/edge-auth/*.err {
    daily missingok rotate 30 compress delaycompress notifempty
    create 0640 root adm
}
EOF
```

---

## 二、NanoMQ MQTT 部署

### 2.1 证书生成

```bash
mkdir -p /data/nanomq/certs && cd /data/nanomq/certs

# CA
openssl genrsa -out ca.key 2048
openssl req -new -x509 -days 3650 -key ca.key -out ca.crt \
  -subj "/C=CN/ST=Beijing/L=Beijing/O=DagaSmart/OU=IoT/CN=DagaSmart-Root-CA"

# 服务器私钥
openssl genrsa -out server.key 2048

# CSR
openssl req -new -key server.key -out server.csr \
  -subj "/C=CN/ST=Beijing/L=Beijing/O=DagaSmart/OU=IoT/CN=ccs.dagasmart.com"

# SAN 扩展
cat > server.ext <<EOF
authorityKeyIdentifier=keyid,issuer
basicConstraints=CA:FALSE
keyUsage = digitalSignature, keyEncipherment
extendedKeyUsage = serverAuth
subjectAltName = @alt_names

[alt_names]
IP.1 = 127.0.0.1
DNS.1 = ccs.dagasmart.com
DNS.2 = localhost
EOF

# 签发
openssl x509 -req -in server.csr -CA ca.crt -CAkey ca.key -CAcreateserial \
  -out server.crt -days 3650 -sha256 -extfile server.ext

# 验证
openssl x509 -in server.crt -noout -text | grep -A1 "Subject Alternative Name"
openssl verify -CAfile ca.crt server.crt
```

### 2.2 配置文件

```bash
mkdir -p /data/nanomq/conf

cat > /data/nanomq/conf/nanomq.conf <<'EOF'
# ===== 监听器 =====
listeners.ssl {
  bind = "0.0.0.0:8883"
  keyfile  = "/etc/nanomq/certs/server.key"
  certfile = "/etc/nanomq/certs/server.crt"
  cacertfile = "/etc/nanomq/certs/ca.crt"
  verify_peer = false
  fail_if_no_peer_cert = false
}

listeners.tcp {
  bind = "0.0.0.0:1883"
}

# ===== 会话 =====
mqtt.session {
  allow_anonymous = false
  max_packet_size = "1MB"
  max_inflight = 64
  max_mqueue_len = 1024
  retry_interval = "10s"
  keepalive_multiplier = 1.5
}

# ===== HTTP 认证（直连本地 EdgeAuth，不走 Nginx） =====
auth {
  allow_anonymous = false
  password {
    admin = "123456"
    fxc = "123456"
  }
  http {
    enable = true
    auth_url = "http://127.0.0.1:9503/admin-api/extension/edge/auth"
    super_url = "http://127.0.0.1:9503/admin-api/extension/edge/super"
    acl_url  = "http://127.0.0.1:9503/admin-api/extension/edge/acl"
    method = "POST"
    headers.content-type = "application/json"
    timeout = "5s"
    connect_timeout = "3s"
    pool_size = 32
  }
}

# ===== 系统资源 =====
system {
  num_taskq_threads = 16
  max_taskq_threads = 32
  parallel = 8
}

# ===== 日志 =====
log {
  level = warn
  to = "console"
}

http_server {
  enable = false
}
EOF
```

### 2.3 Docker Compose 启动

```bash
cat > /data/nanomq/docker-compose.yml <<'EOF'
services:
  nanomq:
    image: emqx/nanomq:latest-full
    container_name: nanomq
    restart: unless-stopped
    ports:
      - "21884:8883"
      - "21883:1883"
    volumes:
      - /data/nanomq/conf/nanomq.conf:/etc/nanomq.conf:ro
      - /data/nanomq/certs:/etc/nanomq/certs:ro
    command: ["/usr/local/bin/nanomq", "start", "--conf", "/etc/nanomq.conf"]
    security_opt:
      - no-new-privileges:true
    ulimits:
      nofile:
        soft: 65536
        hard: 65536
    logging:
      driver: json-file
      options:
        max-size: "10m"
        max-file: "3"
EOF

cd /data/nanomq
docker-compose down
docker-compose up -d
sleep 3

# 验证
ss -tlnp | grep -E '21883|21884'
docker logs nanomq 2>&1 | head -20
```

### 2.4 客户端验证

```bash
# TLS 握手
openssl s_client -connect 127.0.0.1:21884 -CAfile /data/nanomq/certs/ca.crt

# 明文发布（门禁侧，经 EdgeAuth 认证）
mosquitto_pub -h 127.0.0.1 -p 21883 -u fxc -P '123456' -t door/test -m online

# 订阅
mosquitto_sub -h 127.0.0.1 -p 21883 -u fxc -P '123456' -t "door/+" -v
```

### 2.5 iptables 保护明文端口

```bash
# 放行指定门禁 IP
iptables -A INPUT -p tcp -s 192.168.1.100 --dport 21883 -j ACCEPT
iptables -A INPUT -p tcp --dport 21883 -j DROP

# 或放行整个内网段
iptables -A INPUT -p tcp -s 192.168.1.0/24 --dport 21883 -j ACCEPT
iptables -A INPUT -p tcp --dport 21883 -j DROP

# 查看规则
iptables -L INPUT -n --line-numbers | grep 21883
```

### 2.6 常用运维

```bash
docker ps | grep nanomq
docker logs nanomq 2>&1 | tail -20
docker exec nanomq cat /etc/nanomq.conf
cd /data/nanomq && docker-compose restart nanomq
docker-compose down
```

### 2.7 参数调优与生产建议

#### 核心参数速查

| 配置块 | 参数 | 默认值 | 生产建议 | 说明 |
|--------|------|--------|----------|------|
| `listeners.ssl` | `bind` | — | `0.0.0.0:8883` | TLS 端口（Docker 映射 21884） |
| `listeners.tcp` | `bind` | — | `0.0.0.0:1883` | 明文端口（Docker 映射 21883，用 iptables 限制内网） |
| `mqtt.session` | `allow_anonymous` | `true` | **`false`** | 禁止匿名，强制走 EdgeAuth |
| `mqtt.session` | `max_packet_size` | `1MB` | `1MB` | 单包上限，防 OOM |
| `mqtt.session` | `max_inflight` | `32` | `64–128` | QoS 飞行窗口，高吞吐可调大 |
| `mqtt.session` | `max_mqueue_len` | — | `1024` | 离线消息队列长度 |
| `mqtt.session` | `retry_interval` | — | `10s` | 重试间隔 |
| `mqtt.session` | `keepalive_multiplier` | — | `1.5` | 心跳容忍倍数 |
| `auth.http` | `timeout` | — | `5s` | EdgeAuth 响应超时 |
| `auth.http` | `connect_timeout` | — | `3s` | 连接 EdgeAuth 超时 |
| `auth.http` | `pool_size` | — | `32` | HTTP 客户端连接池 |
| `system` | `num_taskq_threads` | — | `CPU×2` | 异步任务线程 |
| `system` | `parallel` | — | `CPU 核数` | 并行处理数 |
| `log` | `level` | `warn` | `warn` | 排错时临时改 `debug` |

#### 性能压测参考

| 场景 | 并发连接 | 消息速率 | 推荐配置 |
|------|----------|----------|----------|
| 门禁设备（低频） | < 500 | < 100 msg/s | 默认即可 |
| 工业采集（中频） | 2K–5K | 1K–5K msg/s | `max_inflight=128`, `pool_size=64` |
| 高吞吐推送 | > 10K | > 10K msg/s | 扩 `parallel`、EdgeAuth 扩到 8 worker |

#### 常见坑位

| 现象 | 原因 | 解决 |
|------|------|------|
| 8883 连不上 | 证书挂载错误 | `docker exec nanomq ls /etc/nanomq/certs` 确认 |
| 认证超时 / 设备频繁断连 | `timeout` 太短或 EdgeAuth 慢 | 调大到 `5s`，确认 Redis 池充足 |
| `too many open files` | ulimit 不够 | Docker 已设 65536，宿主机确认 limits.conf |
| 内存持续增长 | `max_mqueue_len` 过大 | 降低队列或开启 `expiry_interval` |

---

## 三、数据库急救（PostgreSQL 外键死锁）

> 在 `master` 库（`ccs_admin`）执行。  
> **根因**：`gateway_id` 全 NULL 时外键约束对象仍存在，PG 禁止 drop 父表。  
> **解法**：先拆约束，再删表。

```sql
-- 1. 拆外键
ALTER TABLE mcs_edge_gateway_config_snapshot
    DROP CONSTRAINT IF EXISTS mcs_edge_gateway_config_snapshot_config_id_foreign;

ALTER TABLE mcs_edge_gateway_config
    DROP CONSTRAINT IF EXISTS mcs_edge_gateway_config_gateway_id_foreign;

-- 2. 按子→父顺序 drop
DROP TABLE IF EXISTS mcs_edge_gateway_config_snapshot;
DROP TABLE IF EXISTS mcs_edge_gateway_config;
DROP TABLE IF EXISTS mcs_edge_gateway;

-- 3. 清 migrations 记录
DELETE FROM migrations
WHERE migration IN (
    '2026_10_26_080000_create_mcs_edge_gateway_table',
    '2026_10_26_080004_create_mcs_edge_gateway_config_table',
    '2026_10_26_080005_create_mcs_edge_gateway_config_snapshot_table'
);
```

**懒人版（开发环境）：**

```sql
DROP TABLE IF EXISTS mcs_edge_gateway CASCADE;
```

---

## 四、迁移回滚（三张表）

> Laravel 13 迁移 `down()` 逻辑：先拆外键 → 无条件 drop → 零残留。  
> rollback 顺序自动为：

```
① snapshot.down()  → dropForeign(config_id) → drop 表
② config.down()    → dropForeign(gateway_id) → drop 表
③ gateway.down()   → 直接 drop 表
```

```bash
php artisan migrate:rollback --step=3
```

若数据库仍处于死锁状态，先跑**第三节 SQL** 解封，再重新迁移：

```bash
php artisan migrate
```

---

## 五、EdgeAuth 源码级实操补充

### 5.1 artisan 命令完整参数

```bash
php artisan edge:auth-swow [--workers=N] [--port=N]

# 示例：4 进程、端口 9503（REUSEPORT 模式）
php artisan edge:auth-swow --workers=4 --port=9503

# 单进程模式（无 pcntl 或 Windows 自动降级）
php artisan edge:auth-swow --workers=1 --port=9503
```

| 参数 | 默认值 | 说明 |
|------|--------|------|
| `--workers` | `4` | fork 子进程数；Windows / 无 pcntl 时强制单进程 |
| `--port` | `9503` | 监听端口；多 worker 自动启用 `REUSEPORT` |

**进程行为：**
- 父进程负责 fork + 信号监听（SIGTERM / SIGINT / SIGHUP）
- 子进程标题：`edgeauth-worker-{id}`
- 子进程异常退出父进程自动拉起（5 秒优雅退出窗口，超时 SIGKILL）
- `SIGHUP` → 平滑重载所有 worker
- PID 文件：`/var/run/edge-auth/edge-auth.{port}.pid`

### 5.2 环境变量 / 常量清单

常量由 `bootstrap.php` 从 `EdgeGatewayConfig` 数据库配置读取，也可通过环境变量在启动时覆盖：

| 常量 | 默认值 | 说明 |
|------|--------|------|
| `AUTH_PORT` | `9503` | 监听端口 |
| `REDIS_HOST` | `127.0.0.1` | Redis 主机 |
| `REDIS_PORT` | `16379` | Redis 端口 |
| `REDIS_PASSWORD` | `uApDVQD7` | Redis 密码 |
| `REDIS_DB` | `2` | Redis 库号 |
| `REDIS_POOL_SIZE` | `256` | 连接池大小（per worker） |
| `MAX_CONCURRENCY` | `10000` | 全局最大并发 |
| `MAX_CONCURRENCY_PER_DEVICE` | `2` | 单设备最大并发（REUSEPORT 下 × worker 数） |
| `CACHE_TTL` | `300` | Redis 缓存 TTL（秒） |
| `LOCAL_CACHE_TTL` | `60` | 本地内存缓存 TTL（秒） |
| `RATE_MAX` | `10` | 限流窗口内最大请求数 |
| `RATE_TTL` | `60` | 限流窗口大小（秒） |
| `TIME_DRIFT` | `300` | 签名时间漂移容差（±300s） |
| `BACKLOG` | `4096` | TCP accept 队列长度 |
| `AUTH_HTTP_PREFIX` | `/admin-api/extension/edge` | 路由基准前缀 |

```bash
export AUTH_PORT=9503
export REDIS_DB=2
export MAX_CONCURRENCY=20000
export REDIS_POOL_SIZE=512
php artisan edge:auth-swow --workers=4
```

### 5.3 路由端点清单

所有路由挂在 `AUTH_HTTP_PREFIX`（`/admin-api/extension/edge`）下：

| 路径 | 方法 | 用途 | 调用方 |
|------|------|------|--------|
| `/auth` | POST | MQTT 认证 | NanoMQ `auth.http` |
| `/super` | POST | 超级用户判定 | NanoMQ `auth.http` |
| `/acl` | POST | ACL 鉴权 | NanoMQ `auth.http` |
| `/health` | GET | 存活探针 | 监控 / LB |
| `/metrics` | GET | 指标快照 | Prometheus |

### 5.4 seed_redis.php 密钥注入工具

**位置：** `extensions/edge/test/seed_redis.php`  
**密钥来源：** 脚本内置数组（与 DB `edge_authorize.secret` 一致）  
**Redis Key 格式：** `device:secret:{clientId}:{username}`

```bash
/xp/server/php/php-8.4/bin/php extensions/edge/test/seed_redis.php              # 正常写入（存在则跳过）
php seed_redis.php --dry-run              # 只打印不写入
php seed_redis.php --force                # 覆盖已存在密钥
php seed_redis.php --host 10.0.0.5 --port 6379 --db 2 --password 'xxx'
php seed_redis.php --timeout 5.0
php seed_redis.php --help
```

| 选项 | 默认值 | 说明 |
|------|--------|------|
| `--dry-run` | off | 只打印不写入 |
| `--force` | off | 覆盖已存在 key |
| `--host` | `127.0.0.1` | Redis 主机 |
| `--port` | `16379` | Redis 端口 |
| `--password` | `uApDVQD7` | Redis 密码 |
| `--db` | `2` | 数据库号 |
| `--timeout` | `2.0` | 连接超时（支持小数） |

**退出码：** `0` 全部成功 · `1` 参数错误/连接失败 · `2` 部分写入失败

**内置设备：**

| clientId | username | 用途 |
|----------|----------|------|
| `device001` | `user001` | 测试设备 |
| `device002` | `user001` | 测试设备 |
| `device003` | `user002` | 测试设备 |
| `gateway01` | `admin` | 网关管理 |
| `bench_client` | `bench_user` | 压测客户端 |

### 5.5 verify_simple.php 回归测试

**位置：** `extensions/edge/test/verify_simple.php`  
**前置：** EdgeAuth 已启动、Redis 已注入密钥、curl 可用

```bash
/xp/server/php/php-8.4/bin/php extensions/edge/test/verify_simple.php
# 自定义地址与超时
php verify_simple.php http://127.0.0.1:9503/admin-api/extension/edge --timeout 5.0
```

**测试矩阵：**

| 测试 | 验证点 | 预期 |
|------|--------|------|
| Test 1 | `/health` | 200 |
| Test 2 | `/metrics` | 200 |
| Test 3 | 空字段认证 | 401 |
| Test 4 | 不存在设备 | 401 |
| Test 5 | 错误密钥签名 | 401 |
| Test 6 | 所有真实设备 HMAC-SHA256 | 200 |
| Test 7 | 过期时间戳（±300s） | 401 |

**协议格式：**

```
password = b64Sign|timestamp|nonce
b64Sign  = base64url(HMAC-SHA256(clientId|username|timestamp|nonce, secret))
timestamp= Unix 时间戳（服务端校验 ±TIME_DRIFT）
nonce    = 随机 16 进制串
```

**退出码：** `0` ALL PASSED · `1` 有失败或 Redis 不可达

### 5.6 故障排查速查

| 现象 | 排查命令 | 可能原因 |
|------|----------|----------|
| `Swow extension not loaded` | `php -m \| grep swow` | Swow 未安装 |
| fork 失败 / 单进程 | `php -m \| grep pcntl` | pcntl 缺失 |
| Redis DEGRADED 日志 | `redis-cli -h ... ping` | Redis 不可达/密码错 |
| 认证全部 401 | `php seed_redis.php --dry-run` | 密钥未注入或格式错 |
| 端口占用 | `ss -tlnp \| grep 9503` | 旧进程未退出 |
| worker 频繁重启 | `tail -f /var/log/edge-auth/*.err` | 代码异常/内存不足 |
| verify 连接失败 | 自动 waitForPort | 服务未启动/端口错 |

**快速重启流程：**

```bash
# 停
for port in 9503 9504 9505 9506; do
    pid=$(cat /var/run/edge-auth/edge-auth.$port.pid 2>/dev/null)
    [[ -n "$pid" ]] && kill -TERM "$pid" && rm -f /var/run/edge-auth/edge-auth.$port.pid
done

# 清旧密钥（可选）
redis-cli -h 127.0.0.1 -p 16379 -a uApDVQD7 -n 2 --no-auth-warning FLUSHDB

# 重注
php extensions/edge/test/seed_redis.php --force

# 起
for port in 9503 9504 9505 9506; do
    AUTH_PORT=$port REDIS_DB=2 MAX_CONCURRENCY=10000 REDIS_POOL_SIZE=256 \
    nohup /xp/server/php/php-8.4/bin/php artisan edge:auth-swow \
        >> /var/log/edge-auth/edge-auth.$port.log 2>> /var/log/edge-auth/edge-auth.$port.err &
done

# 验
php extensions/edge/test/verify_simple.php
```

### 5.7 文件结构速览

```
extensions/edge/
├── bin/server.php              # Swow 服务入口
├── config/secrets.php          # ⚠️ 已废弃
├── test/
│   ├── seed_redis.php          # 密钥注入工具
│   └── verify_simple.php       # 全链路回归测试
├── bootstrap.php               # 配置预热 → define 常量
├── helper.php                  # 工具函数
├── hmac.php                    # HMAC-SHA256 校验
├── auth.php                    # /auth /super /acl 处理器
└── Console/Commands/
    └── EdgeAuthSwowCommand.php # artisan edge:auth-swow
```

---

## 六、EdgeAuth Prometheus 指标字段解释

> 端点：`GET http://127.0.0.1:9503/admin-api/extension/edge/metrics`  
> 返回 JSON，供 Prometheus / Grafana 消费。

### 6.1 指标完整结构

```json
{
  "redis": {
    "total": 256,
    "free": 254,
    "used": 2
  },
  "cache": { "size": 42 },
  "device_concurrency": {
    "active_devices": 38,
    "total_slots": 76,
    "utilization": 0.18
  },
  "global_concurrency": {
    "active": 156,
    "max": 10000,
    "utilization": 0.0156
  },
  "uptime_seconds": 3600,
  "worker_id": 1
}
```

### 6.2 字段逐条解释

#### `redis` — 连接池状态

| 字段 | 类型 | 说明 |
|------|------|------|
| `total` | int | 连接池总大小（= `REDIS_POOL_SIZE`） |
| `free` | int | 空闲连接数 |
| `used` | int | 被占用数（`total - free`） |

告警：`free == 0` 池耗尽；`used/total > 0.8` 需扩容。

#### `cache` — 本地内存缓存

| 字段 | 类型 | 说明 |
|------|------|------|
| `size` | int | 当前缓存条目数（TTL = `LOCAL_CACHE_TTL`） |

> 本地缓存 per-worker 独立，4 worker 时总缓存 = `size × 4`。

#### `device_concurrency` — 单设备并发

| 字段 | 类型 | 说明 |
|------|------|------|
| `active_devices` | int | 当前有活跃连接的设备数 |
| `total_slots` | int | 并发槽位总数（= `active_devices × MAX_CONCURRENCY_PER_DEVICE`） |
| `utilization` | float | 利用率（0.0–1.0） |

告警：`utilization > 0.9` 即将打满；`active_devices` 突增可能为连接风暴。

#### `global_concurrency` — 全局并发

| 字段 | 类型 | 说明 |
|------|------|------|
| `active` | int | 当前全局活跃请求数 |
| `max` | int | 上限（`MAX_CONCURRENCY`） |
| `utilization` | float | 利用率（0.0–1.0） |

告警：`utilization > 0.8` 需扩并发或加 worker。

#### 顶层元数据

| 字段 | 类型 | 说明 |
|------|------|------|
| `uptime_seconds` | int | worker 运行秒数（重启归零） |
| `worker_id` | int | 响应请求的 worker 编号 |

### 6.3 Prometheus 抓取配置

```yaml
# prometheus.yml
scrape_configs:
  - job_name: 'edgeauth'
    metrics_path: '/admin-api/extension/edge/metrics'
    static_configs:
      - targets: ['127.0.0.1:9503', '127.0.0.1:9504', '127.0.0.1:9505', '127.0.0.1:9506']
    scrape_interval: 5s
    timeout: 3s
```

### 6.4 Grafana 关键查询

```promql
(redis_used / redis_total) * 100                     # 连接池使用率
(global_concurrency_utilization) * 100               # 全局并发利用率
topk(10, device_concurrency_utilization)             # 设备并发 Top10
up{job="edgeauth"}                                   # 实例存活
```

### 6.5 health 与 metrics 端点差异

| 端点 | 用途 | 轻量 | 建议限流 |
|------|------|------|----------|
| `/health` | 存活探针（LB / K8s） | ✅ 极轻 | 10r/s |
| `/metrics` | 监控指标（Prometheus） | ⚠️ 需聚合 | 10r/s |
| `/auth` | 认证 | — | 100r/s burst 200 |

---

## 七、版本与兼容性备忘

| 组件 | 版本要求 | 备注 |
|------|----------|------|
| PHP | ≥ 8.4 | `declare(strict_types=1)` |
| Swow | 最新版 | `Swow\Coroutine`、`Swow\Psr7\Server` |
| pcntl | 可选 | 无则降级单进程 |
| Redis 扩展 | ≥ 5.0 | `Redis::connect()` / `ping()` |
| PostgreSQL | ≥ 13 | 迁移使用 `DO $$` 匿名块 |
| NanoMQ | latest-full | `emqx/nanomq:latest-full` |
| Laravel | 13.x | 迁移匿名类、artisan 命令 |

---

## 八、快速联调顺序（一步到位）

```bash
# 1. EdgeAuth 起服务
cd /path/to/laravel-root
php artisan edge:auth-swow --workers=4 --port=9503

# 2. 注密钥
php extensions/edge/test/seed_redis.php --force

# 3. EdgeAuth 自测
php extensions/edge/test/verify_simple.php

# 4. 重启 NanoMQ 使 auth_url 生效
cd /data/nanomq && docker-compose restart nanomq

# 5. MQTT 客户端发消息验证完整认证链路
mosquitto_pub -h 127.0.0.1 -p 21883 -u fxc -P '123456' -t door/test -m online
mosquitto_sub -h 127.0.0.1 -p 21883 -u fxc -P '123456' -t "door/+" -v
```

---

## 附录 A：完整文件结构

```
extensions/edge/
├── bin/
│   └── server.php                  # Swow 服务入口
├── config/
│   └── secrets.php                 # ⚠️ 已废弃
├── test/
│   ├── seed_redis.php              # 密钥注入工具
│   └── verify_simple.php           # 全链路回归测试
├── bootstrap.php                   # 配置预热 → define 常量
├── helper.php                      # 工具函数
├── hmac.php                        # HMAC-SHA256 校验
├── auth.php                        # /auth /super /acl 处理器
└── Console/Commands/
    └── EdgeAuthSwowCommand.php     # artisan edge:auth-swow

/data/nanomq/
├── conf/nanomq.conf                # NanoMQ 主配置（直连 EdgeAuth）
├── certs/                          # TLS 证书
│   ├── ca.crt / ca.key
│   ├── server.crt / server.key
│   └── server.csr / server.ext
└── docker-compose.yml              # Docker 编排
```

---

## 附录 B：快速索引

| 场景 | 章节 |
|------|------|
| EdgeAuth 全流程部署 | [一](#一edgeauth-部署) |
| NanoMQ + TLS + Docker | [二](#二nanomq-mqtt-部署) |
| NanoMQ 参数调优 | [2.7](#27-参数调优与生产建议) |
| PG 外键死锁急救 | [三](#三数据库急救postgresql-外键死锁) |
| 迁移回滚 | [四](#四迁移回滚三张表) |
| 命令参数 / 常量 / 路由 | [五](#五edgeauth-源码级实操补充) |
| seed_redis 用法 | [5.4](#54-seed_redisphp-密钥注入工具) |
| verify 回归测试 | [5.5](#55-verify_simplephp-回归测试) |
| Prometheus 指标 | [六](#六edgeauth-prometheus-指标字段解释) |
| 故障排查 | [5.6](#56-故障排查速查) |
| 一步联调 | [八](#八快速联调顺序一步到位) |
