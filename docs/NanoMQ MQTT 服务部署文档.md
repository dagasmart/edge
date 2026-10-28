# NanoMQ MQTT 服务部署文档

## 一、项目背景与目标

部署一套 **NanoMQ MQTT Broker** 服务，同时承载两类客户端：

| 客户端类型 | 网络环境 | 协议要求 | 端口 |
|-----------|---------|---------|------|
| 门禁设备 | 只能填 IP，无法改 hosts，无法导入 CA | 明文 TCP | 21883 |
| MQTTX 管理端 | 可控环境 | TLS 单向认证 | 21884 |

**核心约束**：门禁设备无法配置 hosts、无法安装自签 CA 证书，因此必须提供明文 TCP 接入；管理端通过 TLS 保证链路安全。

---

## 二、环境信息

| 项目 | 值 |
|------|-----|
| 服务器 IP | `1.15.51.149` |
| 域名 | `ccs.dagasmart.com`（解析到 `1.15.51.149`） |
| Docker 镜像 | `emqx/nanomq:latest-full`（实际版本 0.25.7） |
| 容器名 | `nanomq` |
| 配置文件路径 | `/data/nanomq/conf/nanomq.conf` |
| 证书路径 | `/data/nanomq/certs/` |
| 部署方式 | docker-compose（Compose v1） |

---

## 三、证书准备（自签 CA + 服务器证书）

> 门禁不走 TLS 不需要证书；服务器证书带 IP SAN，便于 MQTTX 通过 IP 或域名连接时校验通过。

### 3.1 生成 CA 私钥与根证书

```bash
mkdir -p /data/nanomq/certs && cd /data/nanomq/certs

# CA 私钥
openssl genrsa -out ca.key 2048

# CA 根证书（10 年）
openssl req -new -x509 -days 3650 -key ca.key -out ca.crt \
  -subj "/C=CN/ST=Beijing/L=Beijing/O=DagaSmart/OU=IoT/CN=DagaSmart-Root-CA"
```

### 3.2 生成服务器证书（含 IP + DNS SAN）

```bash
# 服务器私钥
openssl genrsa -out server.key 2048

# 证书签名请求配置（关键：包含 IP 和 DNS 的 SAN 扩展）
cat > server.ext <<EOF
authorityKeyIdentifier=keyid,issuer
basicConstraints=CA:FALSE
keyUsage = digitalSignature, keyEncipherment
extendedKeyUsage = serverAuth
subjectAltName = @alt_names

[alt_names]
IP.1 = 1.15.51.149
DNS.1 = ccs.dagasmart.com
DNS.2 = localhost
EOF

# 签发服务器证书（由自签 CA 签署）
openssl x509 -req -in server.csr -CA ca.crt -CAkey ca.key -CAcreateserial \
  -out server.crt -days 3650 -sha256 -extfile server.ext
```

### 3.3 验证证书链

```bash
# 确认 SAN 包含 IP 和 DNS
openssl x509 -in server.crt -noout -text | grep -A1 "Subject Alternative Name"

# 验证服务器证书由 CA 签署
openssl verify -CAfile ca.crt server.crt
# 预期输出：server.crt: OK
```

---

## 四、NanoMQ 配置文件

路径：`/data/nanomq/conf/nanomq.conf`

```hocon
# ===== TLS 监听器（单向 TLS，给 MQTTX 管理端用）=====
listeners.ssl {
  bind = "0.0.0.0:8883"
  keyfile  = "/etc/nanomq/certs/server.key"
  certfile = "/etc/nanomq/certs/server.crt"
  cacertfile = "/etc/nanomq/certs/ca.crt"
  verify_peer = false
  fail_if_no_peer_cert = false
}

# ===== 明文 TCP（给门禁用）=====
listeners.tcp {
  bind = "0.0.0.0:1883"
}

# ===== 会话安全 =====
mqtt.session {
  allow_anonymous = false
  max_packet_size = 1MB
  max_inflight = 32
}

# ===== 认证（门禁 + 管理端共用）=====
auth {
  allow_anonymous = false
  password {
    admin = "123456"
    fxc = "123456"
  }
}

# ===== 管理面关闭 =====
http_server {
  enable = false
}

# ===== 日志 =====
log {
  level = warn
}
```

### 关键配置说明

| 配置项 | 值 | 说明 |
|--------|-----|------|
| `verify_peer` | `false` | 单向 TLS，不验证客户端证书 |
| `fail_if_no_peer_cert` | `false` | 允许客户端不提供证书 |
| `allow_anonymous` | `false` | 禁止匿名连接，必须提供账号密码 |
| `password` | 明文 | 当前为明文密码，生产环境建议改用 bcrypt |

---

## 五、Docker Compose 部署

路径：`/data/nanomq/docker-compose.yml`

```yaml
services:
  nanomq:
    image: emqx/nanomq:latest-full
    container_name: nanomq
    restart: unless-stopped
    ports:
      - "21884:8883"   # 宿主机 21884 → 容器 TLS 8883（MQTTX）
      - "21883:1883"   # 宿主机 21883 → 容器明文 1883（门禁）
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
```

### ⚠️ 踩坑要点

1. **必须显式指定 `--conf`**  
   NanoMQ 不会自动读取挂载到 `/etc/nanomq.conf` 的文件，启动日志会显示：  
   `Config file is not specified, using default config file: /usr/local/bin/nanomq.conf`  
   必须通过在 `command` 中显式指定 `--conf /etc/nanomq.conf` 来加载配置。

2. **二进制路径是 `/usr/local/bin/nanomq`**  
   不是 `/usr/bin/nanomq`，写错会导致 `exec: /usr/bin/nanomq: not found`。

3. **TCP 与 SSL 监听器可共存**  
   NanoMQ 原生支持同时开启 `listeners.tcp` 和 `listeners.ssl`，两个监听器独立运行。

### 启动命令

```bash
cd /data/nanomq
docker-compose down
docker-compose up -d
sleep 3

# 验证
ss -tlnp | grep -E '21883|21884'
docker logs nanomq 2>&1 | head -20
```

预期日志：
```
NanoMQ serving on tcp://0.0.0.0:1883
NanoMQ serving on ssl://0.0.0.0:8883
```

---

## 六、客户端配置

### 6.1 门禁设备（明文 TCP）

| 字段 | 值 |
|------|-----|
| MQTT 启用 | ✅ |
| 服务器地址 | `1.15.51.149` |
| 端口 | `21883` |
| 用户名 | `fxc` |
| 密码 | `123456` |
| 消息等级（QoS） | `2` |
| TLS / MQTT证书 | ❌ 关闭 |
| Client ID | 留空或设备编号 |

连接状态显示 **"正常连接"** 即表示链路打通。

### 6.2 MQTTX 管理端（TLS）

| 字段 | 值 |
|------|-----|
| Protocol | `mqtts://` |
| Host | `ccs.dagasmart.com` |
| Port | `21884` |
| Username | `admin` |
| Password | `123456` |
| TLS | ✅ 开启 |
| SSL 安全 / Verify | ❌ **关闭** |
| CA Certificate | 选择 `ca.crt` |
| Client Certificate | ❌ 留空 |
| Client Key | ❌ 留空 |

> **关键点**：单向 TLS 下，MQTTX 只需导入 CA 证书即可验证服务器身份，无需提供客户端证书。若勾选 "SSL 安全" 则会强制双向认证导致握手失败。

---

## 七、验证测试

### 7.1 验证 TLS 握手

```bash
openssl s_client -connect ccs.dagasmart.com:21884 -CAfile /data/nanomq/certs/ca.crt
```

输出中应有：
```
Verify return code: 0 (ok)
```

### 7.2 验证明文连接（门禁侧）

```bash
mosquitto_pub -h 127.0.0.1 -p 21883 -u fxc -P '123456' -t door/test -m online
```

无报错即表示认证通过。

### 7.3 发布/订阅联调

```bash
# 终端 1：订阅
mosquitto_sub -h 127.0.0.1 -p 21883 -u fxc -P '123456' -t "door/+" -v

# 终端 2：发布
mosquitto_pub -h 127.0.0.1 -p 21883 -u fxc -P '123456' -t "door/entry" -m "card:12345"
```

---

## 八、防火墙（保护明文端口）

1883 明文端口只对门禁 IP 开放，拒绝其他来源：

```bash
# 假设门禁 IP 为 192.168.1.100
iptables -A INPUT -p tcp -s 192.168.1.100 --dport 1883 -j ACCEPT
iptables -A INPUT -p tcp --dport 1883 -j DROP

# 查看规则
iptables -L INPUT -n --line-numbers | grep 1883
```

若门禁 IP 为 DHCP 动态分配，可放行整个内网段：
```bash
iptables -A INPUT -p tcp -s 192.168.1.0/24 --dport 1883 -j ACCEPT
iptables -A INPUT -p tcp --dport 1883 -j DROP
```

---

## 九、常用运维命令

```bash
# 查看容器状态
docker ps | grep nanomq

# 查看日志
docker logs nanomq 2>&1 | tail -20

# 查看监听端口
ss -tlnp | grep -E '21883|21884'

# 进入容器查看配置
docker exec nanomq cat /etc/nanomq.conf

# 重启服务
docker-compose restart nanomq

# 停止并清理
docker-compose down
```

---

## 十、安全加固建议（后续）

| 优先级 | 措施 | 说明 |
|--------|------|------|
| 高 | 密码改为 bcrypt | 明文密码存在配置文件有泄露风险 |
| 高 | 1883 端口加 iptables 白名单 | 防止公网扫描爆破 |
| 中 | 锁定镜像版本 | 将 `latest-full` 改为 `emqx/nanomq:0.25.7-full` |
| 中 | 恢复 mTLS 双向认证 | 门禁走明文，管理端走 mTLS 进一步加固 |
| 低 | 启用日志审计 | 将 `log.level` 调整为 `info` 记录连接事件 |
| 低 | 定期轮换证书 | 当前证书有效期 10 年，建议缩短至 1-2 年 |

---

## 十一、问题排查速查表

| 现象 | 原因 | 解决方案 |
|------|------|---------|
| `Config file is not specified, using default config file` | 未指定 `--conf` | `command` 中加 `--conf /etc/nanomq.conf` |
| `exec: /usr/bin/nanomq: not found` | 二进制路径错误 | 改为 `/usr/local/bin/nanomq` |
| `Bind for 0.0.0.0:21884 failed: port already allocated` | 端口被占用 | `ss -tlnp \| grep 21884` 查找并释放 |
| `Client network socket disconnected before secure TLS connection was established` | MQTTX TLS 配置错误 | 确认端口 21884、传 ca.crt、关闭 SSL 安全 |
| 门禁连不上 | TLS/证书未关闭 | 确认门禁侧 TLS 和 MQTT证书均为关闭状态 |
| 日志刷 `Cryptographic error rv:26` | 有客户端用明文连 TLS 端口（或反之） | 检查客户端协议与端口是否匹配 |
| 1883 端口不监听 | SSL 和 TCP 监听器冲突 / 配置未加载 | 确认 `--conf` 生效，查看 `docker logs` |

---

## 十二、最终架构图

```
┌──────────────┐                    ┌──────────────┐
│   门禁设备    │                    │  MQTTX 客户端 │
│  (IP 直连)   │                    │  (管理端)    │
└──────┬───────┘                    └──────┬───────┘
       │ 明文 TCP                         │ TLS (mqtts)
       │ 21883:1883                       │ 21884:8883
       │ user: fxc                        │ user: admin
       │ TLS ❌                           │ TLS ✅ + ca.crt
       ▼                                  ▼
┌──────────────────────────────────────────────────┐
│            服务器 1.15.51.149                     │
│  ┌────────────────────────────────────────────┐  │
│  │  Docker: nanomq (emqx/nanomq:0.25.7-full) │  │
│  │                                            │  │
│  │  listeners.tcp  → 0.0.0.0:1883  (明文)    │  │
│  │  listeners.ssl  → 0.0.0.0:8883  (TLS)    │  │
│  │                                            │  │
│  │  auth: admin / fxc  (密码认证)             │  │
│  │  allow_anonymous: false                    │  │
│  └────────────────────────────────────────────┘  │
│                                                  │
│  iptables: 1883 仅放行门禁 IP                     │
└──────────────────────────────────────────────────┘
```
