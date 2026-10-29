# EdgeAuth 部署说明

## 目录结构

\`\`\`
extensions/edge/
├── bash/
│   ├── deploy.sh              # 总入口（seed → build → start → health → verify）
│   ├── start-cluster.sh       # 集群启动（systemd ExecStart）
│   ├── stop-cluster.sh        # 集群停止（systemd ExecStop）
│   └── reload-cluster.sh      # 滚动重启
├── config/
│   └── secrets.php            # 设备密钥（统一命名，复数）
└── test/
├── verify_simple.php      # 协议验证（12 项断言）
└── seed_redis.php         # Redis 预热脚本（独立运行用）
\`\`\`

## 对齐契约（部署前必读）

| 项目 | 约定 |
|------|------|
| Redis key 格式 | `device:secret:{clientid}:{username}`（**冒号，不替换**） |
| 密钥文件名 | **`secrets.php`**（全部脚本统一，无单数 secret.php） |
| systemd 引用 | `start-cluster.sh` / `stop-cluster.sh`（与实际文件名一致） |
| 启动方式 | `php artisan edge:auth-swow`（所有脚本统一） |
| 端口 | 9503–9506 |
| Redis | 127.0.0.1:16379 db=2 密码 uApDVQD7 |
| 协议 | b64Sign\|timestamp\|nonce，HMAC-SHA256 raw binary，±300s |

## 快速部署

\`\`\`bash
# 预演
sudo bash extensions/edge/bash/deploy.sh --dry-run

# 正式部署
sudo bash extensions/edge/bash/deploy.sh
\`\`\`

## 自检命令

\`\`\`bash
# 确认无单数引用残留
grep -rn "secret\.php" extensions/edge/    # 只应命中 secrets.php

# 确认无冒号替换残留
grep -rn '${key/:/-}' extensions/edge/      # 应无命中

# 确认 Redis key 格式正确
redis-cli -h 127.0.0.1 -p 16379 -a uApDVQD7 -n 2 --no-auth-warning KEYS "device:secret:*"
\`\`\`

## 运维操作

### 状态查看

\`\`\`bash
sudo bash extensions/edge/bash/deploy.sh status
# 或
systemctl status edge-auth.service
\`\`\`

### 滚动重启（不停服）

\`\`\`bash
sudo bash extensions/edge/bash/reload-cluster.sh
\`\`\`

### 强制重载（Kill + 重启）

当 `stop-cluster.sh` 无法干净退出、或需要紧急重置所有实例时，按以下顺序操作：

\`\`\`bash
# 1. 正规停（最干净）
sudo bash extensions/edge/bash/stop-cluster.sh

# 2. 兜底：确保没有任何 edge:auth-swow 残留
sudo pkill -f "artisan edge:auth-swow" 2>/dev/null || true
sudo fuser -k 9503/tcp 9504/tcp 9505/tcp 9506/tcp 2>/dev/null || true

# 3. 确认全清干净
pgrep -fa "edge:auth-swow" || echo "✅ 无残留进程"
ss -tlnp | grep -E '950[3-6]' || echo "✅ 端口全释放"

ss -tlnp | grep 9503

# 4. 重载 systemd
sudo systemctl daemon-reload

# 5. 重新启动
sudo bash extensions/edge/bash/start-cluster.sh
# 或走 systemd：
# sudo systemctl restart edge-auth.service

# 6. 健康检查
for p in 9503 9504 9505 9506; do
curl -s -o /dev/null -w ":${p} → %{http_code}\n" --max-time 2 http://127.0.0.1:${p}/health
done
\`\`\`

**一步到位版（单条命令）：**

\`\`\`bash
sudo bash extensions/edge/bash/stop-cluster.sh && \
sudo pkill -f "artisan edge:auth-swow" 2>/dev/null; \
sudo fuser -k 9503/tcp 9504/tcp 9505/tcp 9506/tcp 2>/dev/null; \
sudo systemctl daemon-reload && \
sudo bash extensions/edge/bash/start-cluster.sh
\`\`\`

> ⚠️ **注意**：`pkill -f "artisan edge:auth-swow"` 只匹配该特定命令，不会影响其他 PHP 进程。
> 如果确认机器上没有其他 PHP 业务且 Swow worker 仍未退出，可最后补一刀：
> `sudo killall -9 php`（慎用）。

**重载后验证 Redis 密钥未被清掉：**

\`\`\`bash
redis-cli -h 127.0.0.1 -p 16379 -a uApDVQD7 -n 2 --no-auth-warning KEYS "device:secret:*"
\`\`\`

应返回 5 个冒号格式 key：
\`\`\`
device:secret:device001:user001
device:secret:device002:user001
device:secret:device003:user002
device:secret:gateway01:admin
device:secret:bench_client:bench_user
\`\`\`

**最终确认：跑 verify**

\`\`\`bash
php extensions/edge/test/verify_simple.php
# 期望：12/12 ALL PASSED 🎯
\`\`\`

## 变更记录

| 日期 | 修正 |
|------|------|
| 2026-10-28 | seed_redis key 冒号替换 bug 修复（${key/:/-} → ${key}） |
| 2026-10-28 | systemd 引用对齐到 start-cluster.sh / stop-cluster.sh |
| 2026-10-28 | 文件名统一：secret.php → secrets.php（三处同步） |
| 2026-10-28 | secrets.php 注释中错误的短横线说明更正为冒号 |
| 2026-10-28 | 新增强制重载（Kill + 重启）操作文档 |



### 实时健康巡检（带异常告警）

巡检脚本位于 `extensions/edge/bash/edge-health.sh`，对 health / metrics 端点做持续探测，异常时通过 syslog + 可选 webhook 告警，服务恢复时自动通知。

**目录结构更新：**

\`\`\`
extensions/edge/
├── bash/
│   ├── deploy.sh
│   ├── start-cluster.sh
│   ├── stop-cluster.sh
│   ├── reload-cluster.sh
│   └── edge-health.sh          # ← 健康巡检 + 异常告警
├── config/
│   └── secrets.php
└── test/
├── verify_simple.php
└── seed_redis.php
\`\`\`

**快速使用：**

\`\`\`bash
# 单次检查（调试用）
bash extensions/edge/bash/edge-health.sh

# 守护模式（每 5 秒巡检，后台运行）
bash extensions/edge/bash/edge-health.sh --daemon --interval 5 &

# 或 nohup 持久化
nohup bash extensions/edge/bash/edge-health.sh --daemon --interval 5 >/dev/null 2>&1 &
\`\`\`

**告警逻辑：**

| 场景 | 触发条件 | 动作 |
|------|---------|------|
| HTTP 非 200 | health 返回非 200 | 记录 ERROR，累计失败 |
| 连续失败 3 次 | 3 次连续 check 失败 | 发 CRITICAL 告警 |
| Redis 池水位低 | 空闲 < 20% | 发 WARNING 告警 |
| 设备限流突增 | 单次增量 ≥ 10 | 发 WARNING 告警 |
| Redis 枯竭 | exhausted > 0 | 发 WARNING 告警 |
| 端口不监听 | ss 检查 :9503 不存在 | 记录 ERROR |
| 恢复通知 | 从失败回到正常 | 发 INFO 恢复通知 |

**告警通道配置：**

\`\`\`bash
# syslog（默认开启，查看方式）
journalctl -t edge-health -f
# 或
grep edge-health /var/log/syslog

# Webhook（钉钉 / 企微 / Slack）
export WEBHOOK_URL="https://oapi.dingtalk.com/robot/send?access_token=xxx"
ALERT_WEBHOOK=true bash extensions/edge/bash/edge-health.sh --daemon

# 关闭 stderr 输出（纯后台运行）
ALERT_STDERR=false bash extensions/edge/bash/edge-health.sh --daemon
\`\`\`

# 巡检 + 冒烟一起跑
bash extensions/edge/bash/edge-health.sh && php extensions/edge/test/verify_simple.php

**日志与状态文件：**

| 文件 | 说明 |
|------|------|
| `/var/log/edge-auth/health-check.log` | 巡检日志（>50MB 自动轮转） |
| `/var/log/edge-auth/health-check.state` | 连续失败计数、上次告警时间等状态 |

**配合 watch 实时观察（开发/排障用）：**

\`\`\`bash
# 实时看 health 端点
watch -n 1 -d 'curl -s --max-time 2 http://127.0.0.1:9503/admin-api/extension/edge/gateway/health | jq .'

# health + metrics 一起盯
watch -n 1 -d '
echo "=== HEALTH ===";
curl -s --max-time 2 http://127.0.0.1:9503/admin-api/extension/edge/gateway/health | jq .;
echo "=== METRICS ===";
curl -s --max-time 2 http://127.0.0.1:9503/admin-api/extension/edge/gateway/metrics | jq "{global: .global_concurrency, device_exceeded: .device_concurrency_exceeded, auth_ok: .auth_ok, auth_fail: .auth_fail}";
'
\`\`\`

实时动态看 health 端点，直接用 watch + curl + jq 就行，不用写复杂脚本。

1. 最简：每 1 秒看 health 完整 JSON

   watch -n 1 -d 'curl -s --max-time 2 http://127.0.0.1:9503/admin-api/extension/edge/gateway/health | jq .'
   -n 1：每秒刷新
   -d：高亮变化字段（status 切换、redis.free 变动会反白）
   jq .：格式化输出
2. 只看关键字段（不刷屏）

   watch -n 1 'curl -s --max-time 2 http://127.0.0.1:9503/admin-api/extension/edge/gateway/health | jq -r "(.status) + \" | redis_free=\" + (.redis.free|tostring) + \" | cache=\" + (.cache.size|tostring) + \" | \" + (now|strftime(\"%H:%M:%S\"))"'

输出类似：

纯文本
ok | redis_free=256 | cache=3 | 09:49:12
3. 顺带把 metrics 也一起盯（推荐）

   watch -n 1 -d '
   echo "=== HEALTH ===";
   curl -s --max-time 2 http://127.0.0.1:9503/admin-api/extension/edge/gateway/health | jq .;
   echo "=== METRICS ===";
   curl -s --max-time 2 http://127.0.0.1:9503/admin-api/extension/edge/gateway/metrics | jq "{global: .global_concurrency, device_exceeded: .device_concurrency_exceeded, auth_ok: .auth_ok, auth_fail: .auth_fail, redis_exhausted: .redis_pool_exhausted}";
   '

这样一眼能看到：

服务是否活（health.status）
信号量水位（global_concurrency.used/free）
认证成功/失败计数
设备限流触发次数
Redis 池是否枯竭
4. 只盯“活着没”+ 延迟（轻量）

   watch -n 1 'curl -s -o /dev/null -w "code=%{http_code} time=%{time_total}s\n" --max-time 2 http://127.0.0.1:9503/admin-api/extension/edge/gateway/health'

code=000 就是服务挂了或连不上。

5. 后台持续记录到日志（排障用）

   while true; do
   ts=$(date +%F_%T)
   code=$(curl -s -o /dev/null -w '%{http_code}' --max-time 2 http://127.0.0.1:9503/admin-api/extension/edge/gateway/health)
   echo "$ts health=$code"
   sleep 1
   done

想同时记 metrics 就再 curl 一次追加进去。

**systemd 集成（可选）：**

如需开机自启，可将巡检注册为 systemd service（与 `edge-auth.service` 独立），避免巡检脚本异常退出导致认证服务受影响。
