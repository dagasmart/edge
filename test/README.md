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
