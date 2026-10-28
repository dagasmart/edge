# EdgeAuth 部署工具链（对齐版）

本目录为修复后的**全量落地文件**，解决历史遗留的三个不一致问题：
1. Redis key 冒号被错误替换成短横线 → 已统一为冒号 `device:secret:{cid}:{uid}`
2. 文件名 `secret.php` / `secrets.php` 混用 → 已统一为 `secrets.php`（复数）
3. systemd 引用的 `start.sh`/`stop.sh` 与实际文件 `start-cluster.sh`/`stop-cluster.sh` 不符 → 已对齐

---

## 目录结构

```
extensions/edge/
├── README.md                    # 本文件
├── bash/
│   ├── deploy.sh                # 总入口（CI / 首次部署），内部 seed+start+verify
│   ├── start-cluster.sh         # 集群启动，被 systemd ExecStart 引用
│   ├── stop-cluster.sh          # 集群停止，被 systemd ExecStop 引用
│   └── reload-cluster.sh        # 滚动重启（独立运维）
├── config/
│   └── secrets.php              # 设备密钥源（deploy.sh / seed_redis.php / verify 共用）
└── test/
    ├── verify_simple.php        # 12 项回归验证，与 hmac.php 字节级对齐
    └── seed_redis.php           # Redis 密钥写入（--dry-run / --force）
```

## 对齐契约（所有文件共同遵守）

| 项目 | 统一值 |
|------|--------|
| 密钥文件名 | `secrets.php`（复数） |
| Redis key | `device:secret:{clientid}:{username}`（冒号，不替换） |
| 签名协议 | `b64Sign\|timestamp\|nonce`，HMAC-SHA256 原始二进制 + URL-safe base64 |
| 启动命令 | `php artisan edge:auth-swow`（全链路统一，端口由 `AUTH_PORT` 环境变量注入） |
| Redis 环境变量 | `REDIS_HOST` / `REDIS_PORT` / `REDIS_PASS` / `REDIS_DB`，所有脚本一致 |

## 快速使用

```bash
# 首次部署（交互式，会提示是否写 Redis）
sudo bash extensions/edge/bash/deploy.sh

# 预演（不实际写入/启动）
sudo bash extensions/edge/bash/deploy.sh --dry-run

# CI 自动部署
sudo CI=1 bash extensions/edge/bash/deploy.sh

# 只更新密钥
sudo bash extensions/edge/bash/deploy.sh --seed-only

# 滚动重载（生产常用）
sudo bash extensions/edge/bash/reload-cluster.sh

# 手动验证
/xp/server/php/php-8.4/bin/php extensions/edge/test/verify_simple.php
```

## 关键修复点对照

| 文件 | 修复内容 |
|------|---------|
| `bash/deploy.sh` | `seed_redis()` 中 `device:secret:${key}` 保留冒号（去掉 `${key/:/-}`）；systemd 指向 `start-cluster.sh` / `stop-cluster.sh` |
| `bash/start-cluster.sh` | 补齐 `REDIS_HOST`/`REDIS_PORT`/`REDIS_PASS`/`REDIS_DB` 环境变量透传 |
| `bash/reload-cluster.sh` | 同上，补齐 Redis 环境变量；启停逻辑与 deploy 的 `start_all` 一致 |
| `config/secrets.php` | 文件名由 `secret.php` 改为 `secrets.php`；注释中错误的短横线说明改为冒号 |
| `test/verify_simple.php` | 文件名引用 `secret.php` → `secrets.php` |
| `test/seed_redis.php` | 文件名引用 `secret.php` → `secrets.php`；key 格式已是冒号（正确） |

## 部署前自检

```bash
# 1. 文件名统一
ls extensions/edge/config/secrets.php        # 应存在
ls extensions/edge/config/secret.php 2>&1     # 应不存在

# 2. 无单数引用残留
grep -rn "secret\.php" extensions/edge/       # 应只命中 secrets.php

# 3. 无冒号替换残留
grep -rn '${key/:/-}' extensions/edge/         # 应无命中

# 4. systemd 脚本真实存在
ls extensions/edge/bash/start-cluster.sh extensions/edge/bash/stop-cluster.sh
```
