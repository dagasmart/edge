# edge:insert-redis 命令使用说明

## 1. 命令简介

`edge:insert-redis` 是一个基于 Laravel Console（结合 Swow 协程）的常驻命令，用于：

- 订阅 **NanoMQ（MQTT）** 的 `heartbeat/response` 主题；
- 将接收到的设备消息按**批量 + 定时**策略；
- 写入 **Redis Stream**，默认流键为 `stream:device:data`；
- 在 Redis 写入失败时自动转入**死信流** `stream:device:dead`；
- 支持 **优雅停机**、运行时统计和背压保护。

> 适用场景：边缘网关 / 设备消息采集，需要高吞吐地将 MQTT 数据桥接到 Redis Stream 供下游消费。

---

## 2. 基本信息

| 项目 | 说明 |
|---|---|
| 命令签名 | `php artisan edge:insert-redis` |
| 命令类 | `DagaSmart\Edge\Console\Commands\EdgeInsertRedisCommand` |
| 依赖扩展 | PHP Swow、Redis 扩展 |
| 依赖组件 | `DagaSmart\Edge\Engine\NanoMQ` |
| 依赖文件 | `bin/bootstrap.php`（定义 Redis 常量）、`bin/helper.php`（提供 `redisPoolInit`、`withRedis`） |
| 订阅主题 | `heartbeat/response` |
| 主数据流 | `stream:device:data`（最大长度 50,000） |
| 死信流 | `stream:device:dead`（最大长度 10,000） |

---

## 3. 运行环境要求

1. **PHP**：需支持 `strict_types`，建议 PHP 8.1+（代码使用了枚举风格的常量与只读写法）。
2. **Swow**：必须安装并启用 `swow` 扩展，命令依赖：
   - `Swow\Coroutine`
   - `Swow\Channel`
   - `Swow\Signal`
3. **Redis**：需安装 PHP Redis 扩展（`\Redis` 类），并支持：
   - `xAdd`（Redis Stream）
   - `multi` / `exec`（Pipeline）
4. **NanoMQ**：边缘侧需部署并运行 NanoMQ，命令通过 `NanoMQ` 类与其建立 MQTT 连接。
5. **常量定义**：`bootstrap.php` 中必须定义以下常量：
   - `REDIS_HOST`
   - `REDIS_PORT`
   - `REDIS_DB`
   - `REDIS_POOL_SIZE`（可选，缺省为 `64`）

---

## 4. 安装与准备

### 4.1 确认文件位置

命令类文件应位于：

```
app/Console/Commands/ 或对应包路径：
DagaSmart\Edge\Console\Commands\EdgeInsertRedisCommand.php
```

并确保以下两个引导文件存在：

```
<edge_root>/bin/bootstrap.php
<edge_root>/bin/helper.php
```

命令会自动通过 `dirname(__DIR__, 3)` 定位到 `<edge_root>` 并 `require_once`。

### 4.2 检查引导文件

**bootstrap.php** 示例（最小配置）：

```php
<?php

define('REDIS_HOST', '127.0.0.1');
define('REDIS_PORT', 6379);
define('REDIS_DB', 2);
define('REDIS_POOL_SIZE', 64);
```

**helper.php** 需提供以下函数（命令依赖）：

- `redisPoolInit(int $poolSize, int $db): void`
- `withRedis(callable $callback): mixed`

---

## 5. 配置项说明

所有配置均通过 Laravel 的 `config()` 读取，并支持在 `.env` 或配置文件中设置。

| 配置项 | 类型 | 默认值 | 说明 |
|---|---|---|---|
| `edge.mqtt.batch_size` | int | `1` | 每批写入 Redis 的消息数量 |
| `edge.mqtt.flush_interval_ms` | int | `100` | 最大刷盘间隔（毫秒），到点即写 |
| `edge.mqtt.channel_size` | int | `2048` | 内存 Channel 容量（背压上限） |
| `edge.mqtt.max_payload_len` | int | `65536` | 单条 payload 最大字节数（默认 64KB） |
| `edge.redis.pool_size` | int | `64` | Redis 连接池大小 |
| `edge.redis.db` | int | `2` | Redis 数据库编号 |

### 5.1 配置示例

在 `config/edge.php` 中：

```php
<?php

return [
    'mqtt' => [
        'batch_size' => 100,            // 每批 100 条
        'flush_interval_ms' => 50,      // 最多 50ms 刷一次
        'channel_size' => 4096,         // 队列容量 4096
        'max_payload_len' => 131072,    // 最大 128KB
    ],
    'redis' => [
        'pool_size' => 32,
        'db' => 2,
    ],
];
```

---

## 6. 启动命令

### 6.1 前台运行

```bash
php artisan edge:insert-redis
```

启动成功后输出示例：

```
[edge:insert-redis] 启动，PID=12345
[redis] 连接池已初始化 127.0.0.1:6379 db=2
=====================================
MQTT 订阅已启动 | 2026-11-03 10:00:00
订阅主题：heartbeat/response
批量大小：100，刷盘间隔：50ms
Channel 容量：4096，最大 payload：128KB
按 Ctrl+C 停止
=====================================
```

### 6.2 后台运行（推荐）

```bash
# 使用 nohup
nohup php artisan edge:insert-redis >> /var/log/edge-insert-redis.log 2>&1 &

# 或使用 supervisor
```

### 6.3 Supervisor 配置示例

```ini
[program:edge-insert-redis]
command=php /path/to/artisan edge:insert-redis
user=www-data
autostart=true
autorestart=true
redirect_stderr=true
stdout_logfile=/var/log/edge-insert-redis.log
```

---

## 7. 工作原理

```
┌────────────┐
│   NanoMQ   │  heartbeat/response
└─────┬──────┘
      │ MQTT 消息
      ▼
┌──────────────────────────┐
│  MQTT 订阅协程 (Swow)     │
│  - 校验 payload 长度      │
│  - 解析 device sn         │
│  - 推入 Channel           │
└──────────┬───────────────┘
           │ Channel (容量可配)
           ▼
┌──────────────────────────┐
│  Flusher 协程 (Swow)      │
│  - 攒批 (batch_size)      │
│  - 定时 (flush_interval)  │
│  - Pipeline xAdd          │
└──────────┬───────────────┘
           │ Redis Pipeline
           ▼
   ┌───────────────┐
   │  Redis Stream │
   │ stream:device │
   │   :data       │
   └───────────────┘
           │ 写入失败
           ▼
   ┌───────────────┐
   │  stream:device│
   │   :dead       │
   └───────────────┘
```

### 7.1 写入格式

每条消息以 Redis Stream 形式写入，字段如下：

| 字段 | 说明 |
|---|---|
| `topic` | MQTT 主题，固定为 `heartbeat/response` |
| `payload` | 原始消息体（JSON 字符串） |
| `device` | 设备序列号（取自 `sn` 字段，转大写；解析失败为 `UNKNOWN`） |
| `ts` | 接收时的 Unix 时间戳（含毫秒，如 `1762147200.123`） |

### 7.2 死信格式

当 Redis 写入失败时，整批消息以单条记录写入死信流：

| 字段 | 说明 |
|---|---|
| `batch` | 批次消息的 JSON 数组 |
| `error` | 错误原因 |
| `ts` | 写入死信的时间戳 |

---

## 8. 优雅停机

### 8.1 停机方式

- `Ctrl+C`（发送 `SIGINT`）
- `kill <pid>`（发送 `SIGTERM`）

### 8.2 停机流程

1. 收到信号后设置 `$stopping = true`；
2. 复用已有 MQTT 实例调用 `disconnect()`，断开订阅协程连接；
3. 向 Channel 推入 `null` 唤醒 Flusher；
4. Flusher 立即刷写当前批次，并继续排空 Channel 中残留消息（每次 pop 超时 50ms）；
5. MQTT 协程退出后，打印运行统计并结束进程。

> **注意**：若停机过程中再次收到信号，将立即 `exit(1)` 强制退出，可能造成少量未刷盘消息丢失。

---

## 9. 背压与丢弃策略

命令通过以下机制保护系统：

| 机制 | 行为 |
|---|---|
| payload 超长 | payload 长度超过 `max_payload_len` → 丢弃并计数 `droppedCount++` |
| Channel 已满 | Channel 队列满 → 丢弃新消息并输出警告 |
| Channel push 异常 | push 失败 → 丢弃并计数 |
| Redis 写入失败 | 整批转入死信流 `stream:device:dead`，不阻塞主流程 |

---

## 10. 运行统计

命令停止时自动输出：

```
========== 运行统计 ==========
运行时长：120.5s
接收消息：15234
写入成功：15200
丢弃消息：34
死信消息：0
写入 QPS：126.1 msg/s
平均刷盘：2.34ms
==============================
```

| 指标 | 含义 |
|---|---|
| 接收消息 | 成功推入 Channel 的消息数 |
| 写入成功 | 成功写入 Redis Stream 的消息数 |
| 丢弃消息 | 因超长、队列满、push 失败而丢弃的消息数 |
| 死信消息 | 写入 Redis 失败转入死信流的消息数 |
| 写入 QPS | 写入成功数 ÷ 运行时长 |
| 平均刷盘 | 每次 `flushBatch` 的平均耗时 |

---

## 11. 运维与排错

### 11.1 常见错误

| 错误信息 | 原因 | 解决方案 |
|---|---|---|
| `helper.php 未加载` | `redisPoolInit` 函数不存在 | 检查 `bin/helper.php` 是否被正确引入 |
| `bootstrap.php 未加载或 Redis 常量未定义` | 缺少 `REDIS_HOST` 等常量 | 在 `bootstrap.php` 中定义 Redis 常量 |
| `[FLUSH] Redis 写入失败` | Redis 连接异常或 Stream 不可写 | 检查 Redis 服务、连接池和网络 |
| `[FATAL] 死信写入也失败` | Redis 完全不可用 | 立即恢复 Redis 服务，并手动处理日志中的丢失数据 |
| `[CHANNEL] 队列已满` | 消费速度跟不上生产速度 | 增大 `channel_size` 或 `batch_size`，或检查下游消费能力 |

### 11.2 日志关键字速查

| 关键字 | 含义 |
|---|---|
| `[RECV]` | 收到 MQTT 消息（输出到 stderr） |
| `[MQTT]` | 消息已解析并准备入队 |
| `[FLUSH]` | 批次成功写入 Stream |
| `[DROP]` | 消息被丢弃 |
| `[CHANNEL]` | Channel 相关警告 |
| `[DEAD]` | 消息进入死信流 |
| `[signal]` | 信号处理日志 |
| `[FLUSH-DIAG]` | Pipeline 返回结果数量异常 |

---

## 12. 性能调优建议

| 场景 | 建议 |
|---|---|
| 高吞吐设备数据 | 增大 `batch_size`（如 100~500）并减小 `flush_interval_ms`（如 20~50ms） |
| 内存有限 | 减小 `channel_size` 和 `batch_size` |
| 大 payload | 适当增大 `max_payload_len`，但注意 Redis 单条限制 |
| 下游消费慢 | 优先增大 `channel_size` 做缓冲，并增加下游消费协程 |
| Redis 延迟高 | 降低 `batch_size` 以减少单次 pipeline 耗时，或横向扩容 Redis |

---

## 13. 注意事项

1. 命令为**常驻进程**，需配合 `supervisor` 或 `systemd` 保证高可用。
2. 命令内部复用了同一个 `NanoMQ` 实例，避免在信号处理中新建连接导致资源泄漏。
3. 死信流 `stream:device:dead` 需有独立的消费/告警程序，否则失败消息会无限堆积（受 `DEAD_MAX_LEN` 限制）。
4. 若需修改订阅主题，需修改源码中的 `$this->mqtt->subscribe(...)` 调用。
5. `stream:device:data` 和 `stream:device:dead` 均设置了最大长度（`MAXLEN ~` 策略），Redis 会自动裁剪旧数据。

---

## 14. 附录：配置速查表

```env
# .env 示例（如通过配置桥接）
EDGE_MQTT_BATCH_SIZE=100
EDGE_MQTT_FLUSH_INTERVAL_MS=50
EDGE_MQTT_CHANNEL_SIZE=4096
EDGE_MQTT_MAX_PAYLOAD_LEN=131072
EDGE_REDIS_POOL_SIZE=64
EDGE_REDIS_DB=2
```

```php
// config/edge.php 对应结构
return [
    'mqtt' => [
        'batch_size' => env('EDGE_MQTT_BATCH_SIZE', 1),
        'flush_interval_ms' => env('EDGE_MQTT_FLUSH_INTERVAL_MS', 100),
        'channel_size' => env('EDGE_MQTT_CHANNEL_SIZE', 2048),
        'max_payload_len' => env('EDGE_MQTT_MAX_PAYLOAD_LEN', 65536),
    ],
    'redis' => [
        'pool_size' => env('EDGE_REDIS_POOL_SIZE', 64),
        'db' => env('EDGE_REDIS_DB', 2),
    ],
];
```
