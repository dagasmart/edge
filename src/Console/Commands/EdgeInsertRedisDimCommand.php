<?php

declare(strict_types=1);

namespace DagaSmart\Edge\Console\Commands;

use DagaSmart\Edge\Engine\NanoMQ;
use DagaSmart\Edge\Support\MultiDimAdaptiveBatch;
use DagaSmart\Edge\Support\PriorityRouter;
use DagaSmart\Edge\Support\PrometheusExporter;
use DagaSmart\Edge\Support\RedisShardManager;
use Illuminate\Console\Command;
use Illuminate\Support\Facades\DB;
use Swow\Channel;
use Swow\Coroutine;
use Swow\Signal;
use Throwable;

/**
 * 订阅 NanoMQ 多主题，按优先级路由并批量写入 Redis Stream（分片版 + Worker Pool + Auto-Scaler）
 *
 * ★ FIX: 所有 usleep/sleep 替换为 swow_co_sleep，修复 100% CPU 自旋
 * ★ FIX: Worker 哨兵一哨一退，修复缩容后无消费者
 * ★ FIX: wakeupChannel 只 push 活跃数，修复退出时 Channel 满阻塞
 * ★ FIX: 心跳恢复强制高优 + 高优最低 2 Worker + 哨兵数量修正
 * ★ FIX: Channel::push() 返回 static（Channel 对象），改用 try/catch 判定满队列
 * ★ FIX: Channel::pop() 的 timeout 单位是微秒，批量刷新计时改为微秒并按时间 flush
 * ★ FIX: 停机时调用 NanoMQ::stop()，避免退出阶段继续重连
 * ★ FIX: collectWorkerMetrics() 指标名对齐 PrometheusExporter 注册表；counter 不再误用 set()
 * ★ FIX: 补齐 flushed_total / wait_ms / qps / mqtt_connected 等已注册但从未写入的指标
 * ★ FIX: DB Facade 直接关闭 QueryLog，不再依赖 class_exists('DB') 别名
 * ★ FIX: 快速退出分支主动 stop metrics server，确保 9090 端口即时释放
 */
class EdgeInsertRedisDimCommand extends Command
{
    protected $signature = 'edge:insert-redis-dim
        {--auto-scale=1 : 启用自动伸缩(0/1)}
        {--auto-scale-interval=30 : 自动伸缩评估间隔(秒)}
        {--sample-rate-min=0.2 : 最小采样率(低负载时)}
        {--sample-rate-max=1.0 : 最大采样率}
        {--device-active-ttl= : 设备活跃 key TTL(秒)}
        {--heartbeat-topic=heartbeat/response : 心跳 topic}
        {--heartbeat-write-stream=0 : 心跳是否写入 Redis Stream(0/1)}
        {--shard-count=2 : Stream 分片数量}';

    protected $description = '订阅 NanoMQ 多主题，按优先级路由并批量写入 Redis Stream（分片版）';

    /* ==================== Stream（分片版） ==================== */
    private const string STREAM_HIGH_PREFIX = 'stream:device:high:';

    private const string STREAM_DATA_PREFIX = 'stream:device:data:';

    private const string STREAM_DEAD = 'stream:device:dead';

    private const string STREAM_HEARTBEAT = 'stream:device:heartbeat';

    private const int STREAM_MAX_LEN = 50_000;

    private const int DEAD_MAX_LEN = 10_000;

    private const int HEARTBEAT_MAX_LEN = 100_000;

    /**
     * Channel 停止哨兵
     */
    private const string STOP_SENTINEL = '__EDGE_STOP__';

    /* ==================== 分片配置 ==================== */
    private int $shardCount = 2;

    /* ==================== 组件 ==================== */
    private PrometheusExporter $exporter;

    private PriorityRouter $router;

    private MultiDimAdaptiveBatch $adaptiveBatch;

    /* ==================== 普通 Channel ==================== */
    private Channel $normalChannel;

    private int $normalBatchSize;

    private int $normalFlushIntervalMs;

    /* ==================== 高优 Channel ==================== */
    private Channel $highChannel;

    private int $highBatchSize;

    private int $highFlushIntervalMs;

    /* ==================== 死信 Channel ==================== */
    private Channel $deadChannel;

    private int $deadBatchSize;

    private int $deadFlushIntervalMs;

    /* ==================== 公共 ==================== */
    private int $maxPayloadLen;

    private bool $stopping = false;

    private ?NanoMQ $mqtt = null;

    /* ==================== Worker 池配置 ==================== */
    private int $normalWorkers;

    private int $highWorkers;

    private int $deadWorkers;

    private int $workerIdleTimeoutSec;

    /* ==================== Worker 协程引用 ==================== */
    /** @var array<int, Coroutine> */
    private array $normalWorkerCoroutines = [];

    /** @var array<int, Coroutine> */
    private array $highWorkerCoroutines = [];

    /** @var array<int, Coroutine> */
    private array $deadWorkerCoroutines = [];

    /* ==================== 活跃 Worker 计数 ==================== */
    private int $activeNormalWorkers = 0;

    private int $activeHighWorkers = 0;

    private int $activeDeadWorkers = 0;

    /* ==================== Auto-Scaler 状态 ==================== */
    private bool $autoScaleEnabled = true;

    private int $autoScaleIntervalSec = 30;

    private float $sampleRateMin = 0.2;

    private float $sampleRateMax = 1.0;

    private float $sampleRate = 1.0;

    private int $targetNormalWorkers = 4;

    private int $targetHighWorkers = 2;

    private int $activeClientCount = 0;

    private int $lastScanTime = 0;

    private int $cachedClientCount = 0;

    /* ==================== 心跳配置 ==================== */
    private int $deviceActiveTtl = 120;

    private string $heartbeatTopic = 'heartbeat/response';

    private bool $heartbeatWriteStream = true;

    /* ==================== 心跳内存计数 ==================== */
    /** @var array<string, int> */
    private array $activeDevices = [];

    /** @var array<string, int> */
    private array $lastRenewTime = [];

    private int $activeDeviceCount = 0;

    private int $lastExpireScan = 0;

    private int $renewIntervalSec = 30;

    /* ==================== 统计 ==================== */
    private int $receivedCount = 0;

    private int $normalFlushedCount = 0;

    private int $highFlushedCount = 0;

    private int $deadFlushedCount = 0;

    private int $heartbeatFlushedCount = 0;

    private int $droppedCount = 0;

    private float $normalFlushLatencySum = 0.0;

    private int $normalFlushTimes = 0;

    private float $highFlushLatencySum = 0.0;

    private int $highFlushTimes = 0;

    private float $deadFlushLatencySum = 0.0;

    private int $deadFlushTimes = 0;

    private float $startTime;

    private float $lastNormalWaitMs = 0.0;

    private float $lastHighWaitMs = 0.0;

    private float $lastDeadWaitMs = 0.0;

    /** 自维护积压 */
    private int $normalBacklog = 0;

    private int $highBacklog = 0;

    private int $deadBacklog = 0;

    /** @var array<float> 环形 QPS 窗口 */
    private array $qpsWindow = [];

    private const int QPS_WINDOW_MAX = 200;

    public function handle(): int
    {
        // ★ FIX: 直接调用 Facade。class_exists('DB') 依赖 config/app.php 的 aliases，
        // Laravel 11+ 默认不再注册该别名时会返回 false，QueryLog 便不会关闭；
        // 常驻协程下查询日志持续累积会造成内存泄漏。
        DB::disableQueryLog();

        $this->startTime = microtime(true);
        $this->info('[edge:insert-redis-dim] 启动（分片版），PID=' . getmypid());

        $this->shardCount = max(1, (int) $this->option('shard-count'));

        $this->loadConfig();
        $this->initComponents();
        $this->registerSignalHandlers();

        RedisShardManager::init();

        $metricsPort = $this->configInt('edge.metrics.port', 9090);
        $metricsHost = config('edge.metrics.host', '0.0.0.0');
        if ($metricsHost === '0.0.0.0') {
            $metricsHost = '127.0.0.1';
        }

        $this->exporter->startMetricsServer($metricsPort, $metricsHost, function (): array {
            $this->collectWorkerMetrics();
            return $this->exporter->getMetrics();
        });

        [$scheme, $serverIp] = $this->getServerIp();
        $this->info(sprintf('[metrics] Prometheus 监听 %s:%d', $metricsHost, $metricsPort));
        $this->info(sprintf('[metrics] Prometheus 本地 %s://%s:%d', $scheme, $serverIp, $metricsPort));
        echo "[metrics] Prometheus 预览 {$scheme}://{$serverIp}:{$metricsPort}/metrics\n";
        echo "[metrics] Prometheus 接口 {$scheme}://{$serverIp}:{$metricsPort}/api/metrics\n";

        $this->startNormalWorkerPool();
        $this->startHighWorkerPool();
        $this->startDeadLetterWorkerPool();

        if ($this->autoScaleEnabled) {
            $this->startAutoScaler();
        }

        $this->probeMqttConnectivity();
        $this->startPeriodicMqttProbe();

        $topics = $this->resolveTopics();

        $maxReconnects = 10;
        $reconnectCount = 0;
        $reconnectDelay = 3;

        while (true) {
            if ($this->stopping) {
                $this->info('[edge] 收到停止信号，不再重连，退出中...');
                break;
            }

            try {
                $this->mqtt = new NanoMQ;

                $this->info('=====================================');
                $this->info(sprintf('MQTT 订阅已启动 | %s', $this->nowString()));
                $this->info(sprintf('分片数：%d | Stream 前缀：%s', $this->shardCount, self::STREAM_DATA_PREFIX));
                $this->info('订阅主题：' . implode(', ', $topics));
                $this->info(sprintf(
                    '普通：batch=%d interval=%dms channel=%d workers=%d',
                    $this->normalBatchSize,
                    $this->normalFlushIntervalMs,
                    $this->normalChannel->getCapacity(),
                    $this->normalWorkers,
                ));
                $this->info(sprintf(
                    '高优：batch=%d interval=%dms channel=%d workers=%d',
                    $this->highBatchSize,
                    $this->highFlushIntervalMs,
                    $this->highChannel->getCapacity(),
                    $this->highWorkers,
                ));
                $this->info(sprintf(
                    '死信：batch=%d interval=%dms channel=%d workers=%d',
                    $this->deadBatchSize,
                    $this->deadFlushIntervalMs,
                    $this->deadChannel->getCapacity(),
                    $this->deadWorkers,
                ));
                $this->info(sprintf(
                    '心跳 Stream：%s (maxlen=%d) | 写入：%s',
                    self::STREAM_HEARTBEAT,
                    self::HEARTBEAT_MAX_LEN,
                    $this->heartbeatWriteStream ? '开启' : '关闭（仅续期）',
                ));
                $this->info(sprintf(
                    'AutoScale=%s interval=%ds sample=%.2f~%.2f',
                    $this->autoScaleEnabled ? 'on' : 'off',
                    $this->autoScaleIntervalSec,
                    $this->sampleRateMin,
                    $this->sampleRateMax,
                ));
                $this->info(sprintf(
                    '心跳续期：topic=%s ttl=%ds 续期间隔=%ds',
                    $this->heartbeatTopic,
                    $this->deviceActiveTtl,
                    $this->renewIntervalSec,
                ));
                $this->info('按 Ctrl+C 停止');
                $this->info('=====================================');

                $handler = function (string $topic, string $message): void {
                    if ($this->stopping) {
                        return;
                    }

                    $payloadLen = strlen($message);
                    if ($payloadLen > $this->maxPayloadLen) {
                        $this->warn(sprintf(
                            '[DROP] payload 超过限制 %d > %d，丢弃',
                            $payloadLen,
                            $this->maxPayloadLen,
                        ));
                        $this->droppedCount++;
                        $this->exporter->inc('edge_redis_write_failed_total');
                        return;
                    }

                    fwrite(STDERR, sprintf(
                        "[RECV] topic=%s len=%d\n",
                        $topic,
                        $payloadLen,
                    ));

                    $device = 'unknown';
                    try {
                        $data = json_decode($message, false, 512, JSON_THROW_ON_ERROR);
                        if (isset($data->sn)) {
                            $device = strtoupper((string) $data->sn);
                        } elseif (isset($data->client_id)) {
                            $device = strtoupper((string) $data->client_id);
                        }
                    } catch (Throwable) {
                        // ignore
                    }

                    $this->line(sprintf(
                        '[MQTT] topic=%s device=%s payload_len=%d',
                        $topic,
                        $device,
                        $payloadLen,
                    ));

                    $isHeartbeat = ($topic === $this->heartbeatTopic);
                    if ($isHeartbeat) {
                        $activeKey = ($device !== 'unknown') ? $device : null;
                        if ($activeKey !== null && $activeKey !== '') {
                            $this->renewDeviceActive($activeKey);
                        }
                        if (! $this->heartbeatWriteStream) {
                            return;   // ← 直接 return，不进任何 channel
                        }
                    }

                    if (! $isHeartbeat && ! $this->router->isHighPriority($topic) && $this->sampleRate < 1.0) {
                        if ((mt_rand(1, 10000) / 10000.0) > $this->sampleRate) {
                            $this->droppedCount++;
                            $this->exporter->inc('edge_dropped_by_sampling_total');
                            return;
                        }
                    }

                    $shardNo = $this->calcShard($device);

                    $item = [
                        'topic' => $topic,
                        'payload' => $message,
                        'device' => $device,
                        'ts' => sprintf('%.3f', microtime(true)),
                        '_enqueued_at' => microtime(true),
                        'is_heartbeat' => $isHeartbeat,
                        'shard_no' => $shardNo,
                    ];

                    $this->exporter->inc('edge_received_total');
                    $this->qpsWindow[] = microtime(true);
                    if (count($this->qpsWindow) > self::QPS_WINDOW_MAX) {
                        array_shift($this->qpsWindow);
                    }

                    // ★ FIX: 心跳强制高优 + router 高优统一走高优通道
                    if ($isHeartbeat || $this->router->isHighPriority($topic)) {
                        $this->routeToHighChannel($item, $device);
                    } else {
                        $this->routeToNormalChannel($item, $device);
                    }
                };

                $this->mqtt->subscribeMultiple($topics, $handler);

                $connectedAt = microtime(true);
                while ($this->mqtt->isRunning()) {
                    if ($this->stopping) {
                        $this->info('[edge] 收到停止信号，正在退出...');
                        try {
                            // ★ FIX: stop() 会同时置 stopping 标志；只 disconnect()
                            // 的话 NanoMQ 内部协程仍会继续重连，优雅停机会被拖住。
                            $this->mqtt->stop();
                        } catch (Throwable) {
                            // ignore
                        }
                        break 2;
                    }
                    swow_co_sleep(1);
                }

                if ($this->stopping) {
                    break;
                }

                $connectionDuration = microtime(true) - $connectedAt;
                if ($connectionDuration >= 10.0) {
                    $reconnectCount = 0;
                    $this->info(sprintf('[mqtt] 连接稳定 %.1fs，重连计数已重置', $connectionDuration));
                } else {
                    $reconnectCount++;
                    $this->warn(sprintf(
                        '[mqtt] 连接断开（持续 %.1fs，第 %d/%d 次重连）...',
                        $connectionDuration,
                        $reconnectCount,
                        $maxReconnects,
                    ));
                }

                if ($reconnectCount >= $maxReconnects) {
                    $this->error(sprintf('[mqtt] 连续重连 %d 次失败，放弃连接，进程退出（PID=%d）', $maxReconnects, getmypid()));
                    $this->printStats();
                    $this->exporter->stop();
                    return 1;
                }

                $this->warn(sprintf('[mqtt] %d 秒后重连...', $reconnectDelay));
                $elapsed = 0;
                while ($elapsed < $reconnectDelay) {
                    if ($this->stopping) {
                        break 2;
                    }
                    swow_co_sleep(1);
                    $elapsed++;
                }

            } catch (Throwable $e) {
                if ($this->stopping) {
                    break;
                }

                $reconnectCount++;
                $this->error(sprintf('[mqtt] 连接异常：%s（第 %d/%d 次）', $e->getMessage(), $reconnectCount, $maxReconnects));

                if ($reconnectCount >= $maxReconnects) {
                    $this->error(sprintf('[mqtt] 连续重连 %d 次失败，放弃连接，进程退出（PID=%d）', $maxReconnects, getmypid()));
                    $this->printStats();
                    $this->exporter->stop();
                    return 1;
                }

                $this->error(sprintf('[mqtt] %d 秒后重连...', $reconnectDelay));
                $elapsed = 0;
                while ($elapsed < $reconnectDelay) {
                    if ($this->stopping) {
                        break 2;
                    }
                    swow_co_sleep(1);
                    $elapsed++;
                }
            }
        }

        if ($this->stopping) {
            $this->info('[edge] 快速退出模式');
            // ★ FIX: exit(0) 前主动释放监听 socket，避免端口残留占用
            $this->exporter->stop();
            $this->mqtt?->stop();
            $this->printStats();
            $this->info('[edge:insert-redis-dim] 已停止');
            exit(0);
        }

        $this->wakeupChannel($this->normalChannel);
        $this->wakeupChannel($this->highChannel);
        $this->wakeupChannel($this->deadChannel);
        $this->drainWorkerPools();

        $this->printStats();
        $this->exporter->stop();
        $this->info('[edge:insert-redis-dim] 已停止');

        return 0;
    }

    /* ==================== ★ 分片路由 ==================== */

    private function calcShard(string $device): int
    {
        if ($device === 'unknown' || $device === '') {
            return 0;
        }
        return (crc32($device) & 0x7FFFFFFF) % $this->shardCount;
    }

    private function getDataStreamKey(int $shardNo): string
    {
        return self::STREAM_DATA_PREFIX . $shardNo;
    }

    private function getHighStreamKey(int $shardNo): string
    {
        return self::STREAM_HIGH_PREFIX . $shardNo;
    }

    /* ==================== 心跳续期 ==================== */

    protected function renewDeviceActive(string $sn): void
    {
        $now = time();
        $lastRenew = $this->lastRenewTime[$sn] ?? 0;

        if (($now - $lastRenew) < $this->renewIntervalSec) {
            if (! isset($this->activeDevices[$sn])) {
                $this->activeDevices[$sn] = $now;
                $this->activeDeviceCount++;
            } else {
                $this->activeDevices[$sn] = $now;
            }
            return;
        }

        $key = 'device:active:' . $sn;
        $ttl = $this->deviceActiveTtl;

        try {
            withRedisForKey($key, function (\Redis $conn) use ($key, $sn, $ttl): void {
                $conn->setex($key, $ttl, json_encode([
                    'sn' => $sn,
                    'ts' => time(),
                ], JSON_UNESCAPED_SLASHES));
            });
            $this->exporter->inc('edge_heartbeat_renew_total');
            $this->lastRenewTime[$sn] = $now;

            if (! isset($this->activeDevices[$sn])) {
                $this->activeDeviceCount++;
            }
            $this->activeDevices[$sn] = $now;

        } catch (Throwable $e) {
            fwrite(STDERR, '[renewDeviceActive] ' . $e->getMessage() . PHP_EOL);
        }
    }

    /* ==================== Auto-Scaler ==================== */

    private function startAutoScaler(): void
    {
        Coroutine::run(function (): void {
            $this->info('[auto-scaler] started');
            while (! $this->stopping) {
                $deadline = time() + $this->autoScaleIntervalSec;
                while (time() < $deadline && ! $this->stopping) {
                    swow_co_sleep(0.5);
                }

                if ($this->stopping) {
                    break;
                }

                $clientCount = $this->getActiveClientCount();
                $this->activeClientCount = $clientCount;

                $targetNormal = match (true) {
                    $clientCount <= 3 => 1,
                    $clientCount <= 10 => 2,
                    $clientCount <= 30 => 3,
                    $clientCount <= 100 => 5,
                    $clientCount <= 300 => 8,
                    default => 12,
                };

                // ★ FIX: 高优最低 2 Worker，防止缩到 1 后哨兵竞争导致消息饿死
                $targetHigh = match (true) {
                    $clientCount <= 50 => 2,
                    default => 3,
                };

                $newBatchSize = max(10, min(500, (int) ($clientCount * 2 + 20)));

                $newFlushInterval = match (true) {
                    $clientCount <= 3 => 200,
                    $clientCount <= 10 => 100,
                    $clientCount <= 50 => 50,
                    $clientCount <= 200 => 20,
                    default => 10,
                };

                if ($clientCount <= 3) {
                    $newSampleRate = $this->sampleRateMin;
                } elseif ($clientCount <= 10) {
                    $newSampleRate = 0.5;
                } else {
                    $newSampleRate = $this->sampleRateMax;
                }

                $changed = false;

                if ($targetNormal !== $this->targetNormalWorkers) {
                    $this->scaleNormalWorkers($targetNormal);
                    $this->targetNormalWorkers = $targetNormal;
                    $changed = true;
                }

                if ($targetHigh !== $this->targetHighWorkers) {
                    $this->scaleHighWorkers($targetHigh);
                    $this->targetHighWorkers = $targetHigh;
                    $changed = true;
                }

                $this->normalBatchSize = $newBatchSize;
                $this->normalFlushIntervalMs = $newFlushInterval;
                $this->sampleRate = $newSampleRate;

                $this->exporter->set('edge_active_clients', $clientCount);
                $this->exporter->set('edge_target_workers_normal', $targetNormal);
                $this->exporter->set('edge_target_workers_high', $targetHigh);
                $this->exporter->set('edge_sample_rate', $newSampleRate);
                $this->exporter->set('edge_current_batch_size', $newBatchSize);

                if ($changed || $clientCount <= 5) {
                    $this->info(sprintf(
                        '[auto-scaler] clients=%d | normal=%d high=%d | batch=%d | flush=%dms | sample=%.2f',
                        $clientCount,
                        $targetNormal,
                        $targetHigh,
                        $newBatchSize,
                        $newFlushInterval,
                        $newSampleRate
                    ));
                }
            }
            $this->info('[auto-scaler] stopped');
        });
    }

    private function scaleNormalWorkers(int $target): void
    {
        $current = count($this->normalWorkerCoroutines);

        if ($target > $current) {
            $toAdd = $target - $current;
            for ($i = 0; $i < $toAdd; $i++) {
                $this->normalWorkerCoroutines[] = Coroutine::run(function (): void {
                    $this->runNormalWorker();
                });
            }
            $this->normalWorkers = $target;
            $this->info(sprintf('[auto-scaler] normal workers %d → %d (+%d)', $current, $target, $toAdd));
        } elseif ($target < $current) {
            $toStop = $current - $target;
            for ($i = 0; $i < $toStop; $i++) {
                try {
                    $this->normalChannel->push(self::STOP_SENTINEL, 0);
                } catch (Throwable) {
                    break;
                }
            }
            $this->normalWorkers = $target;
            $this->info(sprintf('[auto-scaler] normal workers %d → %d (-%d)', $current, $target, $toStop));
        }
    }

    private function scaleHighWorkers(int $target): void
    {
        $current = count($this->highWorkerCoroutines);

        if ($target > $current) {
            $toAdd = $target - $current;
            for ($i = 0; $i < $toAdd; $i++) {
                $this->highWorkerCoroutines[] = Coroutine::run(function (): void {
                    $this->runHighWorker();
                });
            }
            $this->highWorkers = $target;
            $this->info(sprintf('[auto-scaler] high workers %d → %d (+%d)', $current, $target, $toAdd));
        } elseif ($target < $current) {
            // ★ FIX: 哨兵数量 = 要停的数量，确保每人一个不漏
            $toStop = $current - $target;
            for ($i = 0; $i < $toStop; $i++) {
                try {
                    $this->highChannel->push(self::STOP_SENTINEL, 0);
                } catch (Throwable) {
                    break;
                }
            }
            $this->highWorkers = $target;
            $this->info(sprintf('[auto-scaler] high workers %d → %d (-%d)', $current, $target, $toStop));
        }
    }

    private function getActiveClientCount(): int
    {
        $now = time();

        if (($now - $this->lastExpireScan) >= 30) {
            $expired = [];
            foreach ($this->activeDevices as $sn => $lastSeen) {
                if (($now - $lastSeen) > $this->deviceActiveTtl) {
                    $expired[] = $sn;
                }
            }
            foreach ($expired as $sn) {
                unset($this->activeDevices[$sn]);
                unset($this->lastRenewTime[$sn]);
                $this->activeDeviceCount--;
            }
            $this->lastExpireScan = $now;

            if (! empty($expired)) {
                $this->info(sprintf(
                    '[auto-scaler] 过期淘汰 %d 设备（剩余 %d）',
                    count($expired),
                    max(0, $this->activeDeviceCount),
                ));
            }
        }

        return max(0, $this->activeDeviceCount);
    }

    /* ==================== MQTT 连通性探针 ==================== */

    private function probeMqttConnectivity(): void
    {
        $maxRetries = max(0, $this->configInt('edge.mqtt.probe_retries', 2));
        $maxAttempts = $maxRetries + 1;

        $lastError = '';
        $attempts = 0;

        while ($attempts < $maxAttempts) {
            $attempts++;
            $result = $this->probeOnce($attempts, $maxAttempts);

            if ($result['ok']) {
                $this->info(sprintf('[mqtt-probe] %s 可达（%.2fms）', $result['target'], $result['elapsed_ms']));
                return;
            }

            $lastError = $result['error'];
            $this->warn(sprintf('[mqtt-probe] 第 %d 次探测失败（%.2fms）：%s', $attempts, $result['elapsed_ms'], $lastError));

            if ($attempts < $maxAttempts) {
                swow_co_sleep(0.2);
            }
        }

        throw new \RuntimeException(sprintf('[mqtt-probe] MQTT 连通性探测失败，已重试 %d 次：%s，进程终止', $maxRetries, $lastError));
    }

    private function startPeriodicMqttProbe(): void
    {
        $interval = $this->configInt('edge.mqtt.probe_interval_sec', 30);
        if ($interval <= 0) {
            $this->info('[mqtt-probe:periodic] 已禁用');
            return;
        }

        $threshold = max(1, $this->configInt('edge.mqtt.probe_failure_threshold', 3));
        $consecutiveFailures = 0;
        $wasFailing = false;

        Coroutine::run(function () use ($interval, $threshold, &$consecutiveFailures, &$wasFailing): void {
            while (! $this->stopping) {
                $deadline = time() + $interval;
                while (time() < $deadline && ! $this->stopping) {
                    swow_co_sleep(0.5);
                }

                if ($this->stopping) {
                    break;
                }

                $result = $this->probeOnce();

                if ($result['ok']) {
                    if ($wasFailing) {
                        $this->info(sprintf('[mqtt-probe:periodic] 恢复，%s 可达（%.2fms）', $result['target'], $result['elapsed_ms']));
                        $wasFailing = false;
                    }

                    $consecutiveFailures = 0;
                    $this->exporter->set('edge_mqtt_probe_consecutive_failures', 0);
                    continue;
                }

                $consecutiveFailures++;
                $this->exporter->set('edge_mqtt_probe_consecutive_failures', $consecutiveFailures);

                $this->warn(sprintf('[mqtt-probe:periodic] 第 %d 次失败（%.2fms）：%s', $consecutiveFailures, $result['elapsed_ms'], $result['error']));

                if ($consecutiveFailures >= $threshold && ! $wasFailing) {
                    $this->error(sprintf('[mqtt-probe:periodic] 连续失败 %d 次，MQTT 可能不可达（阈值=%d）', $consecutiveFailures, $threshold));
                    $wasFailing = true;
                }
            }
        });

        $this->info(sprintf('[mqtt-probe:periodic] 已启动，间隔 %ds，失败阈值 %d 次', $interval, $threshold));
    }

    private function probeOnce(int $attempt = 0, int $maxAttempts = 0): array
    {
        $host = config('edge.mqtt.host', '127.0.0.1');
        $port = $this->configInt('edge.mqtt.port', 1883);
        $timeout = (float) config('edge.mqtt.probe_timeout', 3.0);

        $tlsEnabled = (bool) config('edge.mqtt.tls.enable', false);
        $scheme = $tlsEnabled ? 'tls' : 'tcp';
        $target = sprintf('%s://%s:%d', $scheme, $host, $port);

        $context = $this->buildTlsContext();

        $errno = 0;
        $errstr = '';
        $start = microtime(true);

        if ($attempt > 0 && $maxAttempts > 0) {
            $this->info(sprintf('[mqtt-probe] 探测 %s（第 %d/%d 次，超时 %.1fs）...', $target, $attempt, $maxAttempts, $timeout));
        }

        $timeoutInt = (int) ceil($timeout);
        $socket = @stream_socket_client($target, $errno, $errstr, $timeoutInt > 0 ? $timeoutInt : null, STREAM_CLIENT_CONNECT, $context);

        $elapsedMs = (microtime(true) - $start) * 1000;

        if ($socket !== false) {
            try {
                fclose($socket);
            } catch (Throwable) {
                // ignore
            }

            return [
                'ok' => true,
                'target' => $target,
                'elapsed_ms' => $elapsedMs,
                'error' => '',
            ];
        }

        $error = trim(sprintf('errno=%d errstr=%s', $errno, $errstr ?: 'unknown'));
        $this->exporter->inc('edge_mqtt_probe_failed_total');

        return [
            'ok' => false,
            'target' => $target,
            'elapsed_ms' => $elapsedMs,
            'error' => $error,
        ];
    }

    private function buildTlsContext(): mixed
    {
        $tlsEnabled = (bool) config('edge.mqtt.tls.enable', false);

        if (! $tlsEnabled) {
            return stream_context_create([]);
        }

        $options = ['ssl' => [
            'verify_peer' => (bool) config('edge.mqtt.tls.verify_peer', false),
            'verify_peer_name' => (bool) config('edge.mqtt.tls.verify_peer_name', true),
            'allow_self_signed' => (bool) config('edge.mqtt.tls.allow_self_signed', false),
        ]];

        $cafile = config('edge.mqtt.tls.cafile');
        if (is_string($cafile) && $cafile !== '') {
            $options['ssl']['cafile'] = $cafile;
        }

        $capath = config('edge.mqtt.tls.capath');
        if (is_string($capath) && $capath !== '') {
            $options['ssl']['capath'] = $capath;
        }

        $certfile = config('edge.mqtt.tls.certfile');
        if (is_string($certfile) && $certfile !== '') {
            $options['ssl']['local_cert'] = $certfile;
        }

        $keyfile = config('edge.mqtt.tls.keyfile');
        if (is_string($keyfile) && $keyfile !== '') {
            $options['ssl']['local_pk'] = $keyfile;
        }

        $passphrase = config('edge.mqtt.tls.passphrase');
        if (is_string($passphrase) && $passphrase !== '') {
            $options['ssl']['passphrase'] = $passphrase;
        }

        $alpn = config('edge.mqtt.tls.alpn');
        if (is_string($alpn) && $alpn !== '') {
            $options['ssl']['alpn_protocols'] = $alpn;
        }

        return stream_context_create($options);
    }

    /* ==================== 配置 & 初始化 ==================== */

    private function loadConfig(): void
    {
        $this->normalBatchSize = $this->configInt('edge.mqtt.batch_size', 200);
        $this->normalFlushIntervalMs = $this->configInt('edge.mqtt.flush_interval_ms', 100);
        $normalChannelSize = $this->configInt('edge.mqtt.channel_size', 4096);

        $this->highBatchSize = $this->configInt('edge.mqtt.high_batch_size', 10);
        $this->highFlushIntervalMs = $this->configInt('edge.mqtt.high_flush_interval_ms', 20);
        $highChannelSize = $this->configInt('edge.mqtt.high_channel_size', 512);

        $this->deadBatchSize = $this->configInt('edge.mqtt.dead_batch_size', 50);
        $this->deadFlushIntervalMs = $this->configInt('edge.mqtt.dead_flush_interval_ms', 1000);
        $deadChannelSize = $this->configInt('edge.mqtt.dead_channel_size', 1024);

        $this->maxPayloadLen = $this->configInt('edge.mqtt.max_payload_len', 65_536);

        $this->normalWorkers = $this->configInt('edge.mqtt.normal_workers', 4);
        $this->highWorkers = $this->configInt('edge.mqtt.high_workers', 2);
        $this->deadWorkers = $this->configInt('edge.mqtt.dead_workers', 1);
        $this->workerIdleTimeoutSec = $this->configInt('edge.mqtt.worker_idle_timeout', 5);

        $this->autoScaleEnabled = ((int) $this->option('auto-scale')) === 1;
        $this->autoScaleIntervalSec = max(10, (int) $this->option('auto-scale-interval'));
        $this->sampleRateMin = (float) $this->option('sample-rate-min');
        $this->sampleRateMax = (float) $this->option('sample-rate-max');

        $opt = $this->option('device-active-ttl');
        $this->deviceActiveTtl = max(10, (int) (
        $opt === null || $opt === '' ? config('edge.mqtt.device_active_ttl', 120) : $opt
        ));

        $this->heartbeatTopic = $this->option('heartbeat-topic') ?: 'heartbeat/response';

        $hbWriteOpt = $this->option('heartbeat-write-stream');
        $this->heartbeatWriteStream = match ($hbWriteOpt) {
            '0' => false,
            '1' => true,
            default => (bool) config('edge.mqtt.heartbeat_write_stream', true),
        };

        $this->targetNormalWorkers = $this->normalWorkers;
        $this->targetHighWorkers = $this->highWorkers;

        $this->normalChannel = new Channel($normalChannelSize);
        $this->highChannel = new Channel($highChannelSize);
        $this->deadChannel = new Channel($deadChannelSize);
    }

    private function initComponents(): void
    {
        $this->exporter = new PrometheusExporter;

        $adaptMin = $this->configInt('edge.mqtt.adapt_min_batch', 10);
        $adaptMax = $this->configInt('edge.mqtt.adapt_max_batch', 2000);
        $adaptAlpha = (float) config('edge.mqtt.adapt_alpha', 0.3);

        $this->adaptiveBatch = new MultiDimAdaptiveBatch(min: $adaptMin, max: $adaptMax, alpha: $adaptAlpha);

        $this->router = new PriorityRouter(
            highPriorityTopics: $this->resolveHighPriorityTopics(),
            batch: $this->adaptiveBatch,
        );
    }

    /**
     * @return string[]
     */
    private function resolveTopics(): array
    {
        $topics = config('edge.mqtt.topics', ['heartbeat/response']);
        if (is_string($topics)) {
            $topics = array_filter(array_map('trim', explode(',', $topics)));
        }
        return array_values((array) $topics);
    }

    /**
     * @return string[]
     */
    private function resolveHighPriorityTopics(): array
    {
        $topics = config('edge.mqtt.high_priority_topics', [
            'device/+/alarm',
            'device/+/alert',
            'device/+/control',
            'device/+/command',
            'face/+/request',
            'face/+/response',
        ]);
        if (is_string($topics)) {
            $topics = array_filter(array_map('trim', explode(',', $topics)));
        }
        return array_values((array) $topics);
    }

    /* ==================== 路由 ==================== */

    private function routeToNormalChannel(array $item, string $device): void
    {
        // ★ FIX: Swow\Channel::push() 返回 static（Channel 自身），队列满/已关闭时抛异常。
        // 原先用 `$ok === false` 判断永远不成立，异常被 NanoMQ 回调吞掉 → 消息静默丢失。
        try {
            $this->normalChannel->push($item, 0);
        } catch (Throwable $e) {
            $this->warn(sprintf(
                '[CHANNEL] 普通队列写入失败(%s)，降级写死信 device=%s',
                $e->getMessage(),
                $device,
            ));
            $this->droppedCount++;
            $this->exporter->inc('edge_dead_letter_total');
            $this->pushDeadLetter([$item], 'normal_channel_full');
            return;
        }

        $this->receivedCount++;
        $this->normalBacklog++;
    }

    private function routeToHighChannel(array $item, string $device): void
    {
        $aliveHigh = $this->countAliveWorkers($this->highWorkerCoroutines);
        if ($aliveHigh < 1) {
            $this->warn('[ROUTE] 高优 Worker 全部退出，降级普通通道 device=' . $device);
            $this->routeToNormalChannel($item, $device);
            return;
        }

        if ($this->highChannel->isFull()) {
            $this->warn('[CHANNEL] 高优队列已满，降级写死信 device=' . $device);
            $this->exporter->inc('edge_dead_letter_total');
            $this->pushDeadLetter([$item], 'high_channel_full');
            return;
        }

        // ★ FIX: push() 返回 Channel 对象，`$ret < 0` 会触发
        // "Object of class Swow\Channel could not be converted to int"，
        // 异常冒泡到 NanoMQ 回调，且 receivedCount / highBacklog 统计被跳过。
        try {
            $this->highChannel->push($item, 0);
        } catch (Throwable $e) {
            $this->error(sprintf(
                '[CHANNEL] 高优队列 push 失败(%s)，降级普通通道 device=%s',
                $e->getMessage(),
                $device,
            ));
            $this->routeToNormalChannel($item, $device);
            return;
        }

        $this->receivedCount++;
        $this->highBacklog++;
    }

    private function pushDeadLetter(array $batch, string $reason): void
    {
        $item = [
            'batch' => $batch,
            'error' => $reason,
            'ts' => microtime(true),
            '_enqueued_at' => microtime(true),
        ];

        try {
            $this->deadChannel->push($item, 0);
            $this->deadBacklog++;
        } catch (Throwable) {
            // Channel 满：丢弃，避免死循环
        }
    }

    /* ==================== 信号处理 ==================== */

    private function registerSignalHandlers(): void
    {
        foreach ([Signal::INT, Signal::TERM] as $sig) {
            Coroutine::run(function () use ($sig): void {
                try {
                    Signal::wait($sig);
                } catch (Throwable) {
                    return;
                }

                if ($this->stopping) {
                    $this->error('[signal] 再次收到信号，强制退出');
                    exit(1);
                }

                $this->warn(sprintf('[signal] 收到 %d，准备优雅停机', $sig));
                $this->stopping = true;
            });
        }
    }

    private function wakeupChannel(Channel $channel): void
    {
        $aliveNormal = $this->activeNormalWorkers;
        $aliveHigh = $this->activeHighWorkers;
        $aliveDead = $this->activeDeadWorkers;

        $count = match (true) {
            $channel === $this->normalChannel => max(1, $aliveNormal),
            $channel === $this->highChannel => max(1, $aliveHigh),
            $channel === $this->deadChannel => max(1, $aliveDead),
            default => 1,
        };

        $count = min($count, $channel->getCapacity());

        for ($i = 0; $i < $count; $i++) {
            try {
                $channel->push(self::STOP_SENTINEL, 0);
            } catch (Throwable) {
                break;
            }
        }
    }

    private function drainWorkerPools(): void
    {
        // 确保 MQTT 已断开
        if ($this->mqtt !== null && $this->mqtt->isRunning()) {
            $this->mqtt->stop();
        }

        $timeout = microtime(true) + $this->workerIdleTimeoutSec;

        while (microtime(true) < $timeout) {
            $alive = 0;
            foreach ([...$this->normalWorkerCoroutines, ...$this->highWorkerCoroutines, ...$this->deadWorkerCoroutines] as $coroutine) {
                try {
                    if ($coroutine->isAlive()) {
                        $alive++;
                    }
                } catch (Throwable) {
                    // ignore
                }
            }

            if ($alive === 0) {
                $this->info('[edge] 所有 Worker 已退出');
                break;
            }

            swow_co_sleep(0.1);
        }

        if (microtime(true) >= $timeout) {
            $this->warn('[edge] drainWorkerPools 超时，强制退出');
        }
    }

    /* ==================== Worker 池启动 ==================== */

    private function startNormalWorkerPool(): void
    {
        for ($i = 0; $i < $this->normalWorkers; $i++) {
            $this->normalWorkerCoroutines[] = Coroutine::run(function (): void {
                $this->runNormalWorker();
            });
        }
    }

    private function startHighWorkerPool(): void
    {
        for ($i = 0; $i < $this->highWorkers; $i++) {
            $this->highWorkerCoroutines[] = Coroutine::run(function (): void {
                $this->runHighWorker();
            });
        }
    }

    private function startDeadLetterWorkerPool(): void
    {
        for ($i = 0; $i < $this->deadWorkers; $i++) {
            $this->deadWorkerCoroutines[] = Coroutine::run(function (): void {
                $this->runDeadLetterWorker();
            });
        }
    }

    /* ==================== 从池数组移除自己 ==================== */

    private function removeSelfFromPool(array &$pool, Coroutine $self): void
    {
        foreach ($pool as $i => $c) {
            if ($c === $self) {
                unset($pool[$i]);
                break;
            }
        }
    }

    /* ==================== 普通 Worker（分片版） ==================== */

    private function runNormalWorker(): void
    {
        $batch = [];
        $capacity = max(1, $this->normalBatchSize);
        $flushIntervalSec = max(0.02, $this->normalFlushIntervalMs / 1000); // ★ 下限 20ms
        $lastFlush = microtime(true);
        $workerId = Coroutine::getCurrent()->getId();

        $this->debugWorker('normal', $workerId, 'started');
        $this->activeNormalWorkers++;

        while (true) {
            if ($this->stopping && $this->normalChannel->isAvailable() === false) {
                break;
            }

            // ★ FIX: Swow\Channel::pop() 的 timeout 单位是「微秒」。
            // 原来乘 1000 的数值被当成微秒，100ms 实际只等 0.1ms，造成高频空转。
            $timeout = $this->stopping ? 0 : max(20_000, (int)(
                ($flushIntervalSec - (microtime(true) - $lastFlush)) * 1_000_000
            ));

            $item = null;
            try {
                /** @var array<string, mixed>|null $item */
                $item = $this->normalChannel->pop($timeout);
            } catch (Throwable) {
                // ★ FIX: pop 超时在 Swow 里是抛异常；超时必须按时间驱动 flush，
                // 不能重置 $lastFlush，否则低流量下的尾批永远不落盘。
                if ($batch !== [] && (microtime(true) - $lastFlush) >= $flushIntervalSec) {
                    $this->flushNormalBatch($batch);
                    $batch = [];
                    $lastFlush = microtime(true);
                }
                continue;
            }

            // 以下逻辑不变...
            if ($item === self::STOP_SENTINEL) {
                if ($batch !== []) {
                    $this->flushNormalBatch($batch);
                    $batch = [];
                }
                $this->activeNormalWorkers = max(0, $this->activeNormalWorkers - 1);
                $this->removeSelfFromPool($this->normalWorkerCoroutines, Coroutine::getCurrent());
                $this->debugWorker('normal', $workerId, 'stopped by sentinel');
                break;
            }

            if ($item === null) {
                if ($batch !== []) {
                    $this->flushNormalBatch($batch);
                    $batch = [];
                    $lastFlush = microtime(true);
                }

                if ($this->stopping) {
                    $this->activeNormalWorkers = max(0, $this->activeNormalWorkers - 1);
                    $this->removeSelfFromPool($this->normalWorkerCoroutines, Coroutine::getCurrent());
                    $this->debugWorker('normal', $workerId, 'stopped by null+stopping');
                    break;
                }

                continue;
            }

            $batch[] = $item;

            if (count($batch) >= $capacity) {
                $this->flushNormalBatch($batch);
                $batch = [];
                $lastFlush = microtime(true);
            }
        }
    }

    /* ==================== 高优 Worker（分片版） ==================== */

    private function runHighWorker(): void
    {
        $batch = [];
        $capacity = 1; // 每条立即 flush
        $flushIntervalSec = 0.02; // 20ms
        $lastFlush = microtime(true);
        $workerId = Coroutine::getCurrent()->getId();

        $this->debugWorker('high', $workerId, 'started');
        $this->activeHighWorkers++;

        while (true) {
            if ($this->stopping && !$this->highChannel->isAvailable()) {
                break;
            }

            // ★ FIX: 微秒单位，下限 20ms（20_000us）
            $timeout = $this->stopping ? 0 : max(20_000, (int)(
                ($flushIntervalSec - (microtime(true) - $lastFlush)) * 1_000_000
            ));

            $item = null;
            try {
                /** @var array<string, mixed>|null $item */
                $item = $this->highChannel->pop($timeout);
            } catch (Throwable) {
                // ★ FIX: 超时分支保持计时器连续，按时间补充 flush
                if ($batch !== [] && (microtime(true) - $lastFlush) >= $flushIntervalSec) {
                    $this->flushHighBatch($batch);
                    $batch = [];
                    $lastFlush = microtime(true);
                }
                if ($this->output->isVerbose()) {
                    $this->line(sprintf('[WORKER:high] #%d pop timeout', $workerId));
                }
                continue;
            }

            if ($item === self::STOP_SENTINEL) {
                if ($batch !== []) {
                    $this->flushHighBatch($batch);
                    $batch = [];
                }
                $this->activeHighWorkers = max(0, $this->activeHighWorkers - 1);
                $this->removeSelfFromPool($this->highWorkerCoroutines, Coroutine::getCurrent());
                $this->debugWorker('high', $workerId, 'stopped by sentinel');
                break;
            }

            if ($item === null) {
                if ($batch !== []) {
                    $this->flushHighBatch($batch);
                    $batch = [];
                    $lastFlush = microtime(true);
                }

                if ($this->stopping) {
                    $this->activeHighWorkers = max(0, $this->activeHighWorkers - 1);
                    $this->removeSelfFromPool($this->highWorkerCoroutines, Coroutine::getCurrent());
                    $this->debugWorker('high', $workerId, 'stopped by stopping');
                    break;
                }

                continue;
            }

            // ★ 确认拿到消息
            $this->line(sprintf(
                '[WORKER:high] #%d got topic=%s device=%s',
                $workerId,
                $item['topic'] ?? 'unknown',
                $item['device'] ?? 'unknown'
            ));

            $batch[] = $item;

            if (count($batch) >= $capacity) {
                $this->flushHighBatch($batch);
                $batch = [];
                $lastFlush = microtime(true);
            }
        }
    }

    /* ==================== 死信 Worker ==================== */

    private function runDeadLetterWorker(): void
    {
        $batch = [];
        $capacity = max(1, $this->deadBatchSize);
        $flushIntervalSec = max(0.001, $this->deadFlushIntervalMs / 1000);
        $lastFlush = microtime(true);
        $workerId = Coroutine::getCurrent()->getId();

        $this->debugWorker('dead', $workerId, 'started');
        $this->activeDeadWorkers++;

        while (true) {
            // ★ FIX: 微秒单位 + 至少 1ms 下限，避免 timeout=0 时的忙循环空转
            $timeout = $this->stopping ? 0 : max(1_000, (int) (
                ($flushIntervalSec - (microtime(true) - $lastFlush)) * 1_000_000
            ));

            $item = null;
            try {
                /** @var array<string, mixed>|null $item */
                $item = $this->deadChannel->pop($timeout);
            } catch (Throwable) {
                // ★ FIX: 超时按时间驱动 flush，不重置计时器
                if ($batch !== [] && (microtime(true) - $lastFlush) >= $flushIntervalSec) {
                    $this->flushDeadLetterBatch($batch);
                    $batch = [];
                    $lastFlush = microtime(true);
                }
                continue;
            }

            if ($item === null) {
                if ($batch !== []) {
                    $this->flushDeadLetterBatch($batch);
                    $batch = [];
                    $lastFlush = microtime(true);
                }

                if ($this->stopping) {
                    $this->activeDeadWorkers = max(0, $this->activeDeadWorkers - 1);
                    $this->removeSelfFromPool($this->deadWorkerCoroutines, Coroutine::getCurrent());
                    $this->debugWorker('dead', $workerId, 'stopped by stopping');
                    break;
                }

                continue;
            }

            $batch[] = $item;

            if (count($batch) >= $capacity) {
                $this->flushDeadLetterBatch($batch);
                $batch = [];
                $lastFlush = microtime(true);
            }
        }
    }

    private function countAliveWorkers(array $coroutines): int
    {
        $alive = 0;
        foreach ($coroutines as $coroutine) {
            try {
                if ($coroutine->isAlive()) {
                    $alive++;
                }
            } catch (Throwable) {
                // ignore
            }
        }
        return $alive;
    }

    /* ==================== Redis 写入（分片版） ==================== */

    private function flushNormalBatch(array $batch): void
    {
        if ($batch === []) {
            return;
        }

        $flushStart = microtime(true);
        $count = count($batch);
        $this->recordWaitMs('normal', $batch);

        try {
            $shardBatches = [];
            foreach ($batch as $item) {
                $shardNo = $item['shard_no'] ?? 0;
                $shardBatches[$shardNo][] = $item;
            }

            foreach ($shardBatches as $shardNo => $items) {
                $conn = RedisShardManager::popByShard($shardNo);
                if ($conn === null) {
                    $this->error(sprintf('[FLUSH] 无法获取分片 %d 的 Redis 连接', $shardNo));
                    continue;
                }

                try {
                    $streamKey = $this->getDataStreamKey($shardNo);
                    $conn->multi(\Redis::PIPELINE);

                    foreach ($items as $item) {
                        $conn->xAdd(
                            $streamKey,
                            '*',
                            [
                                'topic' => $item['topic'],
                                'payload' => $item['payload'],
                                'device' => $item['device'],
                                'ts' => $item['ts'],
                            ],
                            self::STREAM_MAX_LEN,
                            true,
                        );
                    }

                    $results = $conn->exec();
                    if (is_array($results) && count($results) !== count($items)) {
                        fwrite(STDERR, sprintf(
                            "[FLUSH-DIAG] shard=%d pipeline 结果异常 expect=%d actual=%d\n",
                            $shardNo, count($items), count($results)
                        ));
                    }
                } finally {
                    RedisShardManager::pushByShard($shardNo, $conn);
                }
            }

            $this->normalFlushedCount += $count;
            $this->exporter->inc('edge_data_write_success_total', $count);
            $this->exporter->inc('edge_flushed_total_normal', $count);
            $this->normalBacklog = max(0, $this->normalBacklog - $count);

            $elapsedMs = (microtime(true) - $flushStart) * 1000;
            $this->normalFlushLatencySum += $elapsedMs;
            $this->normalFlushTimes++;

            $this->exporter->observe('edge_flush_duration_ms_normal', $elapsedMs);

            $this->line(sprintf(
                '[FLUSH] batch=%d → %d 个分片 (%.2fms)',
                $count,
                count($shardBatches),
                $elapsedMs,
            ));
        } catch (Throwable $e) {
            $this->error(sprintf('[FLUSH] 普通写入失败（%d 条）→ 死信：%s', $count, $e->getMessage()));
            $this->exporter->inc('edge_redis_write_failed_total', $count);
            $this->pushDeadLetter($batch, $e->getMessage());
        }
    }

    private function flushHighBatch(array $batch): void
    {
        if ($batch === []) {
            return;
        }

        $flushStart = microtime(true);
        $count = count($batch);
        $this->recordWaitMs('high', $batch);

        try {
            $shardBatches = [];
            foreach ($batch as $item) {
                $shardNo = $item['shard_no'] ?? 0;
                $shardBatches[$shardNo][] = $item;
            }

            foreach ($shardBatches as $shardNo => $items) {
                $conn = RedisShardManager::popByShard($shardNo);
                if ($conn === null) {
                    continue;
                }

                try {
                    $conn->multi(\Redis::PIPELINE);

                    foreach ($items as $item) {
                        $isHb = $item['is_heartbeat'] ?? false;
                        if ($isHb) {
                            $stream = self::STREAM_HEARTBEAT;
                            $maxLen = self::HEARTBEAT_MAX_LEN;
                        } else {
                            $stream = $this->getHighStreamKey($shardNo);
                            $maxLen = self::STREAM_MAX_LEN;
                        }

                        $conn->xAdd(
                            $stream,
                            '*',
                            [
                                'topic' => $item['topic'],
                                'payload' => $item['payload'],
                                'device' => $item['device'],
                                'ts' => $item['ts'],
                            ],
                            $maxLen,
                            true,
                        );
                    }

                    $conn->exec();
                } finally {
                    RedisShardManager::pushByShard($shardNo, $conn);
                }
            }

            $this->highFlushedCount += $count;
            $this->exporter->inc('edge_high_write_success_total', $count);
            $this->exporter->inc('edge_flushed_total_high', $count);
            $this->highBacklog = max(0, $this->highBacklog - $count);

            $elapsedMs = (microtime(true) - $flushStart) * 1000;
            $this->highFlushLatencySum += $elapsedMs;
            $this->highFlushTimes++;

            $this->exporter->observe('edge_flush_duration_ms_high', $elapsedMs);

            $this->line(sprintf('[FLUSH:HIGH] batch=%d → %d 分片 (%.2fms)', $count, count($shardBatches), $elapsedMs));
        } catch (Throwable $e) {
            $this->error(sprintf('[FLUSH:HIGH] 失败（%d 条）→ 死信：%s', $count, $e->getMessage()));
            $this->exporter->inc('edge_redis_write_failed_total', $count);
            $this->pushDeadLetter($batch, $e->getMessage());
        }
    }

    private function flushDeadLetterBatch(array $batch): void
    {
        if ($batch === []) {
            return;
        }

        $flushStart = microtime(true);
        $this->recordWaitMs('dead', $batch);

        try {
            $conn = RedisShardManager::popByShard(0);
            if ($conn === null) {
                return;
            }

            try {
                $conn->multi(\Redis::PIPELINE);

                foreach ($batch as $item) {
                    $batchData = $item['batch'] ?? [$item];
                    $reason = $item['error'] ?? 'unknown';

                    foreach ($batchData as $subItem) {
                        $conn->xAdd(
                            self::STREAM_DEAD,
                            '*',
                            [
                                'topic' => $subItem['topic'] ?? 'unknown',
                                'payload' => $subItem['payload'] ?? '',
                                'device' => $subItem['device'] ?? 'unknown',
                                'ts' => $subItem['ts'] ?? sprintf('%.3f', microtime(true)),
                                'error' => $reason,
                            ],
                            self::DEAD_MAX_LEN,
                            true,
                        );
                    }
                }

                $conn->exec();
            } finally {
                RedisShardManager::pushByShard(0, $conn);
            }

            $this->deadFlushedCount += count($batch);
            $this->exporter->inc('edge_flushed_total_dead', count($batch));
            $elapsedMs = (microtime(true) - $flushStart) * 1000;
            $this->deadFlushLatencySum += $elapsedMs;
            $this->deadFlushTimes++;

            $this->line(sprintf('[FLUSH:DEAD] batch=%d (%.2fms)', count($batch), $elapsedMs));
        } catch (Throwable $e) {
            $this->error(sprintf('[FLUSH:DEAD] 失败：%s', $e->getMessage()));
        }
    }

    /* ==================== 统计 & 指标 ==================== */

    private function collectWorkerMetrics(): void
    {
        // ★ FIX: 原名 edge_active_normal_workers / edge_normal_backlog / edge_dropped_total
        // 在 PrometheusExporter::reset() 中并未注册，set() 会被静默忽略；
        // edge_received_total 是 counter，也不能用 set() 覆盖（由 inc() 累加）。
        $this->exporter->set('edge_active_workers_normal', $this->activeNormalWorkers);
        $this->exporter->set('edge_active_workers_high', $this->activeHighWorkers);
        $this->exporter->set('edge_active_workers_dead', $this->activeDeadWorkers);
        $this->exporter->set('edge_active_clients', $this->activeClientCount);

        $this->exporter->set('edge_channel_backlog_normal', $this->normalBacklog);
        $this->exporter->set('edge_channel_backlog_high', $this->highBacklog);
        $this->exporter->set('edge_channel_backlog_dead', $this->deadBacklog);
        $this->exporter->set('edge_queue_backlog', $this->normalBacklog + $this->highBacklog + $this->deadBacklog);

        $this->exporter->set('edge_message_wait_ms_normal', $this->lastNormalWaitMs);
        $this->exporter->set('edge_message_wait_ms_high', $this->lastHighWaitMs);
        $this->exporter->set('edge_message_wait_ms_dead', $this->lastDeadWaitMs);

        $this->exporter->set('edge_flush_latency_ms_normal', $this->avgLatency($this->normalFlushLatencySum, $this->normalFlushTimes));
        $this->exporter->set('edge_flush_latency_ms_high', $this->avgLatency($this->highFlushLatencySum, $this->highFlushTimes));
        $this->exporter->set('edge_flush_latency_ms_dead', $this->avgLatency($this->deadFlushLatencySum, $this->deadFlushTimes));

        $this->exporter->set('edge_qps', $this->currentQps());
        $this->exporter->set('edge_current_flush_interval_ms', $this->normalFlushIntervalMs);
        $this->exporter->set('edge_mqtt_connected', ($this->mqtt !== null && $this->mqtt->isRunning()) ? 1 : 0);

        // counter 类指标一律由 inc() 累加，此处不再用 set() 覆盖
    }

    /** 平均 flush 耗时（毫秒） */
    private function avgLatency(float $sum, int $times): float
    {
        return $times > 0 ? $sum / $times : 0.0;
    }

    /** 最近 1 秒内收到的消息数（qpsWindow 上限 200，极高流量下会低估） */
    private function currentQps(): float
    {
        if ($this->qpsWindow === []) {
            return 0.0;
        }

        $cutoff = microtime(true) - 1.0;
        $hits = 0;
        foreach ($this->qpsWindow as $t) {
            if ($t >= $cutoff) {
                $hits++;
            }
        }

        return (float) $hits;
    }

    /** 记录本批消息在 Channel 中的平均等待时间（毫秒） */
    private function recordWaitMs(string $channel, array $batch): void
    {
        $now = microtime(true);
        $sum = 0.0;
        $n = 0;

        foreach ($batch as $item) {
            if (isset($item['_enqueued_at'])) {
                $sum += ($now - (float) $item['_enqueued_at']) * 1000;
                $n++;
            }
        }

        if ($n === 0) {
            return;
        }

        $avg = $sum / $n;
        if ($channel === 'normal') {
            $this->lastNormalWaitMs = $avg;
        } elseif ($channel === 'high') {
            $this->lastHighWaitMs = $avg;
        } elseif ($channel === 'dead') {
            $this->lastDeadWaitMs = $avg;
        }
    }

    private function printStats(): void
    {
        $uptime = microtime(true) - $this->startTime;
        $this->info('========== 统计 ==========');
        $this->info(sprintf('运行时间：%.1fs', $uptime));
        $this->info(sprintf('接收：%d', $this->receivedCount));
        $this->info(sprintf('普通 flush：%d', $this->normalFlushedCount));
        $this->info(sprintf('高优 flush：%d', $this->highFlushedCount));
        $this->info(sprintf('死信 flush：%d', $this->deadFlushedCount));
        $this->info(sprintf('心跳 flush：%d', $this->heartbeatFlushedCount));
        $this->info(sprintf('丢弃：%d', $this->droppedCount));
        $this->info(sprintf('高优积压：%d', $this->highBacklog));
        $this->info(sprintf('普通积压：%d', $this->normalBacklog));
        if ($this->highFlushTimes > 0) {
            $this->info(sprintf('高优平均延迟：%.2fms', $this->highFlushLatencySum / $this->highFlushTimes));
        }
        if ($this->normalFlushTimes > 0) {
            $this->info(sprintf('普通平均延迟：%.2fms', $this->normalFlushLatencySum / $this->normalFlushTimes));
        }
        $this->info('==========================');
    }

    private function nowString(): string
    {
        return date('Y-m-d H:i:s');
    }

    private function getServerIp(): array
    {
        $host = '127.0.0.1';
        $scheme = 'http';

        if (PHP_OS_FAMILY === 'Windows') {
            $host = gethostbyname(gethostname());
        } else {
            try {
                $output = [];
                @exec('hostname -I 2>/dev/null', $output);
                if (! empty($output) && ! empty($output[0])) {
                    $ips = preg_split('/\s+/', trim($output[0]));
                    if (! empty($ips[0])) {
                        $host = $ips[0];
                    }
                }
            } catch (Throwable) {
                // ignore
            }
        }

        return [$scheme, $host];
    }

    private function configInt(string $key, int $default): int
    {
        return (int) config($key, $default);
    }

    private function debugWorker(string $type, int $workerId, string $message): void
    {
        // Laravel 内置：php artisan xxx -v / -vv / -vvv 时 $this->output->isVerbose() 为 true
        if ($this->output->isVerbose()) {
            $this->line(sprintf('[WORKER:%s] #%d %s', $type, $workerId, $message));
        }
    }
}
