<?php

declare(strict_types=1);

namespace DagaSmart\Edge\Console\Commands;

use DagaSmart\Edge\Engine\NanoMQ;
use DagaSmart\Edge\Support\MultiDimAdaptiveBatch;
use DagaSmart\Edge\Support\PriorityRouter;
use DagaSmart\Edge\Support\PrometheusExporter;
use Illuminate\Console\Command;
use Swow\Channel;
use Swow\Coroutine;
use Swow\Signal;
use Throwable;

/**
 * 订阅 NanoMQ 多主题，按优先级路由并批量写入 Redis Stream（Worker Pool 版 + Auto-Scaler）
 *
 * 架构：
 *   MQTT 接收协程
 *     ├─ 高优 topic → highChannel → HighWorkerPool → stream:device:high
 *     ├─ 普通 topic → normalChannel → NormalWorkerPool → stream:device:data
 *     ├─ 写入失败 → deadChannel → DeadLetterWorkerPool → stream:device:dead
 *     └─ Auto-Scaler：根据入库侧 device:active:* 在线数自适应调节 worker/batch/采样
 *
 * ★ 认证侧与入库侧解耦：auth.php 不再写 device:alive:*
 * ★ 心跳 topic 匹配即续期 device:active:{sn}（payload 异常也续）
 */
class EdgeInsertRedisDimCommand extends Command
{
    protected $signature = 'edge:insert-redis-dim
        {--auto-scale=1 : 启用自动伸缩(0/1)}
        {--auto-scale-interval=30 : 自动伸缩评估间隔(秒)}
        {--sample-rate-min=0.2 : 最小采样率(低负载时)}
        {--sample-rate-max=1.0 : 最大采样率}
        {--device-active-ttl= : 设备活跃 key TTL(秒)}
        {--heartbeat-topic=heartbeat/response : 心跳 topic}';

    protected $description = '订阅 NanoMQ 多主题，按优先级路由并批量写入 Redis Stream（含自适应调节）';

    /* ==================== Stream ==================== */
    private const string STREAM_HIGH = 'stream:device:high';

    private const string STREAM_DATA = 'stream:device:data';

    private const string STREAM_DEAD = 'stream:device:dead';

    private const int STREAM_MAX_LEN = 50_000;

    private const int DEAD_MAX_LEN = 10_000;

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

    private int $cachedClientCount = 1;

    /* ==================== ★ 心跳续期配置 ==================== */
    private int $deviceActiveTtl = 120;

    private string $heartbeatTopic = 'heartbeat/response';

    /* ==================== 统计 ==================== */
    private int $receivedCount = 0;

    private int $normalFlushedCount = 0;

    private int $highFlushedCount = 0;

    private int $deadFlushedCount = 0;

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

    /** @var array<float, int> */
    private array $qpsWindow = [];

    public function handle(): int
    {
        $this->startTime = microtime(true);
        $this->info('[edge:insert-redis] 启动，PID='.getmypid());

        $this->loadConfig();
        $this->initComponents();
        $this->initRedisPool();
        $this->registerSignalHandlers();

        $metricsPort = $this->configInt('edge.metrics.port', 9090);
        $metricsHost = config('edge.metrics.host', '0.0.0.0');

        $this->exporter->startMetricsServer($metricsPort, $metricsHost, function (): array {
            $this->collectWorkerMetrics();

            return $this->exporter->getMetrics();
        });

        [$scheme, $serverIp] = $this->getServerIp();

        $this->info(sprintf('[metrics] Prometheus 监听 %s:%d', $metricsHost, $metricsPort));
        $this->info(sprintf('[metrics] Prometheus 本地 %s://%s:%d', $scheme, $serverIp, $metricsPort));
        echo "[metrics] Prometheus 预览 {$scheme}://{$serverIp}:{$metricsPort}/metrics\n";
        echo "[metrics] Prometheus 接口 {$scheme}://{$serverIp}:{$metricsPort}/api/metrics\n";

        // 启动 Worker 池
        $this->startNormalWorkerPool();
        $this->startHighWorkerPool();
        $this->startDeadLetterWorkerPool();

        // ★ 启动 Auto-Scaler
        if ($this->autoScaleEnabled) {
            $this->startAutoScaler();
        }

        // 启动期探针：失败直接终止，避免订阅后才发现连不上
        $this->probeMqttConnectivity();

        // 运行期定时探针：后台协程，失败告警不退出
        $this->startPeriodicMqttProbe();

        $topics = $this->resolveTopics();

        // MQTT 连接 + 自动重连循环
        while (! $this->stopping) {
            try {
                $this->mqtt = new NanoMQ;

                $this->info('=====================================');
                $this->info(sprintf('MQTT 订阅已启动 | %s', $this->nowString()));
                $this->info('订阅主题：'.implode(', ', $topics));
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
                    'AutoScale=%s interval=%ds sample=%.2f~%.2f',
                    $this->autoScaleEnabled ? 'on' : 'off',
                    $this->autoScaleIntervalSec,
                    $this->sampleRateMin,
                    $this->sampleRateMax,
                ));
                $this->info(sprintf(
                    '心跳续期：topic=%s ttl=%ds',
                    $this->heartbeatTopic,
                    $this->deviceActiveTtl,
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
                        }
                    } catch (Throwable) {
                        // 非法 payload 仍写入，由下游处理
                    }

                    $this->line(sprintf(
                        '[MQTT] topic=%s device=%s payload_len=%d',
                        $topic,
                        $device,
                        $payloadLen,
                    ));

                    // ★ 心跳 topic → 无条件续期（payload 异常也续，topic 本身就是心跳信号）
                    if ($topic === $this->heartbeatTopic) {
                        $activeKey = ($device !== 'unknown') ? $device : null;
                        if ($activeKey !== null && $activeKey !== '') {
                            $this->renewDeviceActive($activeKey);
                        }

                        // 心跳消息不进入数据入库流程
                        return;
                    }

                    // ★ 采样门控：非高优 topic 按采样率丢弃
                    if (! $this->router->isHighPriority($topic) && $this->sampleRate < 1.0) {
                        if ((mt_rand(1, 10000) / 10000.0) > $this->sampleRate) {
                            $this->droppedCount++;
                            $this->exporter->inc('edge_dropped_by_sampling_total');

                            return;
                        }
                    }

                    $item = [
                        'topic' => $topic,
                        'payload' => $message,
                        'device' => $device,
                        'ts' => sprintf('%.3f', microtime(true)),
                        '_enqueued_at' => microtime(true),
                    ];

                    $this->exporter->inc('edge_received_total');

                    if ($this->router->isHighPriority($topic)) {
                        $this->routeToHighChannel($item, $device);
                    } else {
                        $this->routeToNormalChannel($item, $device);
                    }
                };

                foreach ($topics as $topic) {
                    $this->mqtt->subscribe($topic, $handler);
                }

                $this->mqtt->wait();

                if ($this->stopping) {
                    break;
                }

                $this->warn('[mqtt] 连接断开，3 秒后重连...');
                sleep(3);

            } catch (Throwable $e) {
                if ($this->stopping) {
                    break;
                }

                $this->error('[mqtt] 连接异常：'.$e->getMessage().'，3 秒后重连...');
                sleep(3);
            }
        }

        $this->printStats();
        $this->exporter->stop();
        $this->info('[edge:insert-redis] 已停止');

        return 0;
    }

    /* ==================== ★ 心跳续期 ==================== */

    /**
     * 写 device:active:{sn} EX ttl
     * key 集中落在 shard 0（helper.php redisExtractRouteValue 显式声明）
     */
    protected function renewDeviceActive(string $sn): void
    {
        $key = 'device:active:'.$sn;
        $ttl = $this->deviceActiveTtl;

        try {
            withRedisForKey($key, function (\Redis $conn) use ($key, $sn, $ttl): void {
                $conn->setex($key, $ttl, json_encode([
                    'sn' => $sn,
                    'ts' => time(),
                ], JSON_UNESCAPED_SLASHES));
            });
            $this->exporter->inc('edge_heartbeat_renew_total');
        } catch (Throwable $e) {
            fwrite(STDERR, '[renewDeviceActive] '.$e->getMessage().PHP_EOL);
        }
    }

    /* ==================== ★ Auto-Scaler ==================== */

    private function startAutoScaler(): void
    {
        Coroutine::run(function (): void {
            $this->info('[auto-scaler] started');
            while (! $this->stopping) {
                // 分段 sleep，及时响应停机
                $deadline = time() + $this->autoScaleIntervalSec;
                while (time() < $deadline && ! $this->stopping) {
                    usleep(500_000);
                }

                if ($this->stopping) {
                    break;
                }

                $clientCount = $this->getActiveClientCount();
                $this->activeClientCount = $clientCount;

                // ---- 目标 worker 数 ----
                $targetNormal = match (true) {
                    $clientCount <= 3 => 1,
                    $clientCount <= 10 => 2,
                    $clientCount <= 30 => 3,
                    $clientCount <= 100 => 5,
                    $clientCount <= 300 => 8,
                    default => 12,
                };

                $targetHigh = match (true) {
                    $clientCount <= 5 => 1,
                    $clientCount <= 50 => 2,
                    default => 3,
                };

                // ---- 动态 batch size ----
                $newBatchSize = max(10, min(500, (int) ($clientCount * 2 + 20)));

                // ---- 动态 flush 间隔 ----
                $newFlushInterval = match (true) {
                    $clientCount <= 3 => 1000,
                    $clientCount <= 10 => 500,
                    $clientCount <= 30 => 200,
                    default => $this->normalFlushIntervalMs,
                };

                // ---- 采样率 ----
                if ($clientCount <= 3) {
                    $newSampleRate = $this->sampleRateMin;
                } elseif ($clientCount <= 10) {
                    $newSampleRate = 0.5;
                } else {
                    $newSampleRate = $this->sampleRateMax;
                }

                // ---- 应用变更 ----
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

                // batch/flush/采样 实时生效（不需要重启 worker）
                $this->normalBatchSize = $newBatchSize;
                $this->normalFlushIntervalMs = $newFlushInterval;
                $this->sampleRate = $newSampleRate;

                // Prometheus 指标
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
            // 扩容：启动新 worker
            $toAdd = $target - $current;
            for ($i = 0; $i < $toAdd; $i++) {
                $this->normalWorkerCoroutines[] = Coroutine::run(function (): void {
                    $this->runNormalWorker();
                });
            }
            $this->normalWorkers = $target;
            $this->info(sprintf('[auto-scaler] normal workers %d → %d (+%d)', $current, $target, $toAdd));
        } elseif ($target < $current) {
            // 缩容：push null 让多余 worker 退出
            $toStop = $current - $target;
            for ($i = 0; $i < $toStop; $i++) {
                try {
                    $this->normalChannel->push(null);
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
            $toStop = $current - $target;
            for ($i = 0; $i < $toStop; $i++) {
                try {
                    $this->highChannel->push(null);
                } catch (Throwable) {
                    break;
                }
            }
            $this->highWorkers = $target;
            $this->info(sprintf('[auto-scaler] high workers %d → %d (-%d)', $current, $target, $toStop));
        }
    }

    /**
     * 从入库侧心跳续期的 Redis key 统计活跃设备数量
     * SCAN device:active:* 只在 shard 0 上执行（所有 key 集中存储）
     * 30s 内复用缓存，避免频繁 SCAN
     */
    private function getActiveClientCount(): int
    {
        $now = time();

        if (($now - $this->lastScanTime) < 30) {
            return $this->cachedClientCount;
        }

        try {
            $pattern = 'device:active:*';
            $count = withRedisForKey(
                $pattern,
                static function (\Redis $conn) use ($pattern): int {
                    $n = 0;
                    $it = null;
                    do {
                        $keys = $conn->scan($it, $pattern, 500);
                        if (! empty($keys) && is_array($keys)) {
                            $n += count($keys);
                        }
                    } while ($it > 0);

                    return $n;
                }
            );

            $this->cachedClientCount = $count > 0 ? $count : 1;
            $this->lastScanTime = $now;

            return $this->cachedClientCount;
        } catch (Throwable $e) {
            fwrite(STDERR, '[getActiveClientCount] '.$e->getMessage().PHP_EOL);

            return $this->cachedClientCount;
        }
    }

    /* ==================== MQTT 连通性探针 ==================== */

    /**
     * 启动期 TCP/TLS 连通探测，支持重试与可配置超时
     * 失败抛 RuntimeException，进程终止
     */
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
                $this->info(sprintf(
                    '[mqtt-probe] %s 可达（%.2fms）',
                    $result['target'],
                    $result['elapsed_ms'],
                ));

                return;
            }

            $lastError = $result['error'];
            $this->warn(sprintf(
                '[mqtt-probe] 第 %d 次探测失败（%.2fms）：%s',
                $attempts,
                $result['elapsed_ms'],
                $lastError,
            ));

            if ($attempts < $maxAttempts) {
                usleep(200_000); // 200ms，避免瞬时重试风暴
            }
        }

        throw new \RuntimeException(sprintf(
            '[mqtt-probe] MQTT 连通性探测失败，已重试 %d 次：%s，进程终止',
            $maxRetries,
            $lastError,
        ));
    }

    /**
     * 启动后台定时探针协程
     * - 连续失败达到阈值：告警 + 指标
     * - 恢复成功：通知
     * - 不抛异常、不退出进程
     */
    private function startPeriodicMqttProbe(): void
    {
        $interval = $this->configInt('edge.mqtt.probe_interval_sec', 30);
        if ($interval <= 0) {
            $this->info('[mqtt-probe:periodic] 已禁用（probe_interval_sec <= 0）');

            return;
        }

        $threshold = max(1, $this->configInt('edge.mqtt.probe_failure_threshold', 3));
        $consecutiveFailures = 0;
        $wasFailing = false;

        Coroutine::run(function () use ($interval, $threshold, &$consecutiveFailures, &$wasFailing): void {
            while (! $this->stopping) {
                // 分段 sleep，及时响应停机信号
                $deadline = time() + $interval;
                while (time() < $deadline && ! $this->stopping) {
                    usleep(500_000); // 500ms
                }

                if ($this->stopping) {
                    break;
                }

                $result = $this->probeOnce();

                if ($result['ok']) {
                    if ($wasFailing) {
                        $this->info(sprintf(
                            '[mqtt-probe:periodic] 恢复，%s 可达（%.2fms）',
                            $result['target'],
                            $result['elapsed_ms'],
                        ));
                        $wasFailing = false;
                    }

                    $consecutiveFailures = 0;
                    $this->exporter->set('edge_mqtt_probe_consecutive_failures', 0);

                    continue;
                }

                $consecutiveFailures++;
                $this->exporter->set('edge_mqtt_probe_consecutive_failures', $consecutiveFailures);

                $this->warn(sprintf(
                    '[mqtt-probe:periodic] 第 %d 次失败（%.2fms）：%s',
                    $consecutiveFailures,
                    $result['elapsed_ms'],
                    $result['error'],
                ));

                if ($consecutiveFailures >= $threshold && ! $wasFailing) {
                    $this->error(sprintf(
                        '[mqtt-probe:periodic] 连续失败 %d 次，MQTT 可能不可达（阈值=%d）',
                        $consecutiveFailures,
                        $threshold,
                    ));
                    $wasFailing = true;
                }
            }
        });

        $this->info(sprintf(
            '[mqtt-probe:periodic] 已启动，间隔 %ds，失败阈值 %d 次',
            $interval,
            $threshold,
        ));
    }

    /**
     * 执行单次 TCP/TLS 探测
     *
     * @return array{ok: bool, target: string, elapsed_ms: float, error: string}
     */
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
            $this->info(sprintf(
                '[mqtt-probe] 探测 %s（第 %d/%d 次，超时 %.1fs）...',
                $target,
                $attempt,
                $maxAttempts,
                $timeout,
            ));
        }

        // fsockopen 不支持 $context 参数，Windows 下会直接抛 ArgumentCountError
        // 统一用 stream_socket_client，跨平台一致
        $timeoutInt = (int) ceil($timeout);
        $socket = @stream_socket_client(
            $target,
            $errno,
            $errstr,
            $timeoutInt > 0 ? $timeoutInt : null,
            STREAM_CLIENT_CONNECT,
            $context,
        );

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

    /**
     * 构建 TLS 流上下文
     */
    private function buildTlsContext(): mixed
    {
        $tlsEnabled = (bool) config('edge.mqtt.tls.enable', false);

        if (! $tlsEnabled) {
            return stream_context_create([]);
        }

        $options = [
            'ssl' => [
                'verify_peer' => (bool) config('edge.mqtt.tls.verify_peer', false),
                'verify_peer_name' => (bool) config('edge.mqtt.tls.verify_peer_name', true),
                'allow_self_signed' => (bool) config('edge.mqtt.tls.allow_self_signed', false),
            ],
        ];

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

        // ★ Auto-scaler 配置
        $this->autoScaleEnabled = ((int) $this->option('auto-scale')) === 1;
        $this->autoScaleIntervalSec = max(10, (int) $this->option('auto-scale-interval'));
        $this->sampleRateMin = (float) $this->option('sample-rate-min');
        $this->sampleRateMax = (float) $this->option('sample-rate-max');

        // ★ 心跳续期配置（命令行参数 > config > 默认 120）
        $this->deviceActiveTtl = max(10, (int) (
            $this->option('device-active-ttl')
                ?: config('edge.mqtt.device_active_ttl', 120)
        ));
        $this->heartbeatTopic = $this->option('heartbeat-topic') ?: 'heartbeat/response';

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

        $this->adaptiveBatch = new MultiDimAdaptiveBatch(
            min: $adaptMin,
            max: $adaptMax,
            alpha: $adaptAlpha,
        );

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
        ]);

        if (is_string($topics)) {
            $topics = array_filter(array_map('trim', explode(',', $topics)));
        }

        return array_values((array) $topics);
    }

    /* ==================== 路由 ==================== */

    private function routeToNormalChannel(array $item, string $device): void
    {
        if ($this->normalChannel->isFull()) {
            $this->warn('[CHANNEL] 普通队列已满，丢弃 device='.$device);
            $this->droppedCount++;
            $this->exporter->inc('edge_dead_letter_total');

            return;
        }

        try {
            $this->normalChannel->push($item);
            $this->receivedCount++;
            $this->normalBacklog++;
        } catch (Throwable $e) {
            $this->error('[CHANNEL] 普通队列 push 失败：'.$e->getMessage());
            $this->droppedCount++;
        }
    }

    private function routeToHighChannel(array $item, string $device): void
    {
        if ($this->highChannel->isFull()) {
            $this->warn('[CHANNEL] 高优队列已满，降级写死信 device='.$device);
            $this->exporter->inc('edge_dead_letter_total');
            $this->pushDeadLetter([$item], 'high_channel_full');

            return;
        }

        try {
            $this->highChannel->push($item);
            $this->receivedCount++;
            $this->highBacklog++;
        } catch (Throwable $e) {
            $this->error('[CHANNEL] 高优队列 push 失败：'.$e->getMessage());
            $this->exporter->inc('edge_dead_letter_total');
        }
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
            $this->deadChannel->push($item);
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

                try {
                    $this->mqtt?->disconnect();
                } catch (Throwable) {
                    // ignore
                }

                // 唤醒所有 Worker
                $this->wakeupChannel($this->normalChannel);
                $this->wakeupChannel($this->highChannel);
                $this->wakeupChannel($this->deadChannel);

                // 等待 Worker 池排空
                $this->drainWorkerPools();
            });
        }
    }

    private function wakeupChannel(Channel $channel): void
    {
        // 每个 Worker 唤醒一次
        $capacity = $channel->getCapacity();
        for ($i = 0; $i < $capacity; $i++) {
            try {
                $channel->push(null);
            } catch (Throwable) {
                break;
            }
        }
    }

    private function drainWorkerPools(): void
    {
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

            usleep(100_000); // 100ms
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

    /* ==================== 普通 Worker（支持缩容退出）==================== */

    private function runNormalWorker(): void
    {
        $batch = [];
        $capacity = max(1, $this->normalBatchSize);
        $flushIntervalSec = max(0.001, $this->normalFlushIntervalMs / 1000);
        $lastFlush = microtime(true);
        $workerId = Coroutine::getCurrent()->getId();

        $this->debugWorker('normal', $workerId, 'started');
        $this->activeNormalWorkers++;

        while (true) {
            if ($this->stopping && $this->normalChannel->isAvailable() === false) {
                break;
            }

            $timeout = $this->stopping ? 0 : max(0, (int) (
                ($flushIntervalSec - (microtime(true) - $lastFlush)) * 1000
            ));

            $item = null;
            try {
                if ($timeout > 0) {
                    /** @var array<string, mixed>|null $item */
                    $item = $this->normalChannel->pop($timeout);
                } else {
                    /** @var array<string, mixed>|null $item */
                    $item = $this->normalChannel->pop(0);
                }
            } catch (Throwable) {
                continue;
            }

            // ★ null = 缩容信号 或 停机信号
            if ($item === null) {
                if ($batch !== []) {
                    $this->flushNormalBatch($batch);
                    $batch = [];
                    $lastFlush = microtime(true);
                }

                // 缩容：如果当前 worker 数已超过目标，退出
                $currentAlive = $this->countAliveWorkers($this->normalWorkerCoroutines);
                if ($currentAlive > $this->targetNormalWorkers) {
                    break;
                }

                if ($this->stopping) {
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

        $this->activeNormalWorkers = max(0, $this->activeNormalWorkers - 1);
        $this->debugWorker('normal', $workerId, 'stopped');
    }

    /* ==================== 高优 Worker（支持缩容退出）==================== */

    private function runHighWorker(): void
    {
        $batch = [];
        $capacity = max(1, $this->highBatchSize);
        $flushIntervalSec = max(0.001, $this->highFlushIntervalMs / 1000);
        $lastFlush = microtime(true);
        $workerId = Coroutine::getCurrent()->getId();

        $this->debugWorker('high', $workerId, 'started');
        $this->activeHighWorkers++;

        while (true) {
            $timeout = $this->stopping ? 0 : max(0, (int) (
                ($flushIntervalSec - (microtime(true) - $lastFlush)) * 1000
            ));

            $item = null;
            try {
                if ($timeout > 0) {
                    /** @var array<string, mixed>|null $item */
                    $item = $this->highChannel->pop($timeout);
                } else {
                    /** @var array<string, mixed>|null $item */
                    $item = $this->highChannel->pop(0);
                }
            } catch (Throwable) {
                continue;
            }

            if ($item === null) {
                if ($batch !== []) {
                    $this->flushHighBatch($batch);
                    $batch = [];
                    $lastFlush = microtime(true);
                }

                // 缩容检查
                $currentAlive = $this->countAliveWorkers($this->highWorkerCoroutines);
                if ($currentAlive > $this->targetHighWorkers) {
                    break;
                }

                if ($this->stopping) {
                    break;
                }

                continue;
            }

            $batch[] = $item;

            if (count($batch) >= $capacity) {
                $this->flushHighBatch($batch);
                $batch = [];
                $lastFlush = microtime(true);
            }
        }

        $this->activeHighWorkers = max(0, $this->activeHighWorkers - 1);
        $this->debugWorker('high', $workerId, 'stopped');
    }

    /* ==================== 死信 Worker（不变）==================== */

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
            $timeout = $this->stopping ? 0 : max(0, (int) (
                ($flushIntervalSec - (microtime(true) - $lastFlush)) * 1000
            ));

            $item = null;
            try {
                if ($timeout > 0) {
                    /** @var array<string, mixed>|null $item */
                    $item = $this->deadChannel->pop($timeout);
                } else {
                    /** @var array<string, mixed>|null $item */
                    $item = $this->deadChannel->pop(0);
                }
            } catch (Throwable) {
                continue;
            }

            if ($item === null) {
                if ($batch !== []) {
                    $this->flushDeadLetterBatch($batch);
                    $batch = [];
                    $lastFlush = microtime(true);
                }

                if ($this->stopping) {
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

        $this->activeDeadWorkers = max(0, $this->activeDeadWorkers - 1);
        $this->debugWorker('dead', $workerId, 'stopped');
    }

    /**
     * 统计存活 worker 数
     */
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

    /* ==================== Redis 写入 ==================== */

    private function flushNormalBatch(array $batch): void
    {
        if ($batch === []) {
            return;
        }

        $flushStart = microtime(true);
        $now = microtime(true);
        $waitSum = 0.0;

        foreach ($batch as $item) {
            $enqueuedAt = $item['_enqueued_at'] ?? $now;
            $waitSum += ($now - $enqueuedAt) * 1000;
        }

        $count = count($batch);

        try {
            withRedis(function (\Redis $conn) use ($count, $batch): void {
                $conn->multi(\Redis::PIPELINE);

                foreach ($batch as $item) {
                    $conn->xAdd(
                        self::STREAM_DATA,
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

                if (is_array($results) && count($results) !== $count) {
                    fwrite(STDERR, sprintf(
                        "[FLUSH-DIAG] normal pipeline 结果数异常 expect=%d actual=%d\n",
                        $count,
                        count($results),
                    ));
                }
            });

            $this->normalFlushedCount += $count;
            $this->exporter->inc('edge_data_write_success_total', $count);
            $this->normalBacklog = max(0, $this->normalBacklog - $count);

            $elapsedMs = (microtime(true) - $flushStart) * 1000;
            $this->lastNormalWaitMs = $waitSum / $count;
            $this->normalFlushLatencySum += $elapsedMs;
            $this->normalFlushTimes++;

            // 记录 flush 耗时分布（直方图）
            $this->exporter->observe('edge_flush_duration_ms_normal', $elapsedMs);

            $this->line(sprintf(
                '[FLUSH] batch=%d → %s (%.2fms)',
                $count,
                self::STREAM_DATA,
                $elapsedMs,
            ));
        } catch (Throwable $e) {
            $this->error(sprintf(
                '[FLUSH] 普通写入失败（%d 条）→ 死信：%s',
                $count,
                $e->getMessage(),
            ));

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
        $now = microtime(true);
        $waitSum = 0.0;

        foreach ($batch as $item) {
            $enqueuedAt = $item['_enqueued_at'] ?? $now;
            $waitSum += ($now - $enqueuedAt) * 1000;
        }

        $count = count($batch);

        try {
            withRedis(function (\Redis $conn) use ($count, $batch): void {
                $conn->multi(\Redis::PIPELINE);

                foreach ($batch as $item) {
                    $conn->xAdd(
                        self::STREAM_HIGH,
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

                if (is_array($results) && count($results) !== $count) {
                    fwrite(STDERR, sprintf(
                        "[FLUSH-DIAG] high pipeline 结果数异常 expect=%d actual=%d\n",
                        $count,
                        count($results),
                    ));
                }
            });

            $this->highFlushedCount += $count;
            $this->exporter->inc('edge_high_write_success_total', $count);
            $this->highBacklog = max(0, $this->highBacklog - $count);

            $elapsedMs = (microtime(true) - $flushStart) * 1000;
            $this->lastHighWaitMs = $waitSum / $count;
            $this->highFlushLatencySum += $elapsedMs;
            $this->highFlushTimes++;

            // 记录 flush 耗时分布（直方图）
            $this->exporter->observe('edge_flush_duration_ms_high', $elapsedMs);

            $this->line(sprintf(
                '[FLUSH:HIGH] batch=%d → %s (%.2fms)',
                $count,
                self::STREAM_HIGH,
                $elapsedMs,
            ));
        } catch (Throwable $e) {
            $this->error(sprintf(
                '[FLUSH:HIGH] 高优写入失败（%d 条）→ 死信：%s',
                $count,
                $e->getMessage(),
            ));

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
        $now = microtime(true);
        $waitSum = 0.0;

        foreach ($batch as $item) {
            $enqueuedAt = $item['_enqueued_at'] ?? $now;
            $waitSum += ($now - $enqueuedAt) * 1000;
        }

        $count = count($batch);

        try {
            withRedis(function (\Redis $conn) use ($batch): void {
                $conn->multi(\Redis::PIPELINE);

                foreach ($batch as $item) {
                    $conn->xAdd(
                        self::STREAM_DEAD,
                        '*',
                        [
                            'batch' => json_encode(
                                $item['batch'] ?? $item,
                                JSON_UNESCAPED_SLASHES | JSON_UNESCAPED_UNICODE,
                            ),
                            'error' => $item['error'] ?? '',
                            'ts' => sprintf('%.3f', $item['ts'] ?? microtime(true)),
                        ],
                        self::DEAD_MAX_LEN,
                        true,
                    );
                }

                $conn->exec();
            });

            $this->deadFlushedCount += $count;
            $this->exporter->inc('edge_dead_letter_total', $count);
            $this->deadBacklog = max(0, $this->deadBacklog - $count);

            $elapsedMs = (microtime(true) - $flushStart) * 1000;
            $this->lastDeadWaitMs = $waitSum / $count;
            $this->deadFlushLatencySum += $elapsedMs;
            $this->deadFlushTimes++;

            // 记录 flush 耗时分布（直方图）
            $this->exporter->observe('edge_flush_duration_ms_dead', $elapsedMs);

            $this->warn(sprintf(
                '[DEAD] batch=%d → %s (%.2fms)',
                $count,
                self::STREAM_DEAD,
                $elapsedMs,
            ));
        } catch (Throwable $e) {
            $this->error(sprintf(
                '[DEAD] 死信写入失败（%d 条）：%s',
                $count,
                $e->getMessage(),
            ));
        }
    }

    /* ==================== Prometheus 指标（追加 auto-scale 指标）==================== */

    private function collectWorkerMetrics(): void
    {
        $this->exporter->set('edge_active_workers_normal', $this->activeNormalWorkers);
        $this->exporter->set('edge_active_workers_high', $this->activeHighWorkers);
        $this->exporter->set('edge_active_workers_dead', $this->activeDeadWorkers);

        $this->exporter->set('edge_channel_backlog_normal', $this->normalBacklog);
        $this->exporter->set('edge_channel_backlog_high', $this->highBacklog);
        $this->exporter->set('edge_channel_backlog_dead', $this->deadBacklog);

        $this->exporter->set('edge_message_wait_ms_normal', $this->lastNormalWaitMs);
        $this->exporter->set('edge_message_wait_ms_high', $this->lastHighWaitMs);
        $this->exporter->set('edge_message_wait_ms_dead', $this->lastDeadWaitMs);

        $this->exporter->set(
            'edge_flush_latency_ms_normal',
            $this->normalFlushTimes > 0
                ? $this->normalFlushLatencySum / $this->normalFlushTimes
                : 0.0,
        );
        $this->exporter->set(
            'edge_flush_latency_ms_high',
            $this->highFlushTimes > 0
                ? $this->highFlushLatencySum / $this->highFlushTimes
                : 0.0,
        );
        $this->exporter->set(
            'edge_flush_latency_ms_dead',
            $this->deadFlushTimes > 0
                ? $this->deadFlushLatencySum / $this->deadFlushTimes
                : 0.0,
        );

        $this->exporter->set('edge_flushed_total_normal', $this->normalFlushedCount);
        $this->exporter->set('edge_flushed_total_high', $this->highFlushedCount);
        $this->exporter->set('edge_flushed_total_dead', $this->deadFlushedCount);

        $this->exporter->set('edge_queue_backlog', $this->getBacklog());
        $this->exporter->set('edge_qps', $this->calculateQps());
        $this->exporter->set('edge_mqtt_connected', $this->mqtt !== null && ! $this->stopping ? 1 : 0);

        // ★ Auto-scale 指标
        $this->exporter->set('edge_active_clients', $this->activeClientCount);
        $this->exporter->set('edge_target_workers_normal', $this->targetNormalWorkers);
        $this->exporter->set('edge_target_workers_high', $this->targetHighWorkers);
        $this->exporter->set('edge_sample_rate', $this->sampleRate);
        $this->exporter->set('edge_current_batch_size', $this->normalBatchSize);
        $this->exporter->set('edge_current_flush_interval_ms', $this->normalFlushIntervalMs);

        // ★ 心跳续期指标
        $this->exporter->set('edge_heartbeat_renew_total', 0); // counter 由 inc() 维护

        // 直方图占位
        $this->exporter->set('edge_flush_duration_ms_normal', 0);
        $this->exporter->set('edge_flush_duration_ms_high', 0);
        $this->exporter->set('edge_flush_duration_ms_dead', 0);
    }

    /* ==================== Redis 连接池 ==================== */

    private function initRedisPool(): void
    {
        if (! function_exists('redisPoolInit')) {
            throw new \RuntimeException('helper.php 未加载');
        }

        if (! defined('REDIS_HOST') || ! defined('REDIS_PORT') || ! defined('REDIS_DB')) {
            throw new \RuntimeException('bootstrap.php 未加载或 Redis 常量未定义');
        }

        redisPoolInit(
            $this->configInt('edge.redis.pool_size', defined('REDIS_POOL_SIZE') ? REDIS_POOL_SIZE : 64),
            $this->configInt('edge.redis.db', defined('REDIS_DB') ? REDIS_DB : 2),
        );

        $this->info(sprintf(
            '[redis] 连接池已初始化 %s:%s db=%s',
            REDIS_HOST,
            REDIS_PORT,
            REDIS_DB,
        ));
    }

    /* ==================== 统计 ==================== */

    private function getBacklog(): int
    {
        return $this->normalBacklog + $this->highBacklog + $this->deadBacklog;
    }

    private function calculateQps(): float
    {
        $now = microtime(true);
        $current = $this->normalFlushedCount + $this->highFlushedCount + $this->deadFlushedCount;

        $this->qpsWindow[$now] = $current;

        $cutoff = $now - 5.0;
        foreach ($this->qpsWindow as $ts => $_) {
            if ($ts < $cutoff) {
                unset($this->qpsWindow[$ts]);
            }
        }

        $values = array_values($this->qpsWindow);
        if (count($values) < 2) {
            return 0.0;
        }

        $elapsed = array_key_last($this->qpsWindow) - array_key_first($this->qpsWindow);
        if ($elapsed <= 0) {
            return 0.0;
        }

        return max(0.0, ($values[count($values) - 1] - $values[0]) / $elapsed);
    }

    private function printStats(): void
    {
        $elapsed = max(0.001, microtime(true) - $this->startTime);
        $totalFlushed = $this->normalFlushedCount + $this->highFlushedCount + $this->deadFlushedCount;
        $qps = $totalFlushed / $elapsed;
        $avgNormalMs = $this->normalFlushTimes > 0
            ? $this->normalFlushLatencySum / $this->normalFlushTimes
            : 0.0;
        $avgHighMs = $this->highFlushTimes > 0
            ? $this->highFlushLatencySum / $this->highFlushTimes
            : 0.0;
        $avgDeadMs = $this->deadFlushTimes > 0
            ? $this->deadFlushLatencySum / $this->deadFlushTimes
            : 0.0;

        $this->info('========== 运行统计 ==========');
        $this->info(sprintf('运行时长：%.1fs', $elapsed));
        $this->info(sprintf('接收消息：%d', $this->receivedCount));
        $this->info(sprintf('普通写入：%d', $this->normalFlushedCount));
        $this->info(sprintf('高优写入：%d', $this->highFlushedCount));
        $this->info(sprintf('死信写入：%d', $this->deadFlushedCount));
        $this->info(sprintf('丢弃消息：%d（采样+超限）', $this->droppedCount));
        $this->info(sprintf('活跃客户端：%d', $this->activeClientCount));
        $this->info(sprintf('采样率：%.2f', $this->sampleRate));
        $this->info(sprintf('总写入 QPS：%.1f msg/s', $qps));
        $this->info(sprintf('普通平均刷盘：%.2fms', $avgNormalMs));
        $this->info(sprintf('高优平均刷盘：%.2fms', $avgHighMs));
        $this->info(sprintf('死信平均刷盘：%.2fms', $avgDeadMs));
        $this->info(sprintf(
            '积压：normal=%d high=%d dead=%d',
            $this->normalBacklog,
            $this->highBacklog,
            $this->deadBacklog,
        ));
        $this->info(sprintf(
            '活跃 Worker：normal=%d high=%d dead=%d (target: n=%d h=%d)',
            $this->activeNormalWorkers,
            $this->activeHighWorkers,
            $this->activeDeadWorkers,
            $this->targetNormalWorkers,
            $this->targetHighWorkers,
        ));
        $this->info('==============================');
    }

    /* ==================== 工具 ==================== */

    private function configInt(string $key, int $default): int
    {
        try {
            /** @var int|string|null $value */
            $value = config($key, $default);
            if ($value === null) {
                return $default;
            }

            return (int) $value;
        } catch (Throwable) {
            return $default;
        }
    }

    private function debugWorker(string $priority, int $workerId, string $event): void
    {
        fwrite(STDERR, sprintf(
            "[WORKER:%s] #%d %s\n",
            $priority,
            $workerId,
            $event,
        ));
    }

    /**
     * @return array{0: string, 1: string}
     */
    public function getServerIp(): array
    {
        $scheme = config('edge.metrics.scheme', 'http');

        $configured = config('edge.metrics.public_host');
        if ($configured && filter_var($configured, FILTER_VALIDATE_IP)) {
            return [$scheme, $configured];
        }

        // Windows：用 ipconfig 取 IPv4
        if (PHP_OS_FAMILY === 'Windows') {
            $lines = [];
            @exec('ipconfig', $lines);
            foreach ($lines as $line) {
                if (preg_match('/IPv4 Address[.\s]*:\s*([\d.]+)/', $line, $m)) {
                    $ip = $m[1];
                    if (! str_starts_with($ip, '127.')) {
                        return [$scheme, $ip];
                    }
                }
            }

            return [$scheme, '127.0.0.1'];
        }

        // Linux：外网 IP
        foreach (['https://ifconfig.me', 'http://ipinfo.io/ip', 'http://icanhazip.com'] as $url) {
            $ctx = stream_context_create(['http' => ['timeout' => 2]]);
            $body = @file_get_contents($url, false, $ctx);
            if ($body) {
                $ip = trim($body);
                if (filter_var($ip, FILTER_VALIDATE_IP, FILTER_FLAG_IPV4)
                    && ! preg_match('/^(10\.|127\.|192\.168\.|172\.(1[6-9]|2\d|3[01])\.|100\.64\.)/', $ip)) {
                    return [$scheme, $ip];
                }
            }
        }

        $lines = [];
        @exec('ip -4 addr show 2>/dev/null', $lines);
        foreach ($lines as $line) {
            if (preg_match('/^\s+inet\s+([\d.]+)/', $line, $m) && ! str_starts_with($m[1], '127.')) {
                return [$scheme, $m[1]];
            }
        }

        return [$scheme, '127.0.0.1'];
    }

    private function nowString(): string
    {
        return date('Y-m-d H:i:s');
    }
}
