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
 * 订阅 NanoMQ 多主题，按优先级路由并批量写入 Redis Stream（Worker Pool 版）
 *
 * 架构：
 *   MQTT 接收协程
 *     ├─ 高优 topic → highChannel → HighWorkerPool → stream:device:high
 *     ├─ 普通 topic → normalChannel → NormalWorkerPool → stream:device:data
 *     └─ 写入失败 → deadChannel → DeadLetterWorkerPool → stream:device:dead
 */
class EdgeInsertRedisDimCommand extends Command
{
    protected $signature = 'edge:insert-redis-dim';

    protected $description = '订阅 NanoMQ 多主题，按优先级路由并批量写入 Redis Stream';

    /* ==================== Stream ==================== */
    private const string STREAM_HIGH = 'stream:device:high';

    private const string STREAM_DATA = 'stream:device:data';

    private const string STREAM_DEAD = 'stream:device:dead';

    private const int STREAM_MAX_LEN = 50_000;

    private const int DEAD_MAX_LEN = 10_000;

    /* ==================== 停机排空 ==================== */
    private const int DRAIN_POP_TIMEOUT_MS = 50;

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
        $this->info(sprintf('[metrics] Prometheus 本地 %s://%s:%d', $scheme, $metricsHost, $metricsPort));
        echo "[metrics] Prometheus 预览 {$scheme}://{$serverIp}:{$metricsPort}/metrics\n";
        echo "[metrics] Prometheus 接口 {$scheme}://{$serverIp}:{$metricsPort}/api/metrics\n";

        // 启动 Worker 池
        $this->startNormalWorkerPool();
        $this->startHighWorkerPool();
        $this->startDeadLetterWorkerPool();

        $topics = $this->resolveTopics();
        $this->mqtt = new NanoMQ;

        try {
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

            $this->info('[mqtt] 订阅协程已退出');
        } catch (Throwable $e) {
            $this->error('[FATAL] '.$e->getMessage());

            return 1;
        }

        $this->printStats();
        $this->exporter->stop();
        $this->info('[edge:insert-redis] 已停止');

        return 0;
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

    /* ==================== 普通 Worker ==================== */

    private function runNormalWorker(): void
    {
        $batch = [];
        $capacity = max(1, $this->normalBatchSize);
        $flushIntervalSec = max(0.001, $this->normalFlushIntervalMs / 1000);
        $lastFlush = microtime(true);
        $workerId = Coroutine::getCurrent()->getId();

        $this->debugWorker('normal', $workerId, 'started');

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

            if ($item === null) {
                if ($batch !== []) {
                    $this->flushNormalBatch($batch);
                    $batch = [];
                    $lastFlush = microtime(true);
                }

                if ($this->stopping) {
                    break;
                }
                continue;
            }

            $batch[] = $item;
            $this->activeNormalWorkers++;

            if (count($batch) >= $capacity) {
                $this->flushNormalBatch($batch);
                $batch = [];
                $lastFlush = microtime(true);
            }
        }

        $this->activeNormalWorkers = max(0, $this->activeNormalWorkers - 1);
        $this->debugWorker('normal', $workerId, 'stopped');
    }

    /* ==================== 高优 Worker ==================== */

    private function runHighWorker(): void
    {
        $batch = [];
        $capacity = max(1, $this->highBatchSize);
        $flushIntervalSec = max(0.001, $this->highFlushIntervalMs / 1000);
        $lastFlush = microtime(true);
        $workerId = Coroutine::getCurrent()->getId();

        $this->debugWorker('high', $workerId, 'started');

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

                if ($this->stopping) {
                    break;
                }
                continue;
            }

            $batch[] = $item;
            $this->activeHighWorkers++;

            if (count($batch) >= $capacity) {
                $this->flushHighBatch($batch);
                $batch = [];
                $lastFlush = microtime(true);
            }
        }

        $this->activeHighWorkers = max(0, $this->activeHighWorkers - 1);
        $this->debugWorker('high', $workerId, 'stopped');
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
            $this->activeDeadWorkers++;

            if (count($batch) >= $capacity) {
                $this->flushDeadLetterBatch($batch);
                $batch = [];
                $lastFlush = microtime(true);
            }
        }

        $this->activeDeadWorkers = max(0, $this->activeDeadWorkers - 1);
        $this->debugWorker('dead', $workerId, 'stopped');
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
        } finally {
            $this->activeNormalWorkers = max(0, $this->activeNormalWorkers - $count);
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
        } finally {
            $this->activeHighWorkers = max(0, $this->activeHighWorkers - $count);
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
        } finally {
            $this->activeDeadWorkers = max(0, $this->activeDeadWorkers - $count);
        }
    }

    /* ==================== Prometheus 指标 ==================== */

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
        $this->info(sprintf('丢弃消息：%d', $this->droppedCount));
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
            '活跃 Worker：normal=%d high=%d dead=%d',
            $this->activeNormalWorkers,
            $this->activeHighWorkers,
            $this->activeDeadWorkers,
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
        if (array_any($lines, fn ($line) => preg_match('/^\s+inet\s+([\d.]+)/', $line, $m) && ! str_starts_with($m[1], '127.'))) {
            return [$scheme, $m[1]];
        }

        return [$scheme, '0.0.0.0'];
    }

    private function nowString(): string
    {
        return date('Y-m-d H:i:s');
    }
}
