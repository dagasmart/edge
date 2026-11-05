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
 * 订阅 NanoMQ 多主题，按优先级路由并批量写入 Redis Stream
 *
 * - 高优主题 → 高优 Channel → stream:device:high
 * - 普通主题 → 普通 Channel → stream:device:data
 * - 失败统一 → stream:device:dead
 * - Prometheus 指标暴露在独立端口
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

    /* ==================== 普通 Channel（数据） ==================== */
    private Channel $normalChannel;

    private int $normalBatchSize;

    private int $normalFlushIntervalMs;

    /* ==================== 高优 Channel ==================== */
    private Channel $highChannel;

    private int $highBatchSize;

    private int $highFlushIntervalMs;

    /* ==================== 公共 ==================== */
    private int $maxPayloadLen;

    private bool $stopping = false;

    /** @var NanoMQ|null 复用同一个 MQTT 实例，避免信号里新建连接 */
    private ?NanoMQ $mqtt = null;

    /* ==================== 统计 ==================== */
    private int $receivedCount = 0;

    private int $normalFlushedCount = 0;

    private int $highFlushedCount = 0;

    private int $droppedCount = 0;

    private int $deadCount = 0;

    private float $normalFlushLatencySum = 0.0;

    private int $normalFlushTimes = 0;

    private float $highFlushLatencySum = 0.0;

    private int $highFlushTimes = 0;

    private float $startTime;

    /** 等待时间采样（毫秒）—— 消息入队到写出的真实队列等待时间 */
    private float $lastNormalWaitMs = 0.0;

    private float $lastHighWaitMs = 0.0;

    /** 自维护积压（Swow Channel 无 getLength） */
    private int $normalBacklog = 0;

    private int $highBacklog = 0;

    /** QPS 滑动窗口（最近 5 秒） */
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

        // 启动 Prometheus 指标服务
        $metricsPort = $this->configInt('edge.metrics.port', 9090);
        $metricsHost = config('edge.metrics.host', '0.0.0.0');

        $this->exporter->startMetricsServer($metricsPort, $metricsHost, function (): array {
            // 动态刷新实时指标
            $this->exporter->set('edge_queue_backlog', $this->getBacklog());
            $this->exporter->set('edge_message_wait_ms_normal', $this->lastNormalWaitMs);
            $this->exporter->set('edge_message_wait_ms_high', $this->lastHighWaitMs);
            $this->exporter->set('edge_qps', $this->calculateQps());
            $this->exporter->set('edge_mqtt_connected', $this->mqtt !== null && ! $this->stopping ? 1 : 0);

            return $this->exporter->getMetrics();
        });

        [$scheme, $serverIp] = $this->getServerIp();

        $this->info(sprintf('[metrics] Prometheus 监听 %s:%d', $metricsHost, $metricsPort));
        $this->info(sprintf('[metrics] Prometheus 本地 %s://%s:%d', $scheme, $metricsHost, $metricsPort));
        echo "[metrics] Prometheus 预览 {$scheme}://{$serverIp}:{$metricsPort}/metrics\n";
        echo "[metrics] Prometheus 接口 {$scheme}://{$serverIp}:{$metricsPort}/api/metrics\n";

        // 启动两个 Flusher 协程
        $this->startNormalFlusher();
        $this->startHighFlusher();

        $topics = $this->resolveTopics();

        $this->mqtt = new NanoMQ;

        try {
            $this->info('=====================================');
            $this->info(sprintf('MQTT 订阅已启动 | %s', $this->nowString()));
            $this->info('订阅主题：'.implode(', ', $topics));
            $this->info(sprintf(
                '普通：batch=%d interval=%dms channel=%d',
                $this->normalBatchSize,
                $this->normalFlushIntervalMs,
                $this->normalChannel->getCapacity(),
            ));
            $this->info(sprintf(
                '高优：batch=%d interval=%dms channel=%d',
                $this->highBatchSize,
                $this->highFlushIntervalMs,
                $this->highChannel->getCapacity(),
            ));
            $this->info('按 Ctrl+C 停止');
            $this->info('=====================================');

            // NanoMQ::subscribe() 只接受单个 string topic，循环订阅复用同一个回调
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

            // 阻塞等待 MQTT 协程退出，无轮询、无 sleep
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
        $this->normalBatchSize = $this->configInt(
            'edge.mqtt.batch_size',
            200,
        );
        $this->normalFlushIntervalMs = $this->configInt(
            'edge.mqtt.flush_interval_ms',
            100,
        );
        $normalChannelSize = $this->configInt(
            'edge.mqtt.channel_size',
            4096,
        );

        $this->highBatchSize = $this->configInt(
            'edge.mqtt.high_batch_size',
            10,
        );
        $this->highFlushIntervalMs = $this->configInt(
            'edge.mqtt.high_flush_interval_ms',
            20,
        );
        $highChannelSize = $this->configInt(
            'edge.mqtt.high_channel_size',
            512,
        );

        $this->maxPayloadLen = $this->configInt(
            'edge.mqtt.max_payload_len',
            65_536,
        );

        $this->normalChannel = new Channel($normalChannelSize);
        $this->highChannel = new Channel($highChannelSize);
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
            $this->writeDeadLetter([$item], 'high_channel_full');

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

                // 唤醒两个 flusher
                try {
                    $this->normalChannel->push(null);
                } catch (Throwable) {
                    // ignore
                }

                try {
                    $this->highChannel->push(null);
                } catch (Throwable) {
                    // ignore
                }
            });
        }
    }

    /* ==================== 普通 Flusher ==================== */

    private function startNormalFlusher(): void
    {
        $flushIntervalMs = max(1, $this->normalFlushIntervalMs);

        Coroutine::run(function () use ($flushIntervalMs): void {
            $batch = [];
            $capacity = max(1, $this->normalBatchSize);
            $lastFlush = microtime(true);

            while (true) {
                $now = microtime(true);
                $elapsedMs = (int) (($now - $lastFlush) * 1000);
                $timeoutMs = max(0, $flushIntervalMs - $elapsedMs);

                $item = null;

                try {
                    if ($timeoutMs > 0) {
                        /** @var array<string, mixed>|null $item */
                        $item = $this->normalChannel->pop($timeoutMs);
                    } else {
                        /** @var array<string, mixed>|null $item */
                        $item = $this->normalChannel->pop();
                    }
                } catch (Throwable) {
                    continue;
                }

                // null = 停机唤醒信号
                if ($item === null) {
                    if ($batch !== []) {
                        $this->flushNormalBatch($batch);
                        $batch = [];
                    }

                    if ($this->stopping) {
                        while (true) {
                            try {
                                /** @var array<string, mixed>|null $leftover */
                                $leftover = $this->normalChannel->pop(self::DRAIN_POP_TIMEOUT_MS);
                            } catch (Throwable) {
                                break 2;
                            }

                            if ($leftover === null) {
                                break;
                            }

                            $this->flushNormalBatch([$leftover]);
                        }
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

            $this->line('[flusher:normal] 已退出');
        });
    }

    /* ==================== 高优 Flusher ==================== */

    private function startHighFlusher(): void
    {
        $flushIntervalMs = max(1, $this->highFlushIntervalMs);

        Coroutine::run(function () use ($flushIntervalMs): void {
            $batch = [];
            $capacity = max(1, $this->highBatchSize);
            $lastFlush = microtime(true);

            while (true) {
                $now = microtime(true);
                $elapsedMs = (int) (($now - $lastFlush) * 1000);
                $timeoutMs = max(0, $flushIntervalMs - $elapsedMs);

                $item = null;

                try {
                    if ($timeoutMs > 0) {
                        /** @var array<string, mixed>|null $item */
                        $item = $this->highChannel->pop($timeoutMs);
                    } else {
                        /** @var array<string, mixed>|null $item */
                        $item = $this->highChannel->pop();
                    }
                } catch (Throwable) {
                    continue;
                }

                if ($item === null) {
                    if ($batch !== []) {
                        $this->flushHighBatch($batch);
                        $batch = [];
                    }

                    if ($this->stopping) {
                        while (true) {
                            try {
                                /** @var array<string, mixed>|null $leftover */
                                $leftover = $this->highChannel->pop(self::DRAIN_POP_TIMEOUT_MS);
                            } catch (Throwable) {
                                break 2;
                            }

                            if ($leftover === null) {
                                break;
                            }

                            $this->flushHighBatch([$leftover]);
                        }
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

            $this->line('[flusher:high] 已退出');
        });
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

        $success = false;

        try {
            withRedis(function (\Redis $conn) use ($batch): void {
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

                if (is_array($results) && count($results) !== count($batch)) {
                    fwrite(STDERR, sprintf(
                        "[FLUSH-DIAG] normal pipeline 结果数异常 expect=%d actual=%d\n",
                        count($batch),
                        count($results),
                    ));
                }
            });

            $this->normalFlushedCount += count($batch);
            $this->exporter->inc('edge_data_write_success_total', count($batch));
            $this->normalBacklog -= count($batch);
            $success = true;

            $this->line(sprintf(
                '[FLUSH] batch=%d → %s',
                count($batch),
                self::STREAM_DATA,
            ));
        } catch (Throwable $e) {
            $this->error(sprintf(
                '[FLUSH] 普通写入失败（%d 条）→ 死信：%s',
                count($batch),
                $e->getMessage(),
            ));

            $this->exporter->inc('edge_redis_write_failed_total', count($batch));
            $this->writeDeadLetter($batch, $e);
        }

        if ($success) {
            $elapsedMs = (microtime(true) - $flushStart) * 1000;
            $this->lastNormalWaitMs = $waitSum > 0 ? $waitSum / count($batch) : $elapsedMs / count($batch);
            $this->normalFlushLatencySum += $elapsedMs;
            $this->normalFlushTimes++;
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

        try {
            withRedis(function (\Redis $conn) use ($batch): void {
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

                if (is_array($results) && count($results) !== count($batch)) {
                    fwrite(STDERR, sprintf(
                        "[FLUSH-DIAG] high pipeline 结果数异常 expect=%d actual=%d\n",
                        count($batch),
                        count($results),
                    ));
                }
            });

            $this->highFlushedCount += count($batch);
            $this->exporter->inc('edge_high_write_success_total', count($batch));
            $this->highBacklog -= count($batch);

            $this->line(sprintf(
                '[FLUSH:HIGH] batch=%d → %s',
                count($batch),
                self::STREAM_HIGH,
            ));
        } catch (Throwable $e) {
            $this->error(sprintf(
                '[FLUSH:HIGH] 高优写入失败（%d 条）→ 死信：%s',
                count($batch),
                $e->getMessage(),
            ));

            $this->exporter->inc('edge_redis_write_failed_total', count($batch));
            $this->writeDeadLetter($batch, $e);
        }

        $elapsedMs = (microtime(true) - $flushStart) * 1000;
        $this->lastHighWaitMs = $waitSum > 0 ? $waitSum / max(1, count($batch)) : $elapsedMs / max(1, count($batch));
        $this->highFlushLatencySum += $elapsedMs;
        $this->highFlushTimes++;
    }

    private function writeDeadLetter(array $batch, Throwable|string $cause): void
    {
        $error = $cause instanceof Throwable ? $cause->getMessage() : $cause;

        try {
            withRedis(function (\Redis $conn) use ($batch, $error): void {
                $conn->xAdd(
                    self::STREAM_DEAD,
                    '*',
                    [
                        'batch' => json_encode($batch, JSON_UNESCAPED_SLASHES | JSON_UNESCAPED_UNICODE),
                        'error' => $error,
                        'ts' => sprintf('%.3f', microtime(true)),
                    ],
                    self::DEAD_MAX_LEN,
                    true,
                );
            });

            $this->deadCount += count($batch);
            $this->exporter->inc('edge_dead_letter_total', count($batch));
            // 写入死信也算"消费掉"，从积压中扣除（保守处理，统一从 normalBacklog 扣）
            $this->normalBacklog = max(0, $this->normalBacklog - count($batch));
            $this->warn('[DEAD] 已写入死信 '.self::STREAM_DEAD);
        } catch (Throwable $e) {
            fwrite(STDERR, '[FATAL] 死信写入也失败：'.$e->getMessage().PHP_EOL);
        }
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
        return $this->normalBacklog + $this->highBacklog;
    }

    private function calculateQps(): float
    {
        $now = microtime(true);
        $current = $this->normalFlushedCount + $this->highFlushedCount;

        $this->qpsWindow[$now] = $current;

        // 只保留最近 5 秒
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
        $totalFlushed = $this->normalFlushedCount + $this->highFlushedCount;
        $qps = $totalFlushed / $elapsed;
        $avgNormalMs = $this->normalFlushTimes > 0
            ? $this->normalFlushLatencySum / $this->normalFlushTimes
            : 0.0;
        $avgHighMs = $this->highFlushTimes > 0
            ? $this->highFlushLatencySum / $this->highFlushTimes
            : 0.0;

        $this->info('========== 运行统计 ==========');
        $this->info(sprintf('运行时长：%.1fs', $elapsed));
        $this->info(sprintf('接收消息：%d', $this->receivedCount));
        $this->info(sprintf('普通写入：%d', $this->normalFlushedCount));
        $this->info(sprintf('高优写入：%d', $this->highFlushedCount));
        $this->info(sprintf('丢弃消息：%d', $this->droppedCount));
        $this->info(sprintf('死信消息：%d', $this->deadCount));
        $this->info(sprintf('总写入 QPS：%.1f msg/s', $qps));
        $this->info(sprintf('普通平均刷盘：%.2fms', $avgNormalMs));
        $this->info(sprintf('高优平均刷盘：%.2fms', $avgHighMs));
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

    /**
     * 服务器IP
     *
     * @return array{0: string, 1: string} [scheme, ip]
     */
    public function getServerIp(): array
    {
        $scheme = config('edge.metrics.scheme');
        // CLI 下优先用 gethostbyname
        $hostname = gethostname();
        if ($hostname !== false) {
            $ip = gethostbyname($hostname);
            if (filter_var($ip, FILTER_VALIDATE_IP) && ! str_starts_with($ip, '127.')) {
                return [$scheme, $ip];
            }
        }

        return [$scheme, '127.0.0.1'];
    }

    private function nowString(): string
    {
        return date('Y-m-d H:i:s');
    }
}
