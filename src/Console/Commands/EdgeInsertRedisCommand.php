<?php
declare(strict_types=1);

namespace DagaSmart\Edge\Console\Commands;

use DagaSmart\Edge\Services\MqttService;
use Illuminate\Console\Command;
use Throwable;
use Swow\Coroutine;
use Swow\Channel;
use Swow\Signal;

$edgeRoot = dirname(__DIR__, 3);
require_once $edgeRoot . '/bin/bootstrap.php';
require_once $edgeRoot . '/bin/helper.php';

class EdgeInsertRedisCommand extends Command
{
    protected $signature = 'edge:insert-redis';
    protected $description = '订阅 NanoMQ heartbeat/response，批量写入 Redis Stream（MqttService）';

    /* ==================== Stream ==================== */
    private const string STREAM = 'stream:device:data';
    private const string DEAD_STREAM = 'stream:device:dead';
    private const int STREAM_MAX_LEN = 50_000;
    private const int DEAD_MAX_LEN = 10_000;

    /* ==================== 批量参数（可通过配置覆盖） ==================== */
    private const int DEFAULT_BATCH_SIZE = 50;
    private const int DEFAULT_FLUSH_INTERVAL_MS = 100;
    private const int DEFAULT_CHANNEL_SIZE = 2048;
    private const int DEFAULT_MAX_PAYLOAD_LEN = 65_536; // 64KB

    /* ==================== 停机等待 ==================== */
    private const int DRAIN_TIMEOUT_SEC = 10;
    private const int DRAIN_CHECK_INTERVAL_MS = 50;

    private Channel $channel;
    private bool $stopping = false;

    /* ==================== 统计 ==================== */
    private int $receivedCount = 0;
    private int $flushedCount = 0;
    private int $droppedCount = 0;
    private int $deadCount = 0;
    private float $flushLatencySum = 0.0;
    private int $flushTimes = 0;
    private float $startTime;

    public function handle(): int
    {
        $this->startTime = microtime(true);
        $this->info('[edge:insert-redis] 启动，PID=' . getmypid());

        $batchSize = $this->configInt('edge.mqtt.batch_size', self::DEFAULT_BATCH_SIZE);
        $flushIntervalMs = $this->configInt('edge.mqtt.flush_interval_ms', self::DEFAULT_FLUSH_INTERVAL_MS);
        $channelSize = $this->configInt('edge.mqtt.channel_size', self::DEFAULT_CHANNEL_SIZE);
        $maxPayloadLen = $this->configInt('edge.mqtt.max_payload_len', self::DEFAULT_MAX_PAYLOAD_LEN);

        $this->channel = new Channel($channelSize);

        $this->registerSignalHandlers();
        $this->initRedisPool();
        $this->startFlusher($batchSize, $flushIntervalMs);

        try {
            $this->info('=====================================');
            $this->info(sprintf(
                'MQTT 订阅已启动 | %s',
                $this->nowString(),
            ));
            $this->info('订阅主题：heartbeat/response');
            $this->info(sprintf('批量大小：%d，刷盘间隔：%dms', $batchSize, $flushIntervalMs));
            $this->info(sprintf('Channel 容量：%d，最大 payload：%dKB', $channelSize, (int)($maxPayloadLen / 1024)));
            $this->info('按 Ctrl+C 停止');
            $this->info('=====================================');

            $mqtt = new MqttService;
            $mqtt->subscribe('heartbeat/response', function (
                string $topic,
                string $message
            ) use ($maxPayloadLen) {
                if ($this->stopping) {
                    return;
                }

                $payloadLen = strlen($message);
                if ($payloadLen > $maxPayloadLen) {
                    $this->warn(sprintf(
                        '[DROP] payload 超过限制 %d > %d，丢弃',
                        $payloadLen,
                        $maxPayloadLen,
                    ));
                    $this->droppedCount++;
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
                ];

                if ($this->channel->isFull()) {
                    $this->warn('[CHANNEL] 队列已满，丢弃消息 device=' . $device);
                    $this->droppedCount++;
                    return;
                }

                try {
                    $this->channel->push($item);
                    $this->receivedCount++;
                } catch (Throwable $e) {
                    $this->error('[CHANNEL] push 失败：' . $e->getMessage());
                    $this->droppedCount++;
                }
            });

            $this->info('[mqtt] 订阅协程已退出');

        } catch (Throwable $e) {
            $this->error('[FATAL] ' . $e->getMessage());
            return 1;
        }

        $this->drainChannel();
        $this->printStats();

        $this->info('[edge:insert-redis] 已停止');
        return 0;
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
                    return;
                }
                $this->warn(sprintf('[signal] 收到 %d，准备优雅停机', $sig));
                $this->stopping = true;
                // ✅ 推 null 唤醒 flusher，触发剩余数据刷盘
                try {
                    $this->channel->push(null);
                } catch (Throwable) {
                    // channel 已关闭，忽略
                }
            });
        }
    }

    /* ==================== Flusher ==================== */

    private function startFlusher(int $batchSize, int $flushIntervalMs): void
    {
        $flushIntervalSec = max(0.001, $flushIntervalMs / 1000.0);

        Coroutine::run(function () use ($batchSize, $flushIntervalSec): void {
            $batch = [];
            $capacity = max(1, $batchSize);
            $lastFlush = microtime(true);

            while (true) {
                $now = microtime(true);
                $timeout = max(0.0, $flushIntervalSec - ($now - $lastFlush));

                $item = null;
                if ($timeout > 0.0) {
                    try {
                        /** @var array<string, mixed>|null $item */
                        $item = @$this->channel->pop($timeout);
                    } catch (Throwable) {
                        // 超时或 channel 关闭，走兜底刷盘
                    }
                } else {
                    try {
                        /** @var array<string, mixed>|null $item */
                        $item = $this->channel->pop();
                    } catch (Throwable) {
                        // ignore
                    }
                }

                // null 表示停机信号或 channel 关闭
                if ($item === null) {
                    if ($batch !== []) {
                        $this->flushBatch($batch);
                        $batch = [];
                    }
                    // 如果已停机，排空后退出
                    if ($this->stopping) {
                        break;
                    }
                    continue;
                }

                $batch[] = $item;
                if (count($batch) >= $capacity) {
                    $this->flushBatch($batch);
                    $batch = [];
                    $lastFlush = microtime(true);
                }
            }
        });
    }

    /* ==================== Redis ==================== */

    private function initRedisPool(): void
    {
        if (!function_exists('redisPoolInit')) {
            throw new \RuntimeException('helper.php 未加载');
        }
        if (!defined('REDIS_HOST') || !defined('REDIS_PORT') || !defined('REDIS_DB')) {
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

    private function flushBatch(array $batch): void
    {
        if ($batch === []) {
            return;
        }

        $flushStart = microtime(true);
        $success = false;

        try {
            withRedis(function (\Redis $conn) use ($batch): void {
                $conn->multi(\Redis::PIPELINE);
                foreach ($batch as $item) {
                    $conn->xAdd(
                        self::STREAM,
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

                // 诊断：pipeline 返回结果数量应与批量一致
                if (is_array($results) && count($results) !== count($batch)) {
                    fwrite(STDERR, sprintf(
                        "[FLUSH-DIAG] pipeline 结果数异常 expect=%d actual=%d\n",
                        count($batch),
                        count($results),
                    ));
                }
            });

            $this->flushedCount += count($batch);
            $success = true;

            $this->line(sprintf(
                '[FLUSH] batch=%d → %s',
                count($batch),
                self::STREAM,
            ));
        } catch (Throwable $e) {
            $this->error(sprintf(
                '[FLUSH] Redis 写入失败（%d 条）→ 死信：%s',
                count($batch),
                $e->getMessage(),
            ));
            $this->writeDeadLetter($batch, $e);
        }

        if ($success) {
            $elapsedMs = (microtime(true) - $flushStart) * 1000;
            $this->flushLatencySum += $elapsedMs;
            $this->flushTimes++;
        }
    }

    private function writeDeadLetter(array $batch, Throwable $cause): void
    {
        try {
            withRedis(function (\Redis $conn) use ($batch, $cause): void {
                $conn->xAdd(
                    self::DEAD_STREAM,
                    '*',
                    [
                        'batch' => json_encode($batch, JSON_UNESCAPED_SLASHES | JSON_UNESCAPED_UNICODE),
                        'error' => $cause->getMessage(),
                        'ts' => sprintf('%.3f', microtime(true)),
                    ],
                    self::DEAD_MAX_LEN,
                    true,
                );
            });

            $this->deadCount += count($batch);
            $this->warn('[DEAD] 已写入死信 ' . self::DEAD_STREAM);
        } catch (Throwable $e) {
            fwrite(STDERR, '[FATAL] 死信写入也失败：' . $e->getMessage() . PHP_EOL);
        }
    }

    /* ==================== 停机排空 ==================== */

    private function drainChannel(): void
    {
        $this->info('[drain] 开始排空 Channel...');
        $drained = 0;

        // 先把 Channel 里残留的消息全推给 flusher（flusher 已退出，这里自己刷）
        while ($this->channel->getLength() > 0) {
            try {
                /** @var array<string, mixed>|null $item */
                $item = $this->channel->pop(0.1);
            } catch (Throwable) {
                break;
            }
            if ($item === null) {
                break;
            }
            // 直接刷单条，不再走 Channel
            $this->flushBatch([$item]);
            $drained++;
        }

        $this->info(sprintf('[drain] 排空完成，补刷 %d 条', $drained));
    }

    /* ==================== 统计 ==================== */

    private function printStats(): void
    {
        $elapsed = max(0.001, microtime(true) - $this->startTime);
        $qps = $this->flushedCount / $elapsed;
        $avgFlushMs = $this->flushTimes > 0
            ? $this->flushLatencySum / $this->flushTimes
            : 0.0;

        $this->info('========== 运行统计 ==========');
        $this->info(sprintf('运行时长：%.1fs', $elapsed));
        $this->info(sprintf('接收消息：%d', $this->receivedCount));
        $this->info(sprintf('写入成功：%d', $this->flushedCount));
        $this->info(sprintf('丢弃消息：%d', $this->droppedCount));
        $this->info(sprintf('死信消息：%d', $this->deadCount));
        $this->info(sprintf('写入 QPS：%.1f msg/s', $qps));
        $this->info(sprintf('平均刷盘：%.2fms', $avgFlushMs));
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

    private function nowString(): string
    {
        return date('Y-m-d H:i:s');
    }
}
