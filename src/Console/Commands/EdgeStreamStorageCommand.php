<?php

declare(strict_types=1);

namespace DagaSmart\Edge\Console\Commands;

use DagaSmart\Edge\Models\EdgeStream;
use DagaSmart\Edge\Models\EdgeStreamFail;
use DagaSmart\Edge\Support\RedisShardManager;
use Illuminate\Console\Command;
use Swow\Channel;
use Swow\Coroutine;
use Throwable;

/**
 * Redis Stream → PostgreSQL 持久化命令（分片版）
 *
 * 使用方式：
 *   php artisan edge:stream-storage --stream=stream:device:data:0 --group=edge_storage --consumer=worker_0_1
 *   php artisan edge:stream-storage --stream=stream:device:data:1 --group=edge_storage --consumer=worker_1_1
 *   php artisan edge:stream-storage --shard=0 --group=edge_storage --consumer=worker_0_1
 */
class EdgeStreamStorageCommand extends Command
{
    protected $signature = 'edge:stream-storage
        {--stream=stream:device:data:0 : Redis Stream key (如 stream:device:data:0)}
        {--shard=0 : 直接指定分片号，自动推导 stream key}
        {--group=edge_storage : 消费组名}
        {--consumer= : 消费者名，默认 auto_{pid}}
        {--count=50 : 每次 XREADGROUP 拉取条数}
        {--block=2000 : XREADGROUP 阻塞毫秒}
        {--batch=50 : 批量写入 PG 条数}
        {--swow : 强制 Swow 协程模式}
        {--sync : 强制同步模式}
        {--buffer-limit=20000 : 内存缓冲上限}';

    protected $description = 'Redis Stream → PostgreSQL 持久化（分片并行消费）';

    private const string STREAM_PREFIX = 'stream:device:data:';

    private const int BUFFER_FLUSH_INTERVAL_MS = 500;

    private string $stream;

    private int $shardNo;

    private string $groupName;

    private string $consumerName;

    private int $count;

    private int $blockMs;

    private int $batchSize;

    private bool $swowMode;

    private int $bufferLimit;

    private string $bufferKey;

    /** 优雅退出标志 */
    private bool $stopping = false;

    /** @var Channel|null Swow 模式下 Reader→Writer 通道 */
    private ?Channel $channel = null;

    /** @var array 内存缓冲（同步模式 / Swow Writer 消费前缓冲） */
    private array $buffer = [];

    /** @var array 已读取但未 ACK 的 ID 列表 */
    private array $pendingIds = [];

    /** @var int 上次强制刷库时间 */
    private int $lastFlushAt = 0;

    public function handle(): int
    {
        // ---- 参数解析 ----
        $streamOpt = $this->option('stream');
        $shardOpt = $this->option('shard');

        if ($streamOpt !== null && $streamOpt !== '') {
            $this->stream = $streamOpt;
            // 从 stream key 提取分片号
            if (preg_match('/stream:device:data:(\d+)/', $this->stream, $m)) {
                $this->shardNo = (int) $m[1];
            } else {
                $this->shardNo = 0;
            }
        } elseif ($shardOpt !== null && $shardOpt !== '') {
            $this->shardNo = (int) $shardOpt;
            $this->stream = self::STREAM_PREFIX.$this->shardNo;
        } else {
            $this->error('必须指定 --stream 或 --shard 参数');

            return self::FAILURE;
        }

        $this->groupName = $this->option('group') ?: 'edge_storage';
        $this->consumerName = $this->option('consumer') ?: ('auto_'.getmypid());
        $this->count = (int) $this->option('count') ?: 50;
        $this->blockMs = (int) $this->option('block') ?: 2000;
        $this->batchSize = (int) $this->option('batch') ?: 50;
        $this->bufferLimit = (int) $this->option('buffer-limit') ?: 20000;

        // Buffer Key 按分片隔离
        $this->bufferKey = 'edge:buffer:insert_redis:'.$this->shardNo;

        // 检测 Swow 扩展
        $hasSwow = extension_loaded('swow');
        $forceSwow = $this->option('swow');
        $forceSync = $this->option('sync');
        $this->swowMode = ($hasSwow && ! $forceSync) || $forceSwow;

        if ($forceSync) {
            $this->swowMode = false;
        }

        $this->info(sprintf(
            '[EdgeStream] shard=%d stream=%s group=%s consumer=%s mode=%s batch=%d buffer=%s',
            $this->shardNo,
            $this->stream,
            $this->groupName,
            $this->consumerName,
            $this->swowMode ? 'swow' : 'sync',
            $this->batchSize,
            $this->bufferKey
        ));

        // ---- 信号处理（优雅退出）----
        if (extension_loaded('pcntl')) {
            pcntl_async_signals(true);
            pcntl_signal(SIGINT, function (): void {
                if (! $this->stopping) {
                    $this->stopping = true;
                    $this->warn('[EdgeStream] 收到 SIGINT，准备优雅退出...');
                }
            });
            pcntl_signal(SIGTERM, function (): void {
                if (! $this->stopping) {
                    $this->stopping = true;
                    $this->warn('[EdgeStream] 收到 SIGTERM，准备优雅退出...');
                }
            });
        }

        // ---- 初始化 Redis 连接池 ----
        RedisShardManager::init();

        // ---- 确保消费组存在 ----
        $this->ensureConsumerGroup();

        // ---- 恢复 Pending 消息 ----
        $this->recoverPending();

        // ---- 主循环 ----
        if ($this->swowMode) {
            return $this->runSwowMode();
        }

        return $this->runSyncMode();
    }

    /**
     * 确保消费组存在
     */
    private function ensureConsumerGroup(): void
    {
        $redis = RedisShardManager::popByShard($this->shardNo);
        if ($redis === null) {
            $this->warn('[EdgeStream] cannot get redis connection for ensureConsumerGroup');

            return;
        }

        try {
            // 先检查 stream 是否存在
            $exists = $redis->exists($this->stream);
            if (! $exists) {
                // stream 不存在，先创建（写一条占位消息再删掉）
                $tempId = $redis->xAdd($this->stream, '*', ['init' => '1']);
                if ($tempId) {
                    $redis->xDel($this->stream, [$tempId]);
                }
            }

            // 创建消费组
            try {
                $redis->xGroup('CREATE', $this->stream, $this->groupName, '0', true);
                $this->info(sprintf('[EdgeStream] consumer group "%s" created on %s', $this->groupName, $this->stream));
            } catch (Throwable $e) {
                if (! str_contains($e->getMessage(), 'BUSYGROUP')) {
                    $this->warn(sprintf('[EdgeStream] xGroup CREATE warning: %s', $e->getMessage()));
                }
            }
        } finally {
            RedisShardManager::pushByShard($this->shardNo, $redis);
        }
    }

    /**
     * 恢复 Pending 消息（上次未 ACK 的）
     */
    private function recoverPending(): void
    {
        $redis = RedisShardManager::popByShard($this->shardNo);
        if ($redis === null) {
            return;
        }

        try {
            $pending = $redis->xPending(
                $this->stream,
                $this->groupName,
                '-',
                '+',
                100,
                $this->consumerName
            );

            if (empty($pending)) {
                return;
            }

            $this->info(sprintf('[EdgeStream] recovering %d pending messages', count($pending)));

            $ids = array_column($pending, 0);
            $messages = $redis->xRange($this->stream, $ids[0], $ids[count($ids) - 1]);

            foreach ($messages as $id => $fields) {
                $this->enqueueToBuffer($id, $fields);
            }

            // 立即刷一次
            $this->flushBuffer();
        } catch (Throwable $e) {
            $this->error(sprintf('[EdgeStream] recoverPending failed: %s', $e->getMessage()));
        } finally {
            RedisShardManager::pushByShard($this->shardNo, $redis);
        }
    }

    /**
     * Swow 协程模式：Reader + Writer 分离
     */
    private function runSwowMode(): int
    {
        $this->channel = new Channel($this->bufferLimit);

        // ---- Writer 协程 ----
        Coroutine::run(function (): void {
            $this->info('[EdgeStream] Writer coroutine started');

            while (! $this->stopping) {
                try {
                    // ★ -1 = 永久阻塞，0 = 不阻塞（退出时）
                    $item = $this->channel->pop($this->stopping ? 0 : -1);

                    if ($item === null) {
                        if ($this->stopping) {
                            break;
                        }
                        continue;
                    }

                    $this->buffer[] = $item;
                    $this->pendingIds[] = $item['stream_id'];

                    if (count($this->buffer) >= $this->batchSize) {
                        $this->flushBuffer();
                    }
                } catch (Throwable $e) {
                    $this->error(sprintf('[EdgeStream] Writer error: %s', $e->getMessage()));
                }
            }

            $this->flushBuffer();
            $this->info('[EdgeStream] Writer coroutine stopped');
        });

        // ---- Reader 主循环 ----
        $this->info('[EdgeStream] Reader loop started');

        while (! $this->stopping) {
            $redis = RedisShardManager::popByShard($this->shardNo);
            if ($redis === null) {
                usleep(100000);
                continue;
            }

            try {
                $messages = $redis->xReadGroup(
                    $this->groupName,
                    $this->consumerName,
                    [$this->stream => '>'],
                    $this->count,
                    $this->blockMs
                );

                if (empty($messages) || ! isset($messages[$this->stream])) {
                    $now = (int) (microtime(true) * 1000);
                    if ($now - $this->lastFlushAt > self::BUFFER_FLUSH_INTERVAL_MS) {
                        $this->flushBuffer();
                    }

                    continue;
                }

                foreach ($messages[$this->stream] as $id => $fields) {
                    if ($this->stopping) {
                        break;
                    }

                    $item = $this->parseStreamEntry($id, $fields);
                    if ($item === null) {
                        continue;
                    }

                    try {
                        // ★ push 超时：-1 永久阻塞，0 不阻塞
                        $this->channel->push($item, $this->stopping ? 0 : -1);
                    } catch (Throwable) {
                        $this->enqueueToBuffer($id, $fields);
                    }
                }

                $now = (int) (microtime(true) * 1000);
                if ($now - $this->lastFlushAt > self::BUFFER_FLUSH_INTERVAL_MS) {
                    $this->flushBuffer();
                }
            } catch (Throwable $e) {
                if ($this->stopping) {
                    break;
                }
                $this->error(sprintf('[EdgeStream] Reader error: %s', $e->getMessage()));
            } finally {
                RedisShardManager::pushByShard($this->shardNo, $redis);
            }
        }

        try {
            $this->channel->close();
        } catch (Throwable) {
            // ignore
        }

        $this->flushBuffer();
        $this->info('[EdgeStream] 已停止');

        return self::SUCCESS;
    }

    /**
     * 同步模式：Reader + Writer 同循环
     */
    private function runSyncMode(): int
    {
        $this->info('[EdgeStream] Sync mode loop started');

        while (! $this->stopping) {
            $redis = RedisShardManager::popByShard($this->shardNo);
            if ($redis === null) {
                usleep(100000);

                continue;
            }

            try {
                $messages = $redis->xReadGroup(
                    $this->groupName,
                    $this->consumerName,
                    [$this->stream => '>'],
                    $this->count,
                    $this->blockMs
                );

                if (! empty($messages) && isset($messages[$this->stream])) {
                    foreach ($messages[$this->stream] as $id => $fields) {
                        $this->enqueueToBuffer($id, $fields);
                    }
                }

                // 达到批量或定时刷库
                $now = (int) (microtime(true) * 1000);
                $shouldFlush = count($this->buffer) >= $this->batchSize
                    || ($now - $this->lastFlushAt > self::BUFFER_FLUSH_INTERVAL_MS && ! empty($this->buffer));

                if ($shouldFlush) {
                    $this->flushBuffer();
                }
            } catch (Throwable $e) {
                if ($this->stopping) {
                    break;
                }
                $this->error(sprintf('[EdgeStream] Sync loop error: %s', $e->getMessage()));
            } finally {
                RedisShardManager::pushByShard($this->shardNo, $redis);
            }
        }

        $this->flushBuffer();
        $this->info('[EdgeStream] 已停止');

        return self::SUCCESS;
    }

    /**
     * 解析 Stream 条目为缓冲数组
     */
    private function parseStreamEntry(string $id, array $fields): ?array
    {
        // 兼容 payload 可能是 JSON string 或已解码
        $payload = $fields['payload'] ?? $fields['data'] ?? null;
        if ($payload === null) {
            return null;
        }

        if (is_string($payload)) {
            // 保持原始 JSON string，写入 PG 时直接存 jsonb
            $payloadJson = $payload;
        } else {
            $payloadJson = json_encode($payload, JSON_UNESCAPED_UNICODE);
        }

        $device = $fields['device'] ?? $fields['client_id'] ?? '';
        $topic = $fields['topic'] ?? '';
        $ts = $fields['ts'] ?? microtime(true);

        return [
            'shard_no' => $this->shardNo,
            'source_stream' => $this->stream,
            'stream_id' => $id,
            'consumer_group' => $this->groupName,
            'client_id' => $device,
            'topic' => $topic,
            'payload' => $payloadJson,
            'received_ms' => (int) ($ts * 1000),
            'received_at' => date('Y-m-d H:i:s', (int) $ts),
        ];
    }

    /**
     * 加入内存缓冲（同步模式 / 通道满时降级）
     */
    private function enqueueToBuffer(string $id, array $fields): void
    {
        $item = $this->parseStreamEntry($id, $fields);
        if ($item === null) {
            return;
        }

        $this->buffer[] = $item;
        $this->pendingIds[] = $id;
    }

    /**
     * 批量写入 PG 并 XDEL/ACK
     */
    private function flushBuffer(): void
    {
        if (empty($this->buffer)) {
            return;
        }

        $batch = $this->buffer;
        $ids = $this->pendingIds;

        $this->buffer = [];
        $this->pendingIds = [];
        $this->lastFlushAt = (int) (microtime(true) * 1000);

        try {
            // 批量 upsert 到 PG
            EdgeStream::upsertBatch($batch);

            // 写成功 → XDEL 删除已处理消息
            $this->xDelBatch($ids);

            $this->info(sprintf(
                '[EdgeStream] flushed %d rows (shard=%d)',
                count($batch),
                $this->shardNo
            ));
        } catch (Throwable $e) {
            $this->error(sprintf(
                '[EdgeStream] flushBuffer failed: %s',
                $e->getMessage()
            ));

            // 区分毒数据 vs 临时性 DB 错误
            if ($this->isPoisonError($e)) {
                // 毒数据：写失败表，然后 XDEL
                $this->writeDeadLetter($batch, $e);
                $this->xDelBatch($ids);
                $this->warn(sprintf(
                    '[EdgeStream] poison data detected, moved to dead letter: %d rows',
                    count($batch)
                ));
            }
            // 临时性错误：不 XDEL，留给 PEL 下次重试
        }
    }

    /**
     * 判断是否为"毒数据"错误（不应重试）
     */
    private function isPoisonError(Throwable $e): bool
    {
        $msg = $e->getMessage();

        // JSON 解析错误
        if (str_contains($msg, 'invalid input syntax for type json')) {
            return true;
        }

        // 字段超长
        if (str_contains($msg, 'value too long')) {
            return true;
        }

        // 非空约束
        if (str_contains($msg, 'null value in column')) {
            return true;
        }

        // 数据类型错误
        if (str_contains($msg, 'invalid input syntax')) {
            return true;
        }

        return false;
    }

    /**
     * 写死信表
     */
    private function writeDeadLetter(array $batch, Throwable $e): void
    {
        $first = $batch[0] ?? null;
        if ($first === null) {
            return;
        }

        $streamId = $first['stream_id'] ?? '';
        $clientId = $first['client_id'] ?? 'unknown';
        $topic = $first['topic'] ?? '';
        $payload = $first['payload'] ?? '{}';
        $receivedMs = $first['received_ms'] ?? 0;
        $receivedAt = $first['received_at'] ?? null;

        $redis = RedisShardManager::popByShard($this->shardNo);
        $delivered = 0;

        if ($redis !== null) {
            try {
                $pendingInfo = $redis->xPending(
                    $this->stream,
                    $this->groupName,
                    $streamId,
                    $streamId,
                    1
                );
                $delivered = ! empty($pendingInfo) ? (int) ($pendingInfo[0][3] ?? 0) : 0;
            } catch (Throwable $inner) {
                $this->error(sprintf('[EdgeStream] xPending failed: %s', $inner->getMessage()));
            } finally {
                RedisShardManager::pushByShard($this->shardNo, $redis);
            }
        }

        try {
            EdgeStreamFail::insert([
                'shard_no' => $this->shardNo,
                'stream_id' => $streamId,
                'source_stream' => $this->stream,
                'client_id' => $clientId,
                'topic' => $topic,
                'payload' => $payload,
                'received_ms' => $receivedMs,
                'received_at' => $receivedAt,
                'last_error' => mb_substr($e->getMessage(), 0, 65535),
                'group_name' => $this->groupName,
                'consumer' => $this->consumerName,
                'delivered' => $delivered,
                'retry_num' => 0,
                'next_retry_at' => now()->addMinutes(1),
                'is_resolved' => false,
            ]);
        } catch (Throwable $inner) {
            $this->error(sprintf('[EdgeStream] writeDeadLetter failed: %s', $inner->getMessage()));
        }
    }

    /**
     * 批量 XDEL
     */
    private function xDelBatch(array $ids): void
    {
        if (empty($ids)) {
            return;
        }

        $redis = RedisShardManager::popByShard($this->shardNo);
        if ($redis === null) {
            return;
        }

        try {
            $redis->xDel($this->stream, $ids);
        } catch (Throwable $e) {
            $this->error(sprintf('[EdgeStream] xDel failed: %s', $e->getMessage()));
        } finally {
            RedisShardManager::pushByShard($this->shardNo, $redis);
        }
    }
}
