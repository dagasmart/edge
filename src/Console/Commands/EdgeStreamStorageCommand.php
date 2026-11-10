<?php

declare(strict_types=1);

namespace DagaSmart\Edge\Console\Commands;

use DagaSmart\Edge\Models\EdgeStream;
use DagaSmart\Edge\Models\EdgeStreamFail;
use DagaSmart\Edge\Models\EdgeStreamOffset;
use DagaSmart\Edge\Support\RedisShardManager;
use Illuminate\Console\Command;
use Swow\Channel;
use Swow\Coroutine;
use Throwable;

/**
 * Redis Stream → PostgreSQL 持久化命令（分片版 + 可靠性增强）
 *
 * 使用方式：
 *   php artisan edge:stream-storage --shard=0
 *   php artisan edge:stream-storage --shard=0 --backfill
 *   php artisan edge:stream-storage --shard=0 --retry-dead
 *   php artisan edge:stream-storage --stream=stream:device:data:0 --group=edge_storage --consumer=worker_0_1
 */
class EdgeStreamStorageCommand extends Command
{
    protected $signature = 'edge:stream-storage
        {--stream= : Redis Stream key (如 stream:device:data:0)，不填则按 --shard 推导}
        {--shard=0 : 直接指定分片号，自动推导 stream key}
        {--group=edge_storage : 消费组名}
        {--consumer= : 消费者名，默认 auto_{pid}}
        {--count=50 : 每次 XREADGROUP 拉取条数}
        {--block=2000 : XREADGROUP 阻塞毫秒}
        {--batch=50 : 批量写入 PG 条数}
        {--swow : 强制 Swow 协程模式}
        {--sync : 强制同步模式}
        {--buffer-limit=20000 : Redis List 缓冲上限（Swow 模式）}
        {--once : 一批即退（调试用）}
        {--retry-dead : 仅执行死信重试，不消费 Redis Stream}
        {--retry-batch=50 : 死信重试批次大小}
        {--max-retries=3 : 死信最大重试次数}
        {--backfill : 独立 XREAD 回刷 Stream 历史存量，不碰消费组}
        {--max-payload=65536 : 单条 payload 最大字节数}';

    protected $description = 'Redis Stream → PostgreSQL 持久化（分片并行消费 + 可靠性增强）';

    private const string STREAM_PREFIX = 'stream:device:data:';

    private const string BUFFER_KEY_PREFIX = 'edge:buffer:insert_redis:';

    private const int BUFFER_FLUSH_INTERVAL_MS = 500;

    private const int PEL_BATCH = 20;

    private const int PEL_MAX_DELIVERED = 3;

    private const int BACKFILL_BLOCK_MS = 2000;

    private const int DRAIN_TIMEOUT_SEC = 3;

    /* ==================== 运行时状态 ==================== */

    private string $stream;

    private int $shardNo;

    private string $groupName;

    private string $consumerName;

    private int $count;

    private int $blockMs;

    private int $batchSize;

    private bool $swowMode;

    private int $bufferLimit;

    private int $maxPayload;

    private string $bufferKey;

    /** 优雅退出标志 */
    private bool $stopping = false;

    /** @var Channel|null Swow 模式下备用通道（Redis List 优先） */
    private ?Channel $channel = null;

    /** 统计 */
    private int $received = 0;

    private int $written = 0;

    private int $dead = 0;

    private int $deleted = 0;

    private int $duplicated = 0;

    public function handle(): int
    {
        // ---- 参数解析 ----
        $this->parseOptions();

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

        // ---- 初始化 ----
        RedisShardManager::init();
        $this->ensureConsumerGroup();

        // ---- 子命令路由 ----
        if ($this->option('retry-dead')) {
            return $this->runRetryDead();
        }

        if ($this->option('backfill')) {
            return $this->runBackfill();
        }

        // ---- 信号处理 ----
        $this->registerSignalHandlers();

        // ---- 恢复 Pending 消息 ----
        $this->recoverPending();

        // ---- 主循环 ----
        if ($this->swowMode) {
            return $this->runSwowMode();
        }

        return $this->runSyncMode();
    }

    /* ==================== 参数解析 ==================== */

    private function parseOptions(): void
    {
        $streamOpt = $this->option('stream');
        $shardOpt = $this->option('shard');

        if ($streamOpt !== null && $streamOpt !== '') {
            $this->stream = $streamOpt;
            if (preg_match('/stream:device:data:(\d+)/', $this->stream, $m)) {
                $this->shardNo = (int) $m[1];
            } else {
                $this->shardNo = 0;
            }
        } elseif ($shardOpt !== null && $shardOpt !== '') {
            $this->shardNo = (int) $shardOpt;
            $this->stream = self::STREAM_PREFIX . $this->shardNo;
        } else {
            $this->shardNo = 0;
            $this->stream = self::STREAM_PREFIX . '0';
        }

        $this->groupName = $this->option('group') ?: 'edge_storage';
        $this->consumerName = $this->option('consumer') ?: ('auto_' . getmypid());
        $this->count = max(1, (int) $this->option('count') ?: 50);
        $this->blockMs = max(100, (int) $this->option('block') ?: 2000);
        $this->batchSize = max(1, (int) $this->option('batch') ?: 50);
        $this->bufferLimit = max(100, (int) $this->option('buffer-limit') ?: 20000);
        $this->maxPayload = max(1024, (int) $this->option('max-payload') ?: 65536);

        // Buffer Key 按分片隔离
        $this->bufferKey = self::BUFFER_KEY_PREFIX . $this->shardNo;

        // 检测 Swow
        $hasSwow = extension_loaded('swow');
        $forceSwow = (bool) $this->option('swow');
        $forceSync = (bool) $this->option('sync');
        $this->swowMode = (($hasSwow && ! $forceSync) || $forceSwow) && ! $forceSync;
    }

    /* ==================== 信号处理（双路径） ==================== */

    private function registerSignalHandlers(): void
    {
        // Swow Signal
        if ($this->swowMode && class_exists(\Swow\Signal::class)) {
            foreach ([\Swow\Signal::INT, \Swow\Signal::TERM] as $sig) {
                Coroutine::run(function () use ($sig): void {
                    try {
                        \Swow\Signal::wait($sig);
                    } catch (Throwable) {
                        return;
                    }
                    $this->handleStopSignal($sig);
                });
            }
        }

        // pcntl fallback
        if (extension_loaded('pcntl') && function_exists('pcntl_signal')) {
            pcntl_async_signals(true);
            pcntl_signal(SIGINT, fn () => $this->handleStopSignal(SIGINT));
            pcntl_signal(SIGTERM, fn () => $this->handleStopSignal(SIGTERM));
        }
    }

    private function handleStopSignal(int $sig): void
    {
        if ($this->stopping) {
            $this->error('[signal] 再次收到信号，强制退出');
            exit(1);
        }
        $this->warn(sprintf('[signal] 收到 %d，优雅停机', $sig));
        $this->stopping = true;

        // PEL 清理 + 死信锁释放
        $this->handlePending();
        EdgeStreamFail::forceUnlock($this->stream, $this->groupName);
    }

    /* ==================== 消费组 & 恢复 ==================== */

    private function ensureConsumerGroup(): void
    {
        $redis = RedisShardManager::popByShard($this->shardNo);
        if ($redis === null) {
            $this->warn('[EdgeStream] cannot get redis connection for ensureConsumerGroup');
            return;
        }

        try {
            $exists = $redis->exists($this->stream);
            if (! $exists) {
                $tempId = $redis->xAdd($this->stream, '*', ['init' => '1']);
                if ($tempId) {
                    $redis->xDel($this->stream, [$tempId]);
                }
            }

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
     * 恢复 PEL 中遗留的消息（文档2核心逻辑 + 分片适配）
     */
    private function recoverPending(): void
    {
        $this->line('[RECOVER] 检查 PEL 遗留消息...');

        $totalRecovered = 0;
        $totalDeleted = 0;
        $maxDelivered = self::PEL_MAX_DELIVERED - 1;

        while (true) {
            $redis = RedisShardManager::popByShard($this->shardNo);
            if ($redis === null) {
                break;
            }

            try {
                $pending = $redis->xPending($this->stream, $this->groupName, '-', '+', self::PEL_BATCH);
            } catch (Throwable $e) {
                $this->error('[RECOVER] xPending failed: ' . $e->getMessage());
                RedisShardManager::pushByShard($this->shardNo, $redis);
                break;
            }

            if (empty($pending)) {
                RedisShardManager::pushByShard($this->shardNo, $redis);
                break;
            }

            $claimIds = [];
            $skipIds = [];

            foreach ($pending as $p) {
                [, , , $delivered] = $p;
                if ((int) $delivered <= $maxDelivered) {
                    $claimIds[] = (string) $p[0];
                } else {
                    $skipIds[] = (string) $p[0];
                }
            }

            // 超限的直接死信 + XDEL
            if ($skipIds !== []) {
                $this->moveToDeadLetter($skipIds, 'PEL max delivered (recover)');
                $this->xDelBatch($skipIds);
                $totalDeleted += count($skipIds);
            }

            RedisShardManager::pushByShard($this->shardNo, $redis);

            if ($claimIds === []) {
                if ($skipIds === []) {
                    break;
                }
                continue;
            }

            // XCLAIM
            $redis = RedisShardManager::popByShard($this->shardNo);
            if ($redis === null) {
                break;
            }

            try {
                $claimed = $redis->xClaim(
                    $this->stream, $this->groupName, $this->consumerName,
                    max(1000, $this->blockMs),
                    $claimIds,
                    ['RETRYCOUNT' => 0, 'FORCE' => true]
                );
            } catch (Throwable $e) {
                $this->error('[RECOVER] XCLAIM failed: ' . $e->getMessage());
                RedisShardManager::pushByShard($this->shardNo, $redis);
                break;
            }

            if (empty($claimed)) {
                RedisShardManager::pushByShard($this->shardNo, $redis);
                break;
            }

            // 写入 PG
            $buffer = [];
            $invalidIds = [];

            foreach ($claimed as $id => $data) {
                $item = $this->parseStreamEntry($id, $data);
                if ($item === null) {
                    $invalidIds[] = (string) $id;
                    continue;
                }
                $buffer[] = $item;
            }

            RedisShardManager::pushByShard($this->shardNo, $redis);

            if ($invalidIds !== []) {
                $this->xDelBatch($invalidIds);
                $totalDeleted += count($invalidIds);
            }

            if (! empty($buffer)) {
                try {
                    EdgeStream::upsertBatch($buffer);
                    $ids = array_map(fn ($r) => (string) $r['stream_id'], $buffer);
                    $this->xDelBatch($ids);
                    $totalRecovered += count($buffer);
                    $totalDeleted += count($ids);
                    $this->written += count($buffer);
                } catch (Throwable $e) {
                    $this->error('[RECOVER] upsertBatch failed: ' . $e->getMessage());
                    $this->writeDeadLetterBatch($buffer, $e);
                    $ids = array_map(fn ($r) => (string) $r['stream_id'], $buffer);
                    $this->xDelBatch($ids);
                    $totalDeleted += count($ids);
                }
            }

            if (count($pending) < self::PEL_BATCH) {
                break;
            }
        }

        if ($totalRecovered > 0 || $totalDeleted > 0) {
            $this->line(sprintf('[RECOVER] 恢复 %d 条，XDEL %d 条', $totalRecovered, $totalDeleted));
        } else {
            $this->line('[RECOVER] 无遗留消息');
        }
    }

    /**
     * 停机时处理 PEL 中超限消息
     */
    private function handlePending(): void
    {
        try {
            $redis = RedisShardManager::popByShard($this->shardNo);
            if ($redis === null) {
                return;
            }

            try {
                $pending = $redis->xPending($this->stream, $this->groupName, '-', '+', self::PEL_BATCH);
            } catch (Throwable $e) {
                $this->error('[PEL] xPending failed: ' . $e->getMessage());
                RedisShardManager::pushByShard($this->shardNo, $redis);
                return;
            }

            if (empty($pending)) {
                RedisShardManager::pushByShard($this->shardNo, $redis);
                return;
            }

            $claimedIds = [];
            foreach ($pending as $p) {
                [, , $idle, $delivered] = $p;
                if ((int) $delivered < self::PEL_MAX_DELIVERED) {
                    continue;
                }

                $claimed = $redis->xClaim(
                    $this->stream, $this->groupName, $this->consumerName,
                    max((int) $idle, 1000),
                    [(string) $p[0]],
                    ['RETRYCOUNT' => 0, 'FORCE' => true]
                );

                foreach ($claimed as $cid => $data) {
                    $payload = (string) ($data['payload'] ?? '{}');
                    $device = (string) ($data['device'] ?? 'unknown');
                    $topic = (string) ($data['topic'] ?? '');
                    $ts = (float) ($data['ts'] ?? microtime(true));

                    EdgeStreamFail::insert([
                        'shard_no' => $this->shardNo,
                        'stream_id' => (string) $cid,
                        'source_stream' => $this->stream,
                        'client_id' => $device,
                        'topic' => $topic,
                        'payload' => $payload,
                        'received_ms' => (int) ($ts * 1000),
                        'received_at' => date('Y-m-d H:i:s', (int) $ts),
                        'last_error' => 'PEL max delivered on shutdown',
                        'group_name' => $this->groupName,
                        'consumer' => $this->consumerName,
                        'delivered' => (int) $delivered,
                        'retry_num' => 0,
                        'next_retry_at' => now()->addMinutes(1),
                        'is_resolved' => false,
                    ]);
                    $this->dead++;
                    $claimedIds[] = (string) $cid;
                }
            }

            if ($claimedIds !== []) {
                $this->xDelBatch($claimedIds);
            }

            RedisShardManager::pushByShard($this->shardNo, $redis);
        } catch (Throwable $e) {
            $this->error('[PEL] ' . $e->getMessage());
        }
    }

    /* ==================== Swow 模式（Redis List 通道） ==================== */

    private function runSwowMode(): int
    {
        $this->info('[EdgeStream] Swow 模式启动');

        // 清空上次遗留 buffer
        $this->clearBuffer();

        // Writer 协程
        Coroutine::run(fn () => $this->writerLoop());

        // Reader 主循环
        $this->readerLoop();

        // 停机排空
        $this->drainBuffer();
        $this->printStats();

        return self::SUCCESS;
    }

    private function readerLoop(): void
    {
        $this->line('[READER] 启动');

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
                    $this->periodicFlush();
                    continue;
                }

                foreach ($messages[$this->stream] as $id => $fields) {
                    if ($this->stopping) {
                        break;
                    }

                    $item = $this->parseStreamEntry($id, $fields);
                    if ($item === null) {
                        $this->xDelBatch([(string) $id]);
                        continue;
                    }

                    // payload 大小检查
                    if (strlen($item['payload']) > $this->maxPayload) {
                        EdgeStreamFail::insert([/* ... 死信 ... */]);
                        $this->xDelBatch([(string) $id]);
                        $this->dead++;
                        continue;
                    }

                    $this->received++;

                    // 写入 Redis List 缓冲
                    try {
                        $redis->lPush($this->bufferKey, json_encode($item, JSON_UNESCAPED_UNICODE | JSON_UNESCAPED_SLASHES));
                    } catch (Throwable $e) {
                        $this->error('[READER] lPush failed: ' . $e->getMessage());
                    }
                }

                $this->periodicFlush();
            } catch (Throwable $e) {
                if (! $this->stopping) {
                    $this->error('[READER] error: ' . $e->getMessage());
                }
            } finally {
                RedisShardManager::pushByShard($this->shardNo, $redis);
            }

            if ($this->option('once')) {
                break;
            }
        }

        $this->line('[READER] 退出');
    }

    private function writerLoop(): void
    {
        $this->line('[WRITER] 启动');

        $batch = [];
        $lastFlush = microtime(true);

        while (! $this->stopping) {
            try {
                $raw = withRedisForKey($this->getRedisKeyForShard(), fn (\Redis $conn) =>
                $conn->bRPop([$this->bufferKey], max(1, (int) ($this->blockMs / 1000)))
                );
            } catch (Throwable $e) {
                $this->warn('[WRITER] bRPop error: ' . $e->getMessage());
                usleep(500000);
                continue;
            }

            if (empty($raw)) {
                // 超时，检查是否需要定时刷盘
                if (! empty($batch) && (microtime(true) - $lastFlush) * 1000 >= self::BUFFER_FLUSH_INTERVAL_MS) {
                    $this->flushBatch($batch);
                    $batch = [];
                    $lastFlush = microtime(true);
                }
                continue;
            }

            $item = json_decode($raw[1], true);
            if ($item === null) {
                continue;
            }

            $batch[] = $item;

            if (count($batch) >= $this->batchSize) {
                $this->flushBatch($batch);
                $batch = [];
                $lastFlush = microtime(true);
            }
        }

        // 排空残留
        if (! empty($batch)) {
            $this->flushBatch($batch);
        }

        $this->line('[WRITER] 退出');
    }

    /* ==================== 同步模式 ==================== */

    private function runSyncMode(): int
    {
        $this->info('[EdgeStream] Sync 模式启动');

        $buffer = [];
        $lastFlush = microtime(true);

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
                        $item = $this->parseStreamEntry($id, $fields);
                        if ($item === null) {
                            $this->xDelBatch([(string) $id]);
                            continue;
                        }
                        $buffer[] = $item;
                        $this->received++;
                    }
                }

                $now = microtime(true);
                $shouldFlush = count($buffer) >= $this->batchSize
                    || ($now - $lastFlush) * 1000 >= self::BUFFER_FLUSH_INTERVAL_MS;

                if ($shouldFlush && ! empty($buffer)) {
                    $this->flushBatch($buffer);
                    $buffer = [];
                    $lastFlush = $now;
                }
            } catch (Throwable $e) {
                if (! $this->stopping) {
                    $this->error('[Sync] error: ' . $e->getMessage());
                }
            } finally {
                RedisShardManager::pushByShard($this->shardNo, $redis);
            }

            if ($this->option('once')) {
                break;
            }
        }

        if (! empty($buffer)) {
            $this->flushBatch($buffer);
        }

        $this->printStats();
        return self::SUCCESS;
    }

    /* ==================== 核心：写 PG + XDEL ==================== */

    private function flushBatch(array &$batch): void
    {
        if (empty($batch)) {
            return;
        }

        $ids = array_map(fn ($r) => (string) $r['stream_id'], $batch);

        try {
            // 验证
            $validBatch = [];
            $invalidIds = [];
            foreach ($batch as $row) {
                $errors = EdgeStream::validateRow($row);
                if ($errors !== []) {
                    $this->writeDeadLetter($row, implode('; ', $errors));
                    $invalidIds[] = (string) $row['stream_id'];
                    $this->dead++;
                    continue;
                }
                $validBatch[] = $row;
            }

            if ($invalidIds !== []) {
                $this->xDelBatch($invalidIds);
            }

            if (empty($validBatch)) {
                $batch = [];
                return;
            }

            // 批量写入 PG
            [$written] = EdgeStream::upsertBatch($validBatch);
            $this->written += $written;

            // 写成功 → XDEL
            $validIds = array_map(fn ($r) => (string) $r['stream_id'], $validBatch);
            $this->xDelBatch($validIds);
            $this->deleted += count($validIds);

            // 记录偏移量
            $last = end($validBatch);
            if (! empty($last['stream_id'])) {
                EdgeStreamOffset::query()->updateOrInsert(
                    ['stream' => $this->stream, 'group_name' => $this->groupName],
                    [
                        'last_id' => (string) $last['stream_id'],
                        'last_lag' => 0,
                        'updated_at' => now(),
                    ]
                );
            }

            $this->line(sprintf('[FLUSH] batch=%d written=%d XDEL=%d', count($validBatch), $written, count($validIds)));
        } catch (Throwable $e) {
            $this->error('[FLUSH] failed: ' . $e->getMessage());
            // 临时性错误 → 死信 + XDEL（避免阻塞）
            $this->writeDeadLetterBatch($batch, $e);
            $this->xDelBatch($ids);
        }

        $batch = [];
    }

    /* ==================== 回刷存量（--backfill） ==================== */

    private function runBackfill(): int
    {
        $this->info(sprintf('[BACKFILL] shard=%d stream=%s', $this->shardNo, $this->stream));

        $lastId = '0';
        $emptyCount = 0;

        while (! $this->stopping) {
            $redis = RedisShardManager::popByShard($this->shardNo);
            if ($redis === null) {
                break;
            }

            try {
                $entries = $redis->xRead(
                    [$this->stream => $lastId],
                    $this->count,
                    self::BACKFILL_BLOCK_MS
                );
            } catch (Throwable $e) {
                $this->error('[BACKFILL] xRead failed: ' . $e->getMessage());
                RedisShardManager::pushByShard($this->shardNo, $redis);
                break;
            }

            if (empty($entries)) {
                $emptyCount++;
                RedisShardManager::pushByShard($this->shardNo, $redis);
                if ($emptyCount >= 3) {
                    break;
                }
                continue;
            }

            $emptyCount = 0;
            $items = $entries[$this->stream] ?? [];

            $buffer = [];
            foreach ($items as $id => $data) {
                $item = $this->parseStreamEntry($id, $data);
                if ($item !== null) {
                    $buffer[] = $item;
                    $this->received++;
                }
            }

            if (! empty($buffer)) {
                $ids = array_map(fn ($r) => (string) $r['stream_id'], $buffer);
                try {
                    EdgeStream::upsertBatch($buffer);
                    $this->written += count($buffer);
                    $this->xDelBatch($ids);
                } catch (Throwable $e) {
                    $this->writeDeadLetterBatch($buffer, $e);
                    $this->xDelBatch($ids);
                }
            }

            // 更新 lastId
            $lastEntry = array_key_last($items);
            $lastId = (string) $lastEntry;

            if (count($items) < $this->count) {
                break;
            }
        }

        $this->line('[BACKFILL] 完成');
        $this->printStats();
        return self::SUCCESS;
    }

    /* ==================== 死信重试（--retry-dead） ==================== */

    private function runRetryDead(): int
    {
        $batchSize = max(1, (int) $this->option('retry-batch'));
        $maxRetries = max(1, (int) $this->option('max-retries'));

        $this->info(sprintf('[RETRY-DEAD] shard=%d batch=%d maxRetries=%d', $this->shardNo, $batchSize, $maxRetries));

        $total = ['retried' => 0, 'succeeded' => 0, 'failed' => 0];

        while (true) {
            $claims = EdgeStreamFail::claimBatch($this->stream, $this->groupName, $batchSize, $maxRetries, $this->blockMs);
            if ($claims === []) {
                break;
            }

            $stats = EdgeStream::retryBatch($claims, $maxRetries);
            $total['retried'] += $stats['retried'];
            $total['succeeded'] += $stats['succeeded'];
            $total['failed'] += $stats['failed'];

            $this->line(sprintf('[RETRY-DEAD] retried=%d succeeded=%d failed=%d', $stats['retried'], $stats['succeeded'], $stats['failed']));
        }

        $this->info(sprintf('[RETRY-DEAD] 完成 retried=%d succeeded=%d failed=%d', $total['retried'], $total['succeeded'], $total['failed']));
        return self::SUCCESS;
    }

    /* ==================== 工具方法 ==================== */

    private function parseStreamEntry(string $id, array $fields): ?array
    {
        $payload = $fields['payload'] ?? $fields['data'] ?? null;
        if ($payload === null) {
            return null;
        }

        if (is_string($payload)) {
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

    private function writeDeadLetter(array $item, Throwable|string $reason): void
    {
        $msg = $reason instanceof Throwable ? $reason->getMessage() : $reason;
        try {
            EdgeStreamFail::insert([
                'shard_no' => $this->shardNo,
                'stream_id' => $item['stream_id'] ?? '',
                'source_stream' => $this->stream,
                'client_id' => $item['client_id'] ?? 'unknown',
                'topic' => $item['topic'] ?? '',
                'payload' => $item['payload'] ?? '{}',
                'received_ms' => $item['received_ms'] ?? 0,
                'received_at' => $item['received_at'] ?? null,
                'last_error' => mb_substr($msg, 0, 65535),
                'group_name' => $this->groupName,
                'consumer' => $this->consumerName,
                'delivered' => 0,
                'retry_num' => 0,
                'next_retry_at' => now()->addMinutes(1),
                'is_resolved' => false,
            ]);
        } catch (Throwable $e) {
            $this->error('[DEAD] writeDeadLetter failed: ' . $e->getMessage());
        }
    }

    private function writeDeadLetterBatch(array $batch, Throwable|string $reason): void
    {
        $msg = $reason instanceof Throwable ? $reason->getMessage() : $reason;
        foreach ($batch as $item) {
            $this->writeDeadLetter($item, $msg);
        }
        $this->dead += count($batch);
    }

    private function moveToDeadLetter(array $ids, string $reason): void
    {
        $redis = RedisShardManager::popByShard($this->shardNo);
        if ($redis === null) {
            return;
        }

        try {
            $messages = $redis->xRange($this->stream, $ids[0], $ids[count($ids) - 1]);
            foreach ($messages as $id => $fields) {
                if (! in_array((string) $id, $ids, true)) {
                    continue;
                }
                $item = $this->parseStreamEntry($id, $fields);
                if ($item !== null) {
                    $this->writeDeadLetter($item, $reason);
                }
            }
        } catch (Throwable $e) {
            $this->error('[DEAD] moveToDeadLetter failed: ' . $e->getMessage());
        } finally {
            RedisShardManager::pushByShard($this->shardNo, $redis);
        }
    }

    private function xDelBatch(array $ids): void
    {
        $ids = array_values(array_filter($ids, fn ($id) => $id !== ''));
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
            $this->error('[XDEL] failed: ' . $e->getMessage());
        } finally {
            RedisShardManager::pushByShard($this->shardNo, $redis);
        }
    }

    private function clearBuffer(): void
    {
        try {
            $redis = RedisShardManager::popByShard($this->shardNo);
            if ($redis !== null) {
                $redis->del([$this->bufferKey]);
                RedisShardManager::pushByShard($this->shardNo, $redis);
            }
        } catch (Throwable) {
            // ignore
        }
    }

    private function drainBuffer(): void
    {
        $this->line('[DRAIN] 排空 Buffer...');
        $drained = 0;

        while (! $this->stopping) {
            try {
                $raw = withRedisForKey($this->getRedisKeyForShard(), fn (\Redis $conn) =>
                $conn->bRPop([$this->bufferKey], 1)
                );
            } catch (Throwable) {
                break;
            }

            if (empty($raw)) {
                break;
            }

            $item = json_decode($raw[1], true);
            if ($item) {
                $this->flushBatch([$item]);
                $drained++;
            }
        }

        $this->line(sprintf('[DRAIN] 处理 %d 条残留', $drained));
    }

    private function periodicFlush(): void
    {
        // 定时触发由 Writer 的 bRPop 超时机制处理，Reader 不做 flush
        // 这里留作扩展点
    }

    private function getRedisKeyForShard(): string
    {
        return 'stream:device:data:' . $this->shardNo;
    }

    private function printStats(): void
    {
        $this->line('========== 消费统计 ==========');
        $this->line(sprintf('%-20s %s', '分片：', $this->shardNo));
        $this->line(sprintf('%-20s %s', '接收：', $this->received));
        $this->line(sprintf('%-20s %s', '写入：', $this->written));
        $this->line(sprintf('%-20s %s', '死信：', $this->dead));
        $this->line(sprintf('%-20s %s', 'XDEL：', $this->deleted));
        $this->line(sprintf('%-20s %s', '去重：', $this->duplicated));
        $this->line('==============================');
    }

    private function configInt(string $key, int $default): int
    {
        try {
            $val = config($key, $default);
            return (int) $val;
        } catch (Throwable) {
            return $default;
        }
    }
}
