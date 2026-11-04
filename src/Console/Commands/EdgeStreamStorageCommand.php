<?php

declare(strict_types=1);

namespace DagaSmart\Edge\Console\Commands;

use DagaSmart\Edge\Models\EdgeStream;
use DagaSmart\Edge\Models\EdgeStreamFail;
use DagaSmart\Edge\Models\EdgeStreamOffset;
use Illuminate\Console\Command;
use Swow\Channel;
use Swow\Coroutine;
use Throwable;

/**
 * Redis Stream -> PG
 *
 * 数据清理策略：
 *   - 启动 recoverPending()：XCLAIM 认领 PEL 存量 → 写 PG → XDEL 删除 Redis 数据
 *   - 实时消费：xReadGroup('>') → 写 PG → XDEL 删除（不只 ACK）
 *   - 停机 handlePending()：PEL 超限 → 死信表 → XDEL 删除
 *   - 写 PG 失败 → 进死信 → XDEL 删除（避免反复阻塞）
 *   - --backfill：XREAD 从 0 独立消费历史存量，不碰消费组
 */
$edgeRoot = dirname(__DIR__, 3);
require_once $edgeRoot.'/bin/bootstrap.php';
require_once $edgeRoot.'/bin/helper.php';

class EdgeStreamStorageCommand extends Command
{
    protected $signature = 'edge:stream-storage
                            {--stream=stream:device:data}
                            {--group=edge_storage}
                            {--consumer=worker_1}
                            {--batch=50}
                            {--timeout=2000}
                            {--channel-size=2048}
                            {--max-payload=65536}
                            {--swow : 强制 Swow 协程模式}
                            {--no-swow : 强制同步模式}
                            {--once : 一批即退}
                            {--retry-dead : 仅执行死信重试，不消费 Redis Stream}
                            {--retry-batch=50 : 死信重试批次大小}
                            {--max-retries=3 : 死信最大重试次数}
                            {--backfill : 独立 XREAD 回刷 Stream 历史存量，不碰消费组}';

    protected $description = 'Redis Stream -> PG，消费成功即 XDEL 清理 Redis 数据';

    private const int PEL_BATCH = 20;
    private const int PEL_MAX_DELIVERED = 3;
    private const int DRAIN_TIMEOUT_MS = 3000;
    private const int DRAIN_POP_TIMEOUT_MS = 50;
    private const int BACKFILL_BLOCK_MS = 2000;

    private bool $stopping = false;
    private int $received = 0;
    private int $written = 0;
    private int $duplicated = 0;
    private int $dead = 0;
    private int $deleted = 0;

    private ?Channel $sleepChannel = null;
    private ?Channel $writeChannel = null;

    protected function getDriver(): string
    {
        if ($this->option('swow')) {
            return 'swow';
        }
        if ($this->option('no-swow')) {
            return 'sync';
        }
        if (PHP_OS_FAMILY === 'Windows') {
            return 'sync';
        }

        return extension_loaded('swow') ? 'swow' : 'sync';
    }

    private function sleep(float $seconds): void
    {
        if ($this->getDriver() === 'swow') {
            try {
                if ($this->sleepChannel === null) {
                    $this->sleepChannel = new Channel(1);
                }
                $this->sleepChannel->pop((int) ($seconds * 1000));

                return;
            } catch (Throwable) {
                // 降级
            }
        }

        usleep((int) ($seconds * 1_000_000));
    }

    public function handle(): int
    {
        $this->info('[edge:stream-storage] 启动，PID='.getmypid());

        if ($this->option('retry-dead')) {
            return $this->runRetryDead();
        }

        if ($this->option('backfill')) {
            return $this->runBackfill();
        }

        $driver = $this->getDriver();
        $this->info("启动模式：{$driver}");

        if ($driver === 'swow') {
            $this->initRedisPool();
        }

        return $driver === 'swow'
            ? $this->runSwowMode()
            : $this->runSyncMode();
    }

    /* ==================== 回刷存量 ==================== */

    private function runBackfill(): int
    {
        $driver = $this->getDriver();
        $stream = (string) $this->option('stream');
        $batchSize = max(1, (int) $this->option('batch'));
        $maxPayload = max(1, (int) $this->option('max-payload'));

        $this->info(sprintf(
            '[BACKFILL] stream=%s batch=%d driver=%s',
            $stream,
            $batchSize,
            $driver,
        ));

        if ($driver === 'swow') {
            $this->initRedisPool();
            $this->writeChannel = new Channel(
                max(1, (int) $this->option('channel-size')),
            );

            Coroutine::run(fn () => $this->backfillWriterLoop($stream));
            Coroutine::run(fn () => $this->backfillLoop($stream, $batchSize, $maxPayload));

            // writer 会把 $this->writeChannel 置 null，这里作为退出信号
            while ($this->writeChannel !== null) {
                $this->sleep(0.2);
            }
        } else {
            $this->backfillSync($stream, $batchSize, $maxPayload);
        }

        $this->printStats();

        return 0;
    }

    /**
     * 同步回刷：XREAD 从 0 开始，读完即 XDEL
     */
    private function backfillSync(string $stream, int $batchSize, int $maxPayload): void
    {
        $lastId = '0';

        while (! $this->stopping) {
            try {
                $entries = withRedis(fn (\Redis $conn) => $conn->xRead(
                    [$stream => $lastId],
                    $batchSize,
                    self::BACKFILL_BLOCK_MS,
                ));
            } catch (Throwable $e) {
                $this->error('[BACKFILL] xRead 失败：'.$e->getMessage());
                break;
            }

            if (empty($entries)) {
                break;
            }

            $items = $entries[$stream] ?? [];
            if (empty($items)) {
                break;
            }

            $buffer = [];
            $invalidIds = [];

            foreach ($items as $id => $data) {
                $payload = (string) ($data['payload'] ?? '{}');
                $device = (string) ($data['device'] ?? 'unknown');
                $topic = (string) ($data['topic'] ?? '');
                $ts = (float) ($data['ts'] ?? microtime(true));

                if (strlen($payload) > $maxPayload) {
                    EdgeStream::writeDead(
                        (string) $id, $stream, '', '', 1,
                        $payload, 'payload too large (backfill)',
                        $device, $topic,
                        (int) ($ts * 1000),
                        date('Y-m-d H:i:s', (int) $ts),
                    );
                    $invalidIds[] = (string) $id;
                    $this->dead++;
                    continue;
                }

                $this->received++;
                $buffer[] = [
                    'stream_id'   => (string) $id,
                    'client_id'   => $device,
                    'topic'       => $topic,
                    'payload'     => $payload,
                    'received_ms' => (int) ($ts * 1000),
                    'received_at' => date('Y-m-d H:i:s', (int) $ts),
                ];
            }

            if ($invalidIds !== []) {
                $this->xDel($stream, $invalidIds);
            }

            if (! empty($buffer)) {
                $ids = array_map(fn ($r) => (string) $r['stream_id'], $buffer);

                try {
                    [$written] = EdgeStream::upsertBatch($buffer);
                    $this->written += $written;
                    $this->xDel($stream, $ids);
                } catch (Throwable $e) {
                    $this->error('[BACKFILL] 写入失败：'.$e->getMessage());
                    foreach ($buffer as $row) {
                        EdgeStream::writeDead(
                            (string) ($row['stream_id'] ?? ''), $stream, '', '', 1,
                            (string) ($row['payload'] ?? '{}'), $e->getMessage(),
                            (string) ($row['client_id'] ?? 'unknown'),
                            (string) ($row['topic'] ?? ''),
                            (int) ($row['received_ms'] ?? 0),
                            (string) ($row['received_at'] ?? ''),
                        );
                        $this->dead++;
                    }
                    $this->xDel($stream, $ids);
                }
            }

            // 推进游标：取最后一条 id
            $lastEntry = null;
            foreach ($items as $id => $data) {
                $lastEntry = $id;
            }
            $lastId = (string) $lastEntry;

            if (count($items) < $batchSize) {
                break; // 已经读完
            }
        }

        $this->line('[BACKFILL] 同步回刷结束');
    }

    /**
     * Swow 回刷 reader：XREAD 从 0 推送到 writeChannel
     */
    private function backfillLoop(string $stream, int $batchSize, int $maxPayload): void
    {
        $this->line('[BACKFILL][READER] 启动');

        $lastId = '0';
        $emptyCount = 0;           // 连续空返回计数
        $maxEmpty = 3;             // 连续 3 次空返回才认为无数据

        while (! $this->stopping) {
            try {
                $entries = withRedis(fn (\Redis $conn) => $conn->xRead(
                    [$stream => $lastId],
                    $batchSize,
                    self::BACKFILL_BLOCK_MS,
                ));
            } catch (Throwable $e) {
                $this->error('[BACKFILL][READER] xRead 失败：' . $e->getMessage());
                break;
            }

            // 阻塞模式下空返回 = 当前无数据，不是结束信号
            if (empty($entries)) {
                $emptyCount++;

                $this->line(sprintf(
                    '[BACKFILL][READER] xRead 空返回（%d/%d），lastId=%s，继续等待',
                    $emptyCount,
                    $maxEmpty,
                    $lastId,
                ));

                if ($emptyCount >= $maxEmpty) {
                    $this->line('[BACKFILL][READER] 连续空返回，确认无数据，退出循环');
                    break;
                }

                continue;
            }

            // 有数据，重置空计数
            $emptyCount = 0;

            $items = $entries[$stream] ?? [];
            if (empty($items)) {
                continue;
            }

            foreach ($items as $id => $data) {
                $payload = (string) ($data['payload'] ?? '{}');
                $device = (string) ($data['device'] ?? 'unknown');
                $topic = (string) ($data['topic'] ?? '');
                $ts = (float) ($data['ts'] ?? microtime(true));

                if (strlen($payload) > $maxPayload) {
                    EdgeStream::writeDead(
                        (string) $id,
                        $stream,
                        '',
                        '',
                        1,
                        $payload,
                        'payload too large (backfill)',
                        $device,
                        $topic,
                        (int) ($ts * 1000),
                        date('Y-m-d H:i:s', (int) $ts),
                    );
                    $this->dead++;
                    $this->xDel($stream, [(string) $id]);
                    continue;
                }

                $this->received++;
                $this->writeChannel->push([
                    'stream_id'   => (string) $id,
                    'client_id'   => $device,
                    'topic'       => $topic,
                    'payload'     => $payload,
                    'received_ms' => (int) ($ts * 1000),
                    'received_at' => date('Y-m-d H:i:s', (int) $ts),
                ]);
            }

            // 推进游标：取最后一条 id
            $lastEntry = null;
            foreach ($items as $id => $data) {
                $lastEntry = $id;
            }
            $lastId = (string) $lastEntry;

            // 非阻塞模式下，返回数量小于 batch 才认为已读完
            if (self::BACKFILL_BLOCK_MS <= 0 && count($items) < $batchSize) {
                $this->line('[BACKFILL][READER] 非阻塞模式且数据不足一批，认为已读完');
                break;
            }
        }

        // ✅ 确保 Writer 一定能收到结束信号
        try {
            $this->writeChannel->push(null);
        } catch (Throwable $e) {
            $this->error('[BACKFILL][READER] push 结束信号失败：' . $e->getMessage());
        }

        $this->line('[BACKFILL][READER] 退出');
    }

    /**
     * Swow 回刷 writer：批量写 PG + XDEL
     */
    private function backfillWriterLoop(string $stream): void
    {
        $this->line('[BACKFILL][WRITER] 启动');

        $batchSize = max(1, (int) $this->option('batch'));
        $batch = [];

        // ---- 启动期：等 Reader 和 Redis 连接池就绪 ----
        $startupWaitMs = 10_000;   // 最多等 10s
        $pollIntervalMs = 100;     // 每 100ms 检查一次
        $waited = 0;
        $ready = false;

        $this->line('[BACKFILL][WRITER] 等待 Reader 就绪...');

        while ($waited < $startupWaitMs) {
            $len = 0;
            try {
                $len = $this->writeChannel->getLength();
            } catch (Throwable) {
                // channel 未就绪，继续等
            }

            if ($len > 0) {
                $ready = true;
                break;
            }

            // 非阻塞尝试 pop，避免死等
            try {
                $probe = $this->writeChannel->pop(0);
                if ($probe !== null) {
                    $batch[] = $probe;
                    $ready = true;
                    break;
                }
            } catch (Throwable) {
                // 无数据，继续等
            }

            $this->sleep($pollIntervalMs / 1000);
            $waited += $pollIntervalMs;
        }

        if (! $ready && empty($batch)) {
            $this->warn(sprintf(
                '[BACKFILL][WRITER] 启动期 %dms 内未收到数据，进入正常运行',
                $startupWaitMs,
            ));
        } else {
            $this->line('[BACKFILL][WRITER] Reader 已就绪，开始消费');
        }

        // ---- 正常运行：从 Channel 消费 ----
        while (true) {
            $item = null;

            try {
                $item = $this->writeChannel->pop(self::BACKFILL_BLOCK_MS);
            } catch (Throwable $e) {
                // 降级为 warn：启动期超时属正常抖动，不是错误
                $this->warn('[BACKFILL][WRITER] pop 超时（等待 Reader 数据）：' . $e->getMessage());
                $this->sleep(0.1);
                continue;
            }

            if ($item === null) {
                if ($batch !== []) {
                    $this->flushBackfillBatch($batch, $stream);
                    $batch = [];
                }
                break;
            }

            $batch[] = $item;

            if (count($batch) >= $batchSize) {
                $this->flushBackfillBatch($batch, $stream);
                $batch = [];
            }
        }

        // ✅ 通知 runBackfill 回刷结束
        $this->writeChannel = null;

        $this->line('[BACKFILL][WRITER] 退出');
    }

    private function flushBackfillBatch(array $batch, string $stream): void
    {
        if ($batch === []) {
            return;
        }

        $ids = array_map(fn ($r) => (string) $r['stream_id'], $batch);

        try {
            [$written] = EdgeStream::upsertBatch($batch);
            $this->written += $written;
            $this->xDel($stream, $ids);

            $this->line(sprintf(
                '[BACKFILL] batch=%d written=%d XDEL=%d',
                count($batch),
                $written,
                count($ids),
            ));
        } catch (Throwable $e) {
            $this->error('[BACKFILL] 写入失败：'.$e->getMessage());
            foreach ($batch as $row) {
                EdgeStream::writeDead(
                    (string) ($row['stream_id'] ?? ''), $stream, '', '', 1,
                    (string) ($row['payload'] ?? '{}'), $e->getMessage(),
                    (string) ($row['client_id'] ?? 'unknown'),
                    (string) ($row['topic'] ?? ''),
                    (int) ($row['received_ms'] ?? 0),
                    (string) ($row['received_at'] ?? ''),
                );
                $this->dead++;
            }
            $this->xDel($stream, $ids);
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

    private function ensureGroup(string $stream, string $group): void
    {
        try {
            withRedis(fn (\Redis $conn) => $conn->xGroup('CREATE', $stream, $group, '0', true));
        } catch (Throwable) {
            // BUSYGROUP 已存在，忽略
        }
    }

    /* ==================== 死信重试 ==================== */

    private function runRetryDead(): int
    {
        $stream = (string) $this->option('stream');
        $group = (string) $this->option('group');
        $consumer = (string) $this->option('consumer');
        $batchSize = max(1, (int) $this->option('retry-batch'));
        $maxRetries = max(1, (int) $this->option('max-retries'));
        $timeout = max(1, (int) $this->option('timeout'));

        $this->info(sprintf(
            '[RETRY-DEAD] stream=%s group=%s consumer=%s batch=%d maxRetries=%d timeout=%dms',
            $stream, $group, $consumer, $batchSize, $maxRetries, $timeout,
        ));

        $total = ['retried' => 0, 'succeeded' => 0, 'failed' => 0];

        while (true) {
            $claims = EdgeStreamFail::claimBatch($stream, $group, $batchSize, $maxRetries, $timeout);

            if ($claims === []) {
                break;
            }

            $stats = EdgeStream::retryBatch($claims, $maxRetries);

            $total['retried'] += $stats['retried'];
            $total['succeeded'] += $stats['succeeded'];
            $total['failed'] += $stats['failed'];

            $this->line(sprintf(
                '[RETRY-DEAD] retried=%d succeeded=%d failed=%d',
                $stats['retried'],
                $stats['succeeded'],
                $stats['failed'],
            ));
        }

        $this->info(sprintf(
            '[RETRY-DEAD] 完成 retried=%d succeeded=%d failed=%d',
            $total['retried'],
            $total['succeeded'],
            $total['failed'],
        ));

        return 0;
    }

    /* ==================== Swow 模式 ==================== */

    private function runSwowMode(): int
    {
        $stream = (string) $this->option('stream');
        $group = (string) $this->option('group');
        $consumer = (string) $this->option('consumer');
        $channelSize = max(1, (int) $this->option('channel-size'));

        $this->writeChannel = new Channel($channelSize);

        $this->ensureGroup($stream, $group);
        $this->recoverPending($stream, $group, $consumer);

        $this->line('[SWOW] 启动');

        Coroutine::run(function () use ($stream, $group, $consumer): void {
            if (PHP_OS_FAMILY !== 'Windows' && class_exists(\Swow\Signal::class)) {
                $this->registerSignals($stream, $group, $consumer);
            }

            Coroutine::run(fn () => $this->readerLoop($stream, $group, $consumer));
            Coroutine::run(fn () => $this->writerLoop($stream, $group, $consumer));

            while (! $this->stopping) {
                $this->sleep(0.1);
            }

            $this->drainChannel($this->writeChannel);
        });

        $this->printStats();

        return 0;
    }

    private function registerSignals(string $stream, string $group, string $consumer): void
    {
        foreach ([\Swow\Signal::INT, \Swow\Signal::TERM] as $sig) {
            Coroutine::run(function () use ($sig, $stream, $group, $consumer): void {
                try {
                    \Swow\Signal::wait($sig);
                } catch (Throwable) {
                    return;
                }

                if ($this->stopping) {
                    $this->error('[signal] 再次收到信号，强制退出');
                    exit(1);
                }

                $this->warn(sprintf('[signal] 收到 %d，优雅停机', $sig));
                $this->stopping = true;

                $this->handlePending($stream, $group, $consumer);
                EdgeStreamFail::forceUnlock($stream, $group);

                try {
                    $this->writeChannel?->push(null);
                } catch (Throwable) {
                    // channel 已关闭
                }
            });
        }
    }

    /* ==================== 同步模式 ==================== */

    private function runSyncMode(): int
    {
        $stream = (string) $this->option('stream');
        $group = (string) $this->option('group');
        $consumer = (string) $this->option('consumer');
        $batchSize = max(1, (int) $this->option('batch'));
        $timeout = max(1, (int) $this->option('timeout'));
        $maxPayload = max(1, (int) $this->option('max-payload'));
        $once = (bool) $this->option('once');

        $this->ensureGroup($stream, $group);
        $this->recoverPending($stream, $group, $consumer);

        $this->registerSyncSignalHandler($stream, $group);

        $this->info(sprintf(
            '同步模式 | stream=%s group=%s consumer=%s batch=%d timeout=%dms',
            $stream, $group, $consumer, $batchSize, $timeout,
        ));

        $buffer = [];
        $lastFlush = microtime(true);

        while (! $this->stopping) {
            try {
                $entries = withRedis(fn (\Redis $conn) => $conn->xReadGroup(
                    $group,
                    $consumer,
                    [$stream => '>'],
                    $batchSize,
                    $timeout,
                ));
            } catch (Throwable $e) {
                $this->error('[READ] xReadGroup 失败：'.$e->getMessage());
                if ($once) {
                    break;
                }
                $this->sleep(1.0);
                continue;
            }

            if (empty($entries)) {
                if (! empty($buffer) && (microtime(true) - $lastFlush) * 1000 >= $timeout) {
                    $this->flushBatchAndDelete($buffer, $stream, $group, $consumer);
                    $buffer = [];
                    $lastFlush = microtime(true);
                }
                if ($once) {
                    break;
                }
                continue;
            }

            $items = $entries[$stream] ?? [];

            foreach ($items as $id => $data) {
                $payload = (string) ($data['payload'] ?? '{}');
                $device = (string) ($data['device'] ?? 'unknown');
                $topic = (string) ($data['topic'] ?? '');
                $ts = (float) ($data['ts'] ?? microtime(true));

                if (strlen($payload) > $maxPayload) {
                    EdgeStream::writeDead(
                        (string) $id, $stream, $group, $consumer, 1,
                        $payload, 'payload too large',
                        $device, $topic,
                        (int) ($ts * 1000),
                        date('Y-m-d H:i:s', (int) $ts),
                    );
                    $this->dead++;
                    $this->xDel($stream, [(string) $id]);
                    continue;
                }

                $this->received++;
                $buffer[] = [
                    'stream_id'   => (string) $id,
                    'client_id'   => $device,
                    'topic'       => $topic,
                    'payload'     => $payload,
                    'received_ms' => (int) ($ts * 1000),
                    'received_at' => date('Y-m-d H:i:s', (int) $ts),
                ];
            }

            if (count($buffer) >= $batchSize) {
                $this->flushBatchAndDelete($buffer, $stream, $group, $consumer);
                $buffer = [];
                $lastFlush = microtime(true);
            }

            if ($once) {
                break;
            }
        }

        if (! empty($buffer)) {
            $this->flushBatchAndDelete($buffer, $stream, $group, $consumer);
        }

        $this->printStats();

        return 0;
    }

    private function registerSyncSignalHandler(string $stream, string $group): void
    {
        if (! function_exists('pcntl_signal')) {
            return;
        }

        pcntl_async_signals(true);

        $cleanup = function () use ($stream, $group) {
            $this->stopping = true;
            $this->handlePending($stream, $group, 'worker_1');
            EdgeStreamFail::forceUnlock($stream, $group);
            exit(0);
        };

        pcntl_signal(SIGINT, $cleanup);
        pcntl_signal(SIGTERM, $cleanup);
    }

    /* ==================== Reader / Writer ==================== */

    private function readerLoop(string $stream, string $group, string $consumer): void
    {
        $batchSize = max(1, (int) $this->option('batch'));
        $timeout = max(1, (int) $this->option('timeout'));
        $maxPayload = max(1, (int) $this->option('max-payload'));
        $once = (bool) $this->option('once');

        $this->line('[READER] 启动');
        $this->recoverPending($stream, $group, $consumer);

        while (! $this->stopping) {
            try {
                $entries = withRedis(fn (\Redis $conn) => $conn->xReadGroup(
                    $group, $consumer, [$stream => '>'], $batchSize, $timeout,
                ));
            } catch (Throwable $e) {
                $this->error('[READER] '.$e->getMessage());
                if ($once) {
                    break;
                }
                $this->sleep(1.0);
                continue;
            }

            if (empty($entries)) {
                if ($once) {
                    break;
                }
                continue;
            }

            $items = $entries[$stream] ?? [];

            foreach ($items as $id => $data) {
                $payload = (string) ($data['payload'] ?? '{}');
                $device = (string) ($data['device'] ?? 'unknown');
                $topic = (string) ($data['topic'] ?? '');
                $ts = (float) ($data['ts'] ?? microtime(true));

                if (strlen($payload) > $maxPayload) {
                    EdgeStream::writeDead(
                        (string) $id, $stream, $group, $consumer, 1,
                        $payload, 'payload too large',
                        $device, $topic,
                        (int) ($ts * 1000),
                        date('Y-m-d H:i:s', (int) $ts),
                    );
                    $this->dead++;
                    $this->xDel($stream, [(string) $id]);
                    continue;
                }

                $this->received++;
                $this->writeChannel->push([
                    'stream_id'   => (string) $id,
                    'client_id'   => $device,
                    'topic'       => $topic,
                    'payload'     => $payload,
                    'received_ms' => (int) ($ts * 1000),
                    'received_at' => date('Y-m-d H:i:s', (int) $ts),
                ]);
            }

            if ($once) {
                break;
            }
        }

        $this->writeChannel->push(null);
        $this->line('[READER] 退出');
    }

    private function writerLoop(string $stream, string $group, string $consumer): void
    {
        $batchSize = max(1, (int) $this->option('batch'));
        $timeout = max(1, (int) $this->option('timeout'));
        $batch = [];

        $this->line('[WRITER] 启动');

        while (true) {
            $item = null;
            try {
                $item = $this->writeChannel->pop($timeout);
            } catch (Throwable $e) {
                $this->error('[WRITER] pop 失败：'.$e->getMessage());
                $this->sleep(0.1);
                continue;
            }

            if ($item === null) {
                if ($batch !== []) {
                    $this->flushBatchAndDelete($batch, $stream, $group, $consumer);
                    $batch = [];
                }
                break;
            }

            $batch[] = $item;
            if (count($batch) >= $batchSize) {
                $this->flushBatchAndDelete($batch, $stream, $group, $consumer);
                $batch = [];
            }
        }

        $this->line('[WRITER] 退出');
    }

    /* ==================== 核心：写 PG + XDEL ==================== */

    private function flushBatchAndDelete(array $batch, string $stream, string $group, string $consumer): void
    {
        if ($batch === []) {
            return;
        }

        $validBatch = [];
        $invalidIds = [];

        foreach ($batch as $row) {
            $errors = EdgeStream::validateRow($row);
            if ($errors !== []) {
                EdgeStream::writeDead(
                    (string) ($row['stream_id'] ?? ''), $stream, $group, $consumer, 1,
                    (string) ($row['payload'] ?? '{}'), implode('; ', $errors),
                    (string) ($row['client_id'] ?? 'unknown'),
                    (string) ($row['topic'] ?? ''),
                    (int) ($row['received_ms'] ?? 0),
                    (string) ($row['received_at'] ?? ''),
                );
                $invalidIds[] = (string) ($row['stream_id'] ?? '');
                $this->dead++;
                continue;
            }

            $validBatch[] = $row;
        }

        if ($invalidIds !== []) {
            $this->xDel($stream, $invalidIds);
        }

        if ($validBatch === []) {
            return;
        }

        $ids = array_map(fn ($r) => (string) $r['stream_id'], $validBatch);

        try {
            [$written] = EdgeStream::upsertBatch($validBatch);
            $this->written += $written;
            $this->xDel($stream, $ids);

            $last = end($validBatch);
            if (! empty($last['stream_id'])) {
                EdgeStreamOffset::query()->updateOrInsert(
                    ['stream' => $stream, 'group_name' => $group],
                    [
                        'last_id' => (string) $last['stream_id'],
                        'last_lag' => $this->writeChannel?->getLength() ?? 0,
                        'updated_at' => now(),
                    ],
                );
            }
        } catch (Throwable $e) {
            $this->error('[FLUSH] 失败：'.$e->getMessage());
            foreach ($validBatch as $row) {
                EdgeStream::writeDead(
                    (string) ($row['stream_id'] ?? ''), $stream, $group, $consumer, 1,
                    (string) ($row['payload'] ?? '{}'), $e->getMessage(),
                    (string) ($row['client_id'] ?? 'unknown'),
                    (string) ($row['topic'] ?? ''),
                    (int) ($row['received_ms'] ?? 0),
                    (string) ($row['received_at'] ?? ''),
                );
                $this->dead++;
            }
            $this->xDel($stream, $ids);
        }
    }

    private function xDel(string $stream, array $ids): void
    {
        $ids = array_values(array_filter($ids, fn ($id) => $id !== ''));

        if ($ids === []) {
            return;
        }

        try {
            $deleted = withRedis(fn (\Redis $conn) => $conn->xDel($stream, $ids));
            $this->deleted += (int) $deleted;
        } catch (Throwable $e) {
            $this->error('[XDEL] 删除失败：'.$e->getMessage());
        }
    }

    /* ==================== 启动恢复 PEL 存量 ==================== */

    private function recoverPending(string $stream, string $group, string $consumer): void
    {
        $batchSize = max(1, (int) $this->option('batch'));
        $maxPayload = max(1, (int) $this->option('max-payload'));
        $maxDelivered = self::PEL_MAX_DELIVERED - 1;

        $this->line('[RECOVER] 检查 PEL 遗留消息...');

        $totalRecovered = 0;
        $totalDeleted = 0;

        while (true) {
            try {
                $pending = withRedis(fn (\Redis $conn) => $conn->xPending(
                    $stream, $group, '-', '+', $batchSize,
                ));
            } catch (Throwable $e) {
                $this->error('[RECOVER] xPending 失败：'.$e->getMessage());
                break;
            }

            if (empty($pending)) {
                break;
            }

            $claimIds = [];
            $skipIds = [];

            foreach ($pending as $p) {
                [$id, $owner, $idle, $delivered] = $p;
                if ((int) $delivered <= $maxDelivered) {
                    $claimIds[] = (string) $id;
                } else {
                    $skipIds[] = (string) $id;
                }
            }

            if ($skipIds !== []) {
                $this->xDel($stream, $skipIds);
                $totalDeleted += count($skipIds);
            }

            if ($claimIds === []) {
                if ($skipIds === []) {
                    break;
                }
                continue;
            }

            try {
                $claimed = withRedis(fn (\Redis $conn) => $conn->xClaim(
                    $stream, $group, $consumer,
                    max(1000, (int) $this->option('timeout')),
                    $claimIds,
                    ['RETRYCOUNT' => 0, 'FORCE' => true],
                ));
            } catch (Throwable $e) {
                $this->error('[RECOVER] XCLAIM 失败：'.$e->getMessage());
                break;
            }

            if (empty($claimed)) {
                break;
            }

            $buffer = [];
            $invalidIds = [];

            foreach ($claimed as $id => $data) {
                $payload = (string) ($data['payload'] ?? '{}');
                $device = (string) ($data['device'] ?? 'unknown');
                $topic = (string) ($data['topic'] ?? '');
                $ts = (float) ($data['ts'] ?? microtime(true));

                if (strlen($payload) > $maxPayload) {
                    EdgeStream::writeDead(
                        (string) $id, $stream, $group, $consumer, 1,
                        $payload, 'payload too large (recover)',
                        $device, $topic,
                        (int) ($ts * 1000),
                        date('Y-m-d H:i:s', (int) $ts),
                    );
                    $this->dead++;
                    $invalidIds[] = (string) $id;
                    continue;
                }

                $this->received++;
                $buffer[] = [
                    'stream_id'   => (string) $id,
                    'client_id'   => $device,
                    'topic'       => $topic,
                    'payload'     => $payload,
                    'received_ms' => (int) ($ts * 1000),
                    'received_at' => date('Y-m-d H:i:s', (int) $ts),
                ];
            }

            if ($invalidIds !== []) {
                $this->xDel($stream, $invalidIds);
                $totalDeleted += count($invalidIds);
            }

            if (! empty($buffer)) {
                $ids = array_map(fn ($r) => (string) $r['stream_id'], $buffer);

                try {
                    [$written] = EdgeStream::upsertBatch($buffer);
                    $this->written += $written;
                    $this->xDel($stream, $ids);
                    $totalDeleted += count($ids);
                    $totalRecovered += count($buffer);
                } catch (Throwable $e) {
                    $this->error('[RECOVER] 写入失败：'.$e->getMessage());
                    foreach ($buffer as $row) {
                        EdgeStream::writeDead(
                            (string) ($row['stream_id'] ?? ''), $stream, $group, $consumer, 1,
                            (string) ($row['payload'] ?? '{}'), $e->getMessage(),
                            (string) ($row['client_id'] ?? 'unknown'),
                            (string) ($row['topic'] ?? ''),
                            (int) ($row['received_ms'] ?? 0),
                            (string) ($row['received_at'] ?? ''),
                        );
                        $this->dead++;
                    }
                    $this->xDel($stream, $ids);
                    $totalDeleted += count($ids);
                }
            }

            if (count($pending) < $batchSize) {
                break;
            }
        }

        if ($totalRecovered > 0 || $totalDeleted > 0) {
            $this->line(sprintf(
                '[RECOVER] 恢复完成，处理 %d 条，XDEL %d 条',
                $totalRecovered,
                $totalDeleted,
            ));
        } else {
            $this->line('[RECOVER] 无遗留消息');
        }
    }

    /* ==================== 停机 PEL 清理 ==================== */

    private function handlePending(string $stream, string $group, string $consumer): void
    {
        try {
            $pending = withRedis(fn (\Redis $conn) => $conn->xPending(
                $stream, $group, '-', '+', self::PEL_BATCH,
            ));

            if (empty($pending)) {
                return;
            }

            $claimedIds = [];

            foreach ($pending as $p) {
                [$id, $owner, $idle, $delivered] = $p;

                if ((int) $delivered < self::PEL_MAX_DELIVERED) {
                    continue;
                }

                $claimed = withRedis(fn (\Redis $conn) => $conn->xClaim(
                    $stream, $group, $consumer,
                    max((int) $idle, 1000),
                    [(string) $id],
                    ['RETRYCOUNT' => 0, 'FORCE' => true],
                ));

                foreach ($claimed as $cid => $data) {
                    $payload = (string) ($data['payload'] ?? '{}');
                    $device = (string) ($data['device'] ?? 'unknown');
                    $topic = (string) ($data['topic'] ?? '');
                    $ts = (float) ($data['ts'] ?? microtime(true));

                    EdgeStream::writeDead(
                        (string) $cid, $stream, $group, $consumer, (int) $delivered,
                        $payload, 'PEL max delivered',
                        $device, $topic,
                        (int) ($ts * 1000),
                        date('Y-m-d H:i:s', (int) $ts),
                    );
                    $this->dead++;
                    $claimedIds[] = (string) $cid;
                }
            }

            if ($claimedIds !== []) {
                $this->xDel($stream, $claimedIds);
            }
        } catch (Throwable $e) {
            $this->error('[PEL] '.$e->getMessage());
        }
    }

    private function drainChannel(Channel $channel): void
    {
        $deadline = microtime(true) + (self::DRAIN_TIMEOUT_MS / 1000);
        $drained = 0;

        while (microtime(true) < $deadline) {
            try {
                $item = $channel->pop(self::DRAIN_POP_TIMEOUT_MS);
            } catch (Throwable) {
                break;
            }

            if ($item === null) {
                break;
            }

            $drained++;
        }

        if ($drained > 0) {
            $this->line(sprintf('[DRAIN] 排空残留 %d 条', $drained));
        }
    }

    private function printStats(): void
    {
        $this->line('========== 消费统计 ==========');
        $this->line('接收：'.$this->received);
        $this->line('写入：'.$this->written);
        $this->line('去重：'.$this->duplicated);
        $this->line('死信：'.$this->dead);
        $this->line('XDEL：'.$this->deleted);
        $this->line('==============================');
    }

    private function configInt(string $key, $default): int
    {
        $val = function_exists('config') ? config($key, $default) : $default;

        return (int) $val;
    }
}
