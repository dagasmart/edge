<?php

declare(strict_types=1);

namespace DagaSmart\Edge\Console\Commands;

use DagaSmart\Edge\Models\EdgeStream;
use DagaSmart\Edge\Models\EdgeStreamFail;
use DagaSmart\Edge\Models\EdgeStreamOffset;
use Illuminate\Console\Command;
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
 *
 * 传输通道：Swow 模式下使用 Redis List（edge:buffer:insert_redis）
 * 替代 Swow Channel，避免连接池协程上下文导致的数据丢失。
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

    /** Redis List 作为 Reader/Writer 之间的缓冲，避免 Swow Channel 跨协程上下文丢失 */
    private const string BUFFER_KEY = 'edge:buffer:insert_redis';

    private const int PEL_BATCH = 20;
    private const int PEL_MAX_DELIVERED = 3;
    private const int DRAIN_TIMEOUT_SEC = 3;
    private const int BACKFILL_BLOCK_MS = 2000;

    private bool $stopping = false;
    private int $received = 0;
    private int $written = 0;
    private int $duplicated = 0;
    private int $dead = 0;
    private int $deleted = 0;

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

        // ✅ 修复：Swow 模式下 initRedisPool 已由 runSwowMode() 负责，
        // 这里不再重复调用，避免日志里出现两次"连接池已初始化"。
        // 同步模式不需要连接池，所以也不在这里初始化。

        return $driver === 'swow'
            ? $this->runSwowMode()
            : $this->runSyncMode();
    }

    /* ==================== Swow 模式 ==================== */

    private function runSwowMode(): int
    {
        $stream = (string) $this->option('stream');
        $group = (string) $this->option('group');
        $consumer = (string) $this->option('consumer');

        // ✅ 连接池只在这里初始化一次
        $this->initRedisPool();
        $this->ensureGroup($stream, $group);
        $this->recoverPending($stream, $group, $consumer);

        // 清空上次异常遗留的 buffer，避免脏数据
        $this->clearBuffer();

        $this->line('[SWOW] 启动');

        if (PHP_OS_FAMILY !== 'Windows' && class_exists(\Swow\Signal::class)) {
            $this->registerSignals($stream, $group, $consumer);
        }

        Coroutine::run(fn () => $this->readerLoop($stream, $group, $consumer));
        Coroutine::run(fn () => $this->writerLoop($stream, $group, $consumer));

        // 主协程阻塞，保活事件循环
        while (! $this->stopping) {
            $this->sleep(0.1);
        }

        // ✅ 信号只负责设置 $stopping=true，排空统一在这里做
        $this->drainBuffer($stream, $group, $consumer);
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

                // ✅ 只设置停止标志，通知 Reader/Writer 退出
                // 排空和统计由主协程 runSwowMode() 统一处理，不在这里做
                $this->stopping = true;

                // PEL 清理和死信锁释放可以提前做，不影响后续排空
                $this->handlePending($stream, $group, $consumer);
                EdgeStreamFail::forceUnlock($stream, $group);
            });
        }
    }

    /* ==================== Reader / Writer（Redis List 通道） ==================== */

    private function readerLoop(string $stream, string $group, string $consumer): void
    {
        $batchSize = max(1, (int) $this->option('batch'));
        $timeout = max(1, (int) $this->option('timeout'));
        $maxPayload = max(1, (int) $this->option('max-payload'));
        $once = (bool) $this->option('once');

        $this->line('[READER] 启动，开始 xReadGroup 循环');
        $this->recoverPending($stream, $group, $consumer);

        $loopCount = 0;

        while (! $this->stopping) {
            $loopCount++;

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
                if ($loopCount % 10 === 1) {
                    $this->line(sprintf(
                        '[READER] 等待数据中（第 %d 次 xReadGroup）',
                        $loopCount,
                    ));
                }
                if ($once) {
                    break;
                }
                continue;
            }

            $items = $entries[$stream] ?? [];
            if ($items !== []) {
                $this->line(sprintf('[READER] 收到 %d 条数据', count($items)));
            }

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

                $item = [
                    'stream_id'   => (string) $id,
                    'client_id'   => $device,
                    'topic'       => $topic,
                    'payload'     => $payload,
                    'received_ms' => (int) ($ts * 1000),
                    'received_at' => date('Y-m-d H:i:s', (int) $ts),
                ];

                // ✅ 写入 Redis List，由 Writer 消费
                try {
                    withRedis(function (\Redis $conn) use ($item): void {
                        $conn->lPush(self::BUFFER_KEY, json_encode(
                            $item,
                            JSON_UNESCAPED_SLASHES | JSON_UNESCAPED_UNICODE,
                        ));
                    });
                } catch (Throwable $e) {
                    $this->error('[READER] lPush 失败：'.$e->getMessage());
                }
            }

            if ($once) {
                break;
            }
        }

        $this->line('[READER] 退出');
    }

    private function writerLoop(string $stream, string $group, string $consumer): void
    {
        $batchSize = max(1, (int) $this->option('batch'));
        $timeout = max(1, (int) $this->option('timeout'));
        $batch = [];

        $this->line('[WRITER] 启动');

        while (! $this->stopping) {
            try {
                // bRPop 超时单位是秒
                $raw = withRedis(fn (\Redis $conn) => $conn->bRPop(
                    [self::BUFFER_KEY],
                    max(1, (int) ($timeout / 1000)),
                ));
            } catch (Throwable $e) {
                $this->warn('[WRITER] bRPop 异常：'.$e->getMessage());
                $this->sleep(0.5);
                continue;
            }

            if (empty($raw)) {
                // 超时无数据，刷一次残留
                if (! empty($batch)) {
                    $this->flushBatchAndDelete($batch, $stream, $group, $consumer);
                    $batch = [];
                }
                continue;
            }

            $item = json_decode($raw[1], true);
            if ($item === null) {
                continue;
            }

            $batch[] = $item;

            if (count($batch) >= $batchSize) {
                $this->flushBatchAndDelete($batch, $stream, $group, $consumer);
                $batch = [];
            }
        }

        // ✅ stopping=true 后，把最后一批残留数据刷完
        if (! empty($batch)) {
            $this->flushBatchAndDelete($batch, $stream, $group, $consumer);
        }

        $this->line('[WRITER] 退出');
    }

    /* ==================== Buffer 管理 ==================== */

    private function clearBuffer(): void
    {
        try {
            withRedis(fn (\Redis $conn) => $conn->del([self::BUFFER_KEY]));
        } catch (Throwable) {
            // 忽略
        }
    }

    private function drainBuffer(string $stream, string $group, string $consumer): void
    {
        $this->line('[SWOW] 停机排空 Buffer...');

        $drained = 0;
        $emptyCount = 0;

        // ✅ 修复：while(true) 改为 while(!$this->stopping)，
        // 防止信号触发后仍然无限阻塞在 bRPop 上导致进程卡死
        while (! $this->stopping) {
            try {
                $raw = withRedis(fn (\Redis $conn) => $conn->bRPop(
                    [self::BUFFER_KEY],
                    1, // 最多阻塞 1 秒，定期回到事件循环检查 stopping
                ));
            } catch (Throwable) {
                break;
            }

            if (empty($raw)) {
                $emptyCount++;
                // ✅ 连续 2 次空返回（约 2 秒）说明 buffer 已空，直接退出
                if ($emptyCount >= 2) {
                    break;
                }
                continue;
            }

            $emptyCount = 0;
            $item = json_decode($raw[1], true);
            if ($item) {
                $this->flushBatchAndDelete([$item], $stream, $group, $consumer);
                $drained++;
            }
        }

        if ($drained > 0) {
            $this->line(sprintf('[SWOW] Buffer 排空完成，处理 %d 条', $drained));
        } else {
            $this->line('[SWOW] Buffer 已空，无需排空');
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
                    $group, $consumer, [$stream => '>'], $batchSize, $timeout,
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
            $this->backfillSwow($stream, $batchSize, $maxPayload);
        } else {
            $this->backfillSync($stream, $batchSize, $maxPayload);
        }

        $this->printStats();

        return 0;
    }

    private function backfillSwow(string $stream, int $batchSize, int $maxPayload): void
    {
        $emptyCount = 0;
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
                $emptyCount++;
                if ($emptyCount >= 3) {
                    break;
                }
                continue;
            }

            $emptyCount = 0;
            $items = $entries[$stream] ?? [];

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
                    $this->dead++;
                    $this->xDel($stream, [(string) $id]);
                    continue;
                }

                $this->received++;
                $this->flushBackfillItem(
                    $stream,
                    (string) $id,
                    $device,
                    $topic,
                    $payload,
                    (int) ($ts * 1000),
                    date('Y-m-d H:i:s', (int) $ts),
                );
            }

            $lastEntry = null;
            foreach ($items as $id => $data) {
                $lastEntry = $id;
            }
            $lastId = (string) $lastEntry;

            if (count($items) < $batchSize) {
                break;
            }
        }

        $this->line('[BACKFILL] Swow 回刷结束');
    }

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

            $lastEntry = null;
            foreach ($items as $id => $data) {
                $lastEntry = $id;
            }
            $lastId = (string) $lastEntry;

            if (count($items) < $batchSize) {
                break;
            }
        }

        $this->line('[BACKFILL] 同步回刷结束');
    }

    private function flushBackfillItem(
        string $stream,
        string $id,
        string $device,
        string $topic,
        string $payload,
        int $receivedMs,
        string $receivedAt,
    ): void {
        try {
            [$written] = EdgeStream::upsertBatch([[
                'stream_id'   => $id,
                'client_id'   => $device,
                'topic'       => $topic,
                'payload'     => $payload,
                'received_ms' => $receivedMs,
                'received_at' => $receivedAt,
            ]]);
            $this->written += $written;
            $this->xDel($stream, [$id]);
        } catch (Throwable $e) {
            EdgeStream::writeDead(
                $id, $stream, '', '', 1,
                $payload, $e->getMessage(),
                $device, $topic, $receivedMs, $receivedAt,
            );
            $this->dead++;
            $this->xDel($stream, [$id]);
        }
    }

    /* ==================== Redis ==================== */

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

            $this->line(sprintf(
                '[FLUSH] batch=%d written=%d XDEL=%d',
                count($validBatch),
                $written,
                count($ids),
            ));

            $last = end($validBatch);
            if (! empty($last['stream_id'])) {
                EdgeStreamOffset::query()->updateOrInsert(
                    ['stream' => $stream, 'group_name' => $group],
                    [
                        'last_id' => (string) $last['stream_id'],
                        'last_lag' => 0,
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

    /* ==================== PEL 恢复 ==================== */

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
                [, , , $delivered] = $p;
                if ((int) $delivered <= $maxDelivered) {
                    $claimIds[] = (string) $p[0];
                } else {
                    $skipIds[] = (string) $p[0];
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
                [$id, , $idle, $delivered] = $p;

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

    /* ==================== 统计 ==================== */

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
