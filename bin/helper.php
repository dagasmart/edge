<?php

declare(strict_types=1);

use Swow\Channel;
use Swow\Coroutine;
use Swow\Psr7\Server\ServerConnection;

/* =========================================================
 * Redis 分片连接池（Swow Channel 真协程池）
 *
 * 环境变量：EDGE_REDIS_SHARDS=host:port[:password][@db],...
 * 未配置时退化为单实例（与原逻辑完全一致）
 * ========================================================= */

/** @var array<int, Channel>|null $redisShardPools */
$redisShardPools = null;

/** @var array<int, array{host:string,port:int,password:string,db:int}>|null $redisShardConfigs */
$redisShardConfigs = null;

$redisPoolSize = 16;
$redisDb = 2;
$redisPassword = '';
$redisHost = '127.0.0.1';
$redisPort = 6379;

/**
 * 解析 EDGE_REDIS_SHARDS 环境变量
 *
 * 格式：host:port[:[password]][@db]
 * - host: IPv4 或域名（不含冒号，IPv6 暂不支持）
 * - password 允许为空（:: 或 :@db 或无密码段）
 *
 * @return array<int, array{host:string,port:int,password:string,db:int}>
 */
function parseRedisShardList(): array
{
    $raw = (string) getenv('EDGE_REDIS_SHARDS');
    if ($raw === '') {
        return [];
    }

    $result = [];
    foreach (explode(',', $raw) as $part) {
        $part = trim($part);
        if ($part === '') {
            continue;
        }

        // host:port[:[password]][@db]
        // 密码部分 (?:::?) 吃掉一个或两个冒号，支持 ::password / :password / 空密码
        if (! preg_match('/^([^:]+):(\d+)(?:::?(.+?))?(?:@(\d+))?$/', $part, $m)) {
            fwrite(STDERR, '[redis] invalid shard config: ' . $part . PHP_EOL);
            continue;
        }

        $result[] = [
            'host'     => $m[1],
            'port'     => (int) $m[2],
            'password' => $m[3] ?? '',
            'db'       => isset($m[4]) && $m[4] !== '' ? (int) $m[4] : $GLOBALS['redisDb'],
        ];
    }

    return $result;
}

/** @var array<int,int> $redisShardFillFailures 后台填充连续失败计数（熔断用） */
$redisShardFillFailures = [];

function redisPoolInit(int $size = 256, int $db = 2): void
{
    global $redisShardPools, $redisShardConfigs,
           $redisPoolSize, $redisDb, $redisPassword, $redisHost, $redisPort,
           $redisShardFillFailures;

    $redisPoolSize = $size;
    $redisDb = $db;
    $redisShardFillFailures = [];

    if (defined('REDIS_PASSWORD')) {
        $redisPassword = REDIS_PASSWORD;
    }
    if (defined('REDIS_HOST')) {
        $redisHost = REDIS_HOST;
    }
    if (defined('REDIS_PORT')) {
        $redisPort = (int) REDIS_PORT;
    }

    $shards = parseRedisShardList();

    // ---- 单实例退化模式（与原逻辑完全一致）----
    if ($shards === []) {
        $redisShardPools = null;
        $redisShardConfigs = null;

        $pool = new Channel($size);
        $probeSize = min(3, $size);
        $ok = 0;

        for ($i = 0; $i < $probeSize; $i++) {
            $redis = new Redis;
            try {
                $redis->connect($redisHost, $redisPort, 2.0);
                if ($redisPassword !== '') {
                    $authOk = $redis->auth($redisPassword);
                    if (! $authOk) {
                        fwrite(STDERR, '[redis] AUTH FAILED for ' . $redisHost . ':' . $redisPort . ' (password length=' . strlen($redisPassword) . ')' . PHP_EOL);
                        continue;
                    }
                }
                if ($redisDb > 0) {
                    $selectOk = $redis->select($redisDb);
                    if (! $selectOk) {
                        fwrite(STDERR, '[redis] SELECT DB ' . $redisDb . ' FAILED' . PHP_EOL);
                        continue;
                    }
                }
                $pool->push($redis);
                $ok++;
            } catch (Throwable $e) {
                fwrite(STDERR, '[redis] probe ' . $i . ' failed: ' . $e->getMessage() . PHP_EOL);
            }
        }

        fwrite(STDOUT, "[EdgeAuth] Redis pool probed: {$ok}/{$probeSize}" . PHP_EOL);

        if ($ok > 0 && $size > $probeSize) {
            $remaining = $size - $probeSize;
            $fillHost = $redisHost;
            $fillPort = $redisPort;
            $fillPassword = $redisPassword;
            $fillDb = $redisDb;

            Coroutine::run(static function () use ($remaining, $fillHost, $fillPort, $fillPassword, $fillDb): void {
                global $redisShardFillFailures;
                $filled = 0;
                $maxFailures = 50;

                for ($i = 0; $i < $remaining; $i++) {
                    $failures = $redisShardFillFailures[0] ?? 0;
                    if ($failures >= $maxFailures) {
                        fwrite(STDERR, '[redis] single-mode background fill STOPPED (consecutive failures=' . $failures . ')' . PHP_EOL);
                        break;
                    }

                    try {
                        $redis = new Redis;
                        $redis->connect($fillHost, $fillPort, 2.0);
                        if ($fillPassword !== '') {
                            $redis->auth($fillPassword);
                        }
                        if ($fillDb > 0) {
                            $redis->select($fillDb);
                        }
                        if (isset($GLOBALS['redisShardPools'][0])) {
                            $GLOBALS['redisShardPools'][0]->push($redis);
                        }
                        $redisShardFillFailures[0] = 0;
                        $filled++;
                    } catch (Throwable $e) {
                        $redisShardFillFailures[0] = $failures + 1;
                        fwrite(STDERR, '[redis] background fill failed: ' . $e->getMessage() . ' (consecutive=' . ($failures + 1) . ')' . PHP_EOL);
                    }
                    usleep(10000);
                }
                fwrite(STDOUT, "[EdgeAuth] Redis pool background filled: {$filled}/{$remaining}" . PHP_EOL);
            });
        }

        $redisShardPools = [0 => $pool];
        $redisShardConfigs = [0 => [
            'host'     => $redisHost,
            'port'     => $redisPort,
            'password' => $redisPassword,
            'db'       => $redisDb,
        ]];

        if ($ok === 0) {
            fwrite(STDERR, '[EdgeAuth] Redis pool: ALL PROBES FAILED, pool is empty!' . PHP_EOL);
        }

        return;
    }

    // ---- 分片模式 ----
    $redisShardConfigs = $shards;
    $redisShardPools = [];

    foreach ($shards as $index => $cfg) {
        $pool = new Channel($size);
        $probeSize = min(3, $size);
        $ok = 0;

        for ($i = 0; $i < $probeSize; $i++) {
            try {
                $redis = new Redis;
                $redis->connect($cfg['host'], $cfg['port'], 2.0);
                if ($cfg['password'] !== '') {
                    $redis->auth($cfg['password']);
                }
                if ($cfg['db'] > 0) {
                    $redis->select($cfg['db']);
                }
                $pool->push($redis);
                $ok++;
            } catch (Throwable $e) {
                fwrite(STDERR, '[redis] shard=' . $index . ' probe ' . $i . ' failed: ' . $e->getMessage() . PHP_EOL);
            }
        }

        fwrite(STDOUT, sprintf(
            '[EdgeAuth] Redis shard=%d probed=%d/%d (%s:%d/%d)' . PHP_EOL,
            $index, $ok, $probeSize, $cfg['host'], $cfg['port'], $cfg['db']
        ));

        if ($ok > 0 && $size > $probeSize) {
            $remaining = $size - $probeSize;
            $shardIndex = $index;
            $shardCfg = $cfg;

            Coroutine::run(static function () use ($remaining, $pool, $shardCfg, $shardIndex): void {
                global $redisShardFillFailures;
                $filled = 0;
                $maxFailures = 50;

                for ($i = 0; $i < $remaining; $i++) {
                    $failures = $redisShardFillFailures[$shardIndex] ?? 0;
                    if ($failures >= $maxFailures) {
                        fwrite(STDERR,
                            '[redis] shard=' . $shardIndex
                            . ' background fill STOPPED (consecutive failures=' . $failures . ')' . PHP_EOL
                        );
                        break;
                    }

                    try {
                        $redis = new Redis;
                        $redis->connect($shardCfg['host'], $shardCfg['port'], 2.0);
                        if ($shardCfg['password'] !== '') {
                            $redis->auth($shardCfg['password']);
                        }
                        if ($shardCfg['db'] > 0) {
                            $redis->select($shardCfg['db']);
                        }
                        $pool->push($redis);
                        $redisShardFillFailures[$shardIndex] = 0;
                        $filled++;
                    } catch (Throwable $e) {
                        $redisShardFillFailures[$shardIndex] = $failures + 1;
                        fwrite(STDERR,
                            '[redis] shard=' . $shardIndex
                            . ' background fill failed: ' . $e->getMessage()
                            . ' (consecutive=' . ($failures + 1) . ')' . PHP_EOL
                        );
                    }
                    usleep(10000);
                }

                fwrite(STDOUT,
                    '[EdgeAuth] Redis shard=' . $shardIndex
                    . ' background filled: ' . $filled . '/' . $remaining . PHP_EOL
                );
            });
        }

        $redisShardPools[$index] = $pool;
    }

    fwrite(STDOUT, '[EdgeAuth] Redis sharded mode: ' . count($shards) . ' shards' . PHP_EOL);
}

/**
 * 根据 key 计算 shard 索引
 */
function redisShardIndex(string $key): int
{
    global $redisShardConfigs;

    if ($redisShardConfigs === null || count($redisShardConfigs) <= 1) {
        return 0;
    }

    if (preg_match('#^(edge:(?:metrics|config|global):)#', $key)) {
        return 0;
    }

    $routeBy = redisExtractRouteValue($key);
    if ($routeBy === '') {
        return 0;
    }

    $shardCount = count($redisShardConfigs);
    return (crc32($routeBy) & 0x7FFFFFFF) % $shardCount;
}

function redisExtractRouteValue(string $key): string
{
    if (preg_match('#^device:secret:([^:]+):#', $key, $m)) {
        return $m[1];
    }
    if (preg_match('#^auth:online:(.+)$#', $key, $m)) {
        return $m[1];
    }
    if (preg_match('#^(?:fail|block|rate):(.+)$#', $key, $m)) {
        return $m[1];
    }
    if (preg_match('#^device:secret:(.+)$#', $key, $m)) {
        return $m[1];
    }

    return '';
}

function redisPoolPop(float $timeout = 0.3, string $key = 'edge:global:legacy'): ?Redis
{
    global $redisShardPools;

    if ($redisShardPools === null) {
        redisPoolInit();
    }

    $index = redisShardIndex($key);
    $pool = $redisShardPools[$index] ?? null;
    if ($pool === null) {
        return null;
    }

    try {
        $ms = max(1, (int) ($timeout * 1000));
        return $pool->pop($ms);
    } catch (Throwable) {
        return null;
    }
}

function redisPoolPush(Redis $conn, string $key = 'edge:global:legacy'): void
{
    global $redisShardPools, $redisShardConfigs;

    if ($redisShardPools === null) {
        return;
    }

    $index = redisShardIndex($key);
    $pool = $redisShardPools[$index] ?? null;
    if ($pool === null) {
        return;
    }

    $cfg = $redisShardConfigs[$index] ?? null;

    try {
        $conn->ping();
        $pool->push($conn);
        return;
    } catch (Throwable) {
        // dead, try recreate once
    }

    if ($cfg !== null) {
        try {
            $new = new Redis;
            $new->connect($cfg['host'], $cfg['port'], 2.0);
            if ($cfg['password'] !== '') {
                $new->auth($cfg['password']);
            }
            if ($cfg['db'] > 0) {
                $new->select($cfg['db']);
            }
            $pool->push($new);
            return;
        } catch (Throwable $e2) {
            fwrite(STDERR, '[redis] shard=' . $index . ' replace failed: ' . $e2->getMessage() . PHP_EOL);
            metricsInc('redis_pool_missing_slot');
        }
    }

    // 不再保 slot：塞可能已死的占位连接比丢一个 slot 更危险
    // 上层 pop 超时后会走 PG fallback，池容量是软限制
}

/**
 * 按 key 路由到对应 shard 执行 Redis 操作
 */
function withRedisForKey(string $key, callable $fn, float $timeout = 0.3): mixed
{
    $conn = redisPoolPop($timeout, $key);
    if ($conn === null) {
        throw new RuntimeException('Redis shard exhausted (key=' . $key . ')');
    }
    try {
        return $fn($conn);
    } finally {
        redisPoolPush($conn, $key);
    }
}

/**
 * @deprecated 无 key 的全局操作使用 withRedisForKey() 明确指定
 * 保留为 shard-0 兼容别名
 */
function withRedis(callable $fn): mixed
{
    return withRedisForKey('edge:global:legacy', $fn);
}

/* =========================================================
 * 业务 Redis 操作
 * ========================================================= */

function redisGet(string $key): mixed
{
    return withRedisForKey($key, static function (Redis $conn) use ($key): mixed {
        $val = $conn->get($key);
        return $val !== false ? (string) $val : null;
    });
}

function redisSetex(string $key, int $ttl, string $value): bool
{
    return withRedisForKey($key, static function (Redis $conn) use ($key, $ttl, $value): bool {
        return (bool) $conn->setex($key, $ttl, $value);
    });
}

function redisExists(string $key): bool
{
    return withRedisForKey($key, static function (Redis $conn) use ($key): bool {
        return (bool) $conn->exists($key);
    });
}

function redisDel(string ...$keys): int
{
    $deleted = 0;
    foreach ($keys as $key) {
        try {
            $deleted += withRedisForKey($key, static function (Redis $conn) use ($key): int {
                return $conn->del($key);
            });
        } catch (Throwable) {
            // 单个失败不影响其他
        }
    }
    return $deleted;
}

function redisSetNxEx(string $key, string $value, int $ttl): bool
{
    return withRedisForKey($key, static function (Redis $conn) use ($key, $value, $ttl): bool {
        return (bool) $conn->set($key, $value, ['NX', 'EX' => $ttl]);
    });
}

/* =========================================================
 * L1 本地缓存
 * ========================================================= */

/** @var ArrayObject|null $localCache */
$localCache = null;

function localCacheInit(): void
{
    global $localCache;
    $localCache = new ArrayObject;
}

function localCacheGet(string $key): mixed
{
    global $localCache;
    if ($localCache === null || ! isset($localCache[$key])) {
        return null;
    }
    $entry = $localCache[$key];
    if (($entry['expires'] ?? 0) < time()) {
        unset($localCache[$key]);

        return null;
    }

    return $entry['value'];
}

function localCacheSet(string $key, mixed $value, int $ttl): void
{
    global $localCache;
    if ($localCache === null) {
        return;
    }
    if ($localCache->count() >= 10000) {
        return;
    }
    $localCache[$key] = [
        'value' => $value,
        'expires' => time() + $ttl,
    ];
}

function localCacheStats(): int
{
    global $localCache;

    return $localCache !== null ? $localCache->count() : 0;
}

/* =========================================================
 * 连接池状态
 * ========================================================= */

function redisPoolStats(): array
{
    global $redisShardPools, $redisPoolSize, $redisShardConfigs;

    if ($redisShardPools === null) {
        return ['mode' => 'uninitialized', 'shards' => []];
    }

    $shards = [];
    $mode = ($redisShardConfigs !== null && count($redisShardConfigs) > 1) ? 'sharded' : 'single';

    foreach ($redisShardPools as $index => $pool) {
        try {
            $free = $pool->getLength();
        } catch (Throwable) {
            $free = 0;
        }
        $cfg = $redisShardConfigs[$index] ?? null;
        $shards[$index] = [
            'free'      => $free,
            'capacity'  => $redisPoolSize,
            'config'    => $cfg,
        ];
    }

    return [
        'mode'        => $mode,
        'shard_count' => count($redisShardPools),
        'shards'     => $shards,
    ];
}

function redisPoolIsReady(float $minRatio = 0.8): bool
{
    global $redisShardPools, $redisPoolSize;

    if ($redisShardPools === null || $redisPoolSize <= 0) {
        return false;
    }

    $totalFree = 0;
    $totalCap = 0;

    foreach ($redisShardPools as $pool) {
        try {
            $totalFree += $pool->getLength();
        } catch (Throwable) {
            // ignore
        }
        $totalCap += $redisPoolSize;
    }

    if ($totalCap <= 0) {
        return false;
    }

    return ($totalFree / $totalCap) >= $minRatio;
}

function redisPoolWaitReady(int $timeoutSec = 10, float $minRatio = 0.8): bool
{
    $deadline = time() + $timeoutSec;
    while (time() < $deadline) {
        if (redisPoolIsReady($minRatio)) {
            return true;
        }
        usleep(200000);
    }

    return redisPoolIsReady($minRatio);
}

/* =========================================================
 * 并发控制 —— Swow\Channel 信号量
 * ========================================================= */

$globalConcurrency = null;
$concurrencyMax = 10000;

function concurrencyInit(int $max): void
{
    global $globalConcurrency, $concurrencyMax;
    $concurrencyMax = $max;
    $globalConcurrency = new Channel($max);
    for ($i = 0; $i < $max; $i++) {
        $globalConcurrency->push(1, 1);
    }
    fwrite(STDOUT, '[EdgeAuth] Concurrency: Swow\Channel semaphore (max=' . $max . ')' . PHP_EOL);
}

function concurrencyProgressiveExpand(int $targetSize, int $steps = 10, int $stepMs = 300): void
{
    global $globalConcurrency, $concurrencyMax;

    $current = $concurrencyMax;
    if ($current >= $targetSize) {
        return;
    }

    $toAdd = $targetSize - $current;
    $perStep = max(1, (int) ceil($toAdd / $steps));

    Coroutine::run(static function () use ($perStep, $toAdd, $targetSize, $steps, $stepMs): void {
        global $globalConcurrency, $concurrencyMax;

        $added = 0;
        for ($step = 0; $step < $steps; $step++) {
            usleep($stepMs * 1000);

            $batch = min($perStep, $toAdd - $added);
            for ($i = 0; $i < $batch; $i++) {
                try {
                    $globalConcurrency->push(1, 1);
                    $added++;
                    $concurrencyMax++;
                } catch (Throwable) {
                    return;
                }
            }

            if ($added >= $toAdd) {
                break;
            }
        }

        fwrite(STDOUT, "[EdgeAuth] Concurrency fully opened: {$targetSize}" . PHP_EOL);
    });
}

function concurrencyAcquire(float $timeout = 2.0): bool
{
    global $globalConcurrency;
    if ($globalConcurrency === null) {
        return true;
    }
    try {
        $ms = (int) ($timeout * 1000);

        return (bool) $globalConcurrency->pop($ms > 0 ? $ms : 1);
    } catch (Throwable) {
        return false;
    }
}

function concurrencyRelease(): void
{
    global $globalConcurrency;
    if ($globalConcurrency === null) {
        return;
    }
    try {
        $globalConcurrency->push(1, 1);
    } catch (Throwable) {
        // 满了忽略
    }
}

function concurrencyStats(): array
{
    global $globalConcurrency, $concurrencyMax;
    if ($globalConcurrency === null) {
        return ['max' => $concurrencyMax, 'used' => 0, 'free' => $concurrencyMax];
    }
    try {
        $free = $globalConcurrency->getLength();
    } catch (Throwable) {
        $free = 0;
    }

    return [
        'max'  => $concurrencyMax,
        'used' => $concurrencyMax - $free,
        'free' => $free,
    ];
}

/* =========================================================
 * 设备级并发分桶限流
 * ========================================================= */

$__edge_device_semaphores = [];
$__edge_device_sem_max = 5;
$__edge_device_sem_ttl = 300;
$__edge_device_last_cleanup = 0;

function deviceConcurrencyInit(int $perDevice, int $cleanupTtl = 300): void
{
    global $__edge_device_sem_max, $__edge_device_sem_ttl;
    $__edge_device_sem_max = $perDevice;
    $__edge_device_sem_ttl = $cleanupTtl;
}

function deviceConcurrencyAcquire(string $clientId): ?string
{
    global $__edge_device_semaphores, $__edge_device_sem_max;
    if (! isset($__edge_device_semaphores[$clientId])) {
        try {
            $ch = new Channel($__edge_device_sem_max);
            for ($i = 0; $i < $__edge_device_sem_max; $i++) {
                $ch->push(1, 1);
            }
        } catch (Throwable $e) {
            fwrite(STDERR, '[device-acquire] channel create failed: ' . $e->getMessage() . PHP_EOL);

            return null;
        }
        $__edge_device_semaphores[$clientId] = [
            'channel'   => $ch,
            'last_used' => time(),
        ];
    }
    try {
        $bucket = $__edge_device_semaphores[$clientId];
        if (! isset($bucket['channel']) || ! $bucket['channel'] instanceof Channel) {
            unset($__edge_device_semaphores[$clientId]);

            return null;
        }
        $got = $bucket['channel']->pop(1);

        return $got ? $clientId : null;
    } catch (Throwable) {
        return null;
    }
}

function deviceConcurrencyRelease(?string $clientId): void
{
    global $__edge_device_semaphores;
    if ($clientId === null || ! isset($__edge_device_semaphores[$clientId])) {
        return;
    }
    try {
        $bucket = $__edge_device_semaphores[$clientId];
        if (! isset($bucket['channel']) || ! $bucket['channel'] instanceof Channel) {
            unset($__edge_device_semaphores[$clientId]);

            return;
        }
        $bucket['channel']->push(1, 1);
        $__edge_device_semaphores[$clientId]['last_used'] = time();
    } catch (Throwable) {
        // ignore
    }
}

function deviceConcurrencyCleanup(): void
{
    global $__edge_device_semaphores, $__edge_device_sem_ttl, $__edge_device_last_cleanup;
    $now = time();
    if (! is_array($__edge_device_semaphores)) {
        return;
    }
    if ($now - $__edge_device_last_cleanup < 5) {
        return;
    }
    $__edge_device_last_cleanup = $now;
    $snapshot = $__edge_device_semaphores;
    $removed = 0;
    foreach ($snapshot as $cid => $bucket) {
        if (! isset($bucket['last_used']) || ($now - $bucket['last_used']) > $__edge_device_sem_ttl) {
            unset($__edge_device_semaphores[$cid]);
            $removed++;
        }
    }
    if ($removed > 0) {
        debugLog('deviceConcurrencyCleanup: removed ' . $removed . ' idle buckets');
    }
}

function deviceConcurrencyStats(): array
{
    global $__edge_device_semaphores, $__edge_device_sem_max;
    if (! is_array($__edge_device_semaphores)) {
        return [];
    }
    $stats = [];
    $snapshot = $__edge_device_semaphores;
    foreach ($snapshot as $cid => $bucket) {
        if (! isset($bucket['channel']) || ! $bucket['channel'] instanceof Channel) {
            continue;
        }
        try {
            $free = $bucket['channel']->getLength();
        } catch (Throwable) {
            $free = 0;
        }
        $stats[$cid] = [
            'max'  => $__edge_device_sem_max,
            'used' => $__edge_device_sem_max - $free,
            'free' => $free,
        ];
    }

    return $stats;
}

/* =========================================================
 * 客户端失败熔断
 * ========================================================= */

function isClientBlocked(string $clientId): bool
{
    $blocked = redisGet('block:' . $clientId);
    if ($blocked === null) {
        return false;
    }

    // TTL 剩余不足一半时刷新，防止攻击者通过持续重试无限续期
    try {
        withRedisForKey('block:' . $clientId, static function (Redis $conn) use ($clientId): void {
            $ttl = $conn->ttl('block:' . $clientId);
            if ($ttl > 0 && $ttl < 300) { // 600 的一半
                $conn->expire('block:' . $clientId, 600);
            }
        });
    } catch (Throwable) {
    }

    return true;
}

/**
 * 5 次失败即封禁：60s → 120s → 240s → 480s → 960s → 封顶 1h
 */
function recordClientFail(string $clientId): void
{
    $key = 'fail:' . $clientId;
    withRedisForKey($key, static function (Redis $conn) use ($key, $clientId): void {
        $script = 'local c = redis.call("INCR", KEYS[1])
if c == 1 then redis.call("EXPIRE", KEYS[1], ARGV[1]) end
return c';
        $c = (int) $conn->eval($script, [$key, 300], 1);

        if ($c >= 5) {
            $ttl = min(3600, 60 * (2 ** min($c - 5, 6)));
            $conn->setex('block:' . $clientId, $ttl, (string) $c);
            fwrite(STDERR, '[block] ' . $clientId . ' (fail count=' . $c . ', ttl=' . $ttl . 's)' . PHP_EOL);
        }
    });
}

/* =========================================================
 * Metrics 埋点
 * ========================================================= */

$metrics = [
    'auth_total'                 => 0,
    'auth_ok'                    => 0,
    'auth_fail'                  => 0,
    'cache_hit'                  => 0,
    'redis_pool_exhausted'       => 0,
    'redis_pool_missing_slot'    => 0,
    'redis_pool_slot_lost'       => 0,
    'device_concurrency_exceeded' => 0,
    'auth_conflict'              => 0,
];

function metricsInc(string $key, int $n = 1): void
{
    global $metrics;
    if (isset($metrics[$key])) {
        $metrics[$key] += $n;
    }
}

function metricsSnapshot(): array
{
    global $metrics;

    return is_array($metrics) ? $metrics : [];
}

/* =========================================================
 * 滑动窗口限流
 * ========================================================= */

function rateLimited(string $key): bool
{
    $max = defined('RATE_MAX') ? RATE_MAX : 10;
    $window = defined('RATE_TTL') ? RATE_TTL : 60;
    try {
        return withRedisForKey($key, static function (Redis $conn) use ($key, $max, $window): bool {
            $script = 'local c = redis.call("INCR", KEYS[1])
if c == 1 then redis.call("EXPIRE", KEYS[1], ARGV[1]) end
return c > tonumber(ARGV[2])';

            return (bool) $conn->eval($script, [$key, $window, $max], 1);
        });
    } catch (Throwable) {
        return false;
    }
}

function hitRate(string $key, int $ttl = 60): int
{
    try {
        return withRedisForKey($key, static function (Redis $conn) use ($key): int {
            return (int) $conn->incr($key);
        });
    } catch (Throwable) {
        return 0;
    }
}

function resetRate(string $key): void
{
    try {
        withRedisForKey($key, static function (Redis $conn) use ($key): void {
            $conn->del($key);
        });
    } catch (Throwable) {
        // ignore
    }
}

/* =========================================================
 * 工具函数
 * ========================================================= */

function base64urlDecode(string $data): string|false
{
    $b64 = strtr($data, '-_', '+/');
    $pad = strlen($b64) % 4;
    if ($pad > 0) {
        $b64 .= str_repeat('=', 4 - $pad);
    }

    return base64_decode($b64, true);
}

if (! function_exists('respondJson')) {
    function respondJson($conn, $data, int $status = 200): void
    {
        if (! is_array($data) && ! ($data instanceof JsonSerializable)) {
            $data = ['error' => 'internal', 'msg' => 'invalid response data'];
            $status = 500;
        }

        $body = json_encode($data, JSON_UNESCAPED_SLASHES | JSON_UNESCAPED_UNICODE);
        if ($body === false) {
            fwrite(STDERR, '[respondJson] json_encode failed: ' . json_last_error_msg() . PHP_EOL);
            $body = '{"error":"json_encode_failed"}';
            $status = 500;
        }

        $length = strlen($body);

        try {
            /** @var ServerConnection $conn */
            $conn->respond($body, $status, [
                'Content-Type'   => 'application/json; charset=utf-8',
                'Content-Length' => (string) $length,
            ]);
        } catch (Throwable $e) {
            fwrite(STDERR, '[respondJson] respond failed: ' . $e->getMessage() . PHP_EOL);
        }
    }
}

if (! function_exists('debugLog')) {
    function debugLog(string $msg): void
    {
        if (! defined('DEBUG') || DEBUG !== true) {
            return;
        }
        fwrite(STDERR, '[EdgeAuth] ' . $msg . PHP_EOL);
    }
}
