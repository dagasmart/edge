<?php

declare(strict_types=1);

use DagaSmart\Edge\Support\RedisShardManager;
use Swow\Channel;
use Swow\Coroutine;
use Swow\Psr7\Server\ServerConnection;

/* =========================================================
 * Redis 操作 - 统一走 RedisShardManager
 *
 * 改动说明：
 * 1. 删除所有连接池管理函数（redisPoolInit/Pop/Push/Stats/IsReady/WaitReady）
 * 2. 删除 redisShardIndex/redisExtractRouteValue/parseRedisShardList
 * 3. withRedisForKey() 改为薄包装调 RedisShardManager
 * 4. 路由规则统一在 RedisShardManager::extractRouteValue() 一处维护
 * 5. 新增 redisPoolInit() 兼容别名 → 调 RedisShardManager::init()
 * 6. 新增 redisPoolStats() 兼容别名 → 调 RedisShardManager::stats()
 * 7. 新增 redisPoolIsReady() 兼容别名 → 调 RedisShardManager::isReady()
 * ========================================================= */

/**
 * 兼容别名：server.php 里调 redisPoolInit() 不需要改
 */
function redisPoolInit(int $size = 256, int $db = 2): void
{
    RedisShardManager::init($size, $db);
}

/**
 * 兼容别名
 */
function redisPoolStats(): array
{
    return RedisShardManager::stats();
}

/**
 * 兼容别名
 */
function redisPoolIsReady(float $minRatio = 0.8): bool
{
    return RedisShardManager::isReady($minRatio);
}

/**
 * 兼容别名
 */
function redisPoolWaitReady(int $timeoutSec = 10, float $minRatio = 0.8): bool
{
    $deadline = time() + $timeoutSec;
    while (time() < $deadline) {
        if (RedisShardManager::isReady($minRatio)) {
            return true;
        }
        usleep(200000);
    }

    return RedisShardManager::isReady($minRatio);
}

/**
 * 按 key 路由到对应 shard 执行 Redis 操作
 * 签名保持不变，auth.php 零改动
 */
function withRedisForKey(string $key, callable $fn, float $timeout = 0.3): mixed
{
    $conn = RedisShardManager::pop($key, $timeout);
    if ($conn === null) {
        throw new RuntimeException('Redis shard exhausted (key='.$key.')');
    }
    try {
        return $fn($conn);
    } finally {
        RedisShardManager::push($key, $conn);
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
 * 业务 Redis 操作（原样保留）
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
 * L1 本地缓存（原样保留）
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
 * 并发控制 —— Swow\Channel 信号量（原样保留）
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
    fwrite(STDOUT, '[EdgeAuth] Concurrency: Swow\Channel semaphore (max='.$max.')'.PHP_EOL);
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

        fwrite(STDOUT, "[EdgeAuth] Concurrency fully opened: {$targetSize}".PHP_EOL);
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
        'max' => $concurrencyMax,
        'used' => $concurrencyMax - $free,
        'free' => $free,
    ];
}

/* =========================================================
 * 设备级并发分桶限流（原样保留）
 * ========================================================= */

$__edge_device_semaphores = [];
$__edge_device_sem_max = 5;
$__edge_device_sem_ttl = 300;
$__edge_device_last_cleanup = 0;

function deviceConcurrencyInit(int $perDevice, int $cleanupTtl = 300): void
{
    global $__edge_device_semaphores, $__edge_device_sem_max, $__edge_device_sem_ttl;
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
            fwrite(STDERR, '[device-acquire] channel create failed: '.$e->getMessage().PHP_EOL);

            return null;
        }
        $__edge_device_semaphores[$clientId] = [
            'channel' => $ch,
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
        debugLog('deviceConcurrencyCleanup: removed '.$removed.' idle buckets');
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
            'max' => $__edge_device_sem_max,
            'used' => $__edge_device_sem_max - $free,
            'free' => $free,
        ];
    }

    return $stats;
}

/* =========================================================
 * 客户端失败熔断
 *
 * ★ 改动：
 * 1. recordClientFail() - fail 窗口 300s → 60s
 * 2. isClientBlocked() - 删除刷新 TTL 逻辑（防攻击者续期）
 * ========================================================= */

function isClientBlocked(string $clientId): bool
{
    $blocked = redisGet('block:' . $clientId);
    if ($blocked === null) {
        return false;
    }

    // ★ 不再刷新 TTL，让 block 自然过期，防止持续重试续期
    return true;
}

/**
 * 5 次失败即封禁：60s → 120s → 240s → 480s → 960s → 封顶 1h
 * ★ fail 窗口从 300s 改为 60s
 */
function recordClientFail(string $clientId): void
{
    $key = 'fail:' . $clientId;
    withRedisForKey($key, static function (Redis $conn) use ($key, $clientId): void {
        $script = 'local c = redis.call("INCR", KEYS[1])
if c == 1 then redis.call("EXPIRE", KEYS[1], ARGV[1]) end
return c';
        $c = (int) $conn->eval($script, [$key, 60], 1);  // ★ 改 300 → 60

        if ($c >= 5) {
            $ttl = min(3600, 60 * (2 ** min($c - 5, 6)));
            $conn->setex('block:' . $clientId, $ttl, (string) $c);
            fwrite(STDERR, '[block] ' . $clientId . ' (fail count=' . $c . ', ttl=' . $ttl . 's)' . PHP_EOL);
        }
    });
}

/* =========================================================
 * Metrics 埋点（原样保留）
 * ========================================================= */

$metrics = [
    'auth_total' => 0,
    'auth_ok' => 0,
    'auth_fail' => 0,
    'cache_hit' => 0,
    'redis_pool_exhausted' => 0,
    'redis_pool_missing_slot' => 0,
    'redis_pool_slot_lost' => 0,
    'device_concurrency_exceeded' => 0,
    'auth_conflict' => 0,
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
 * 滑动窗口限流（原样保留）
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
 * 工具函数（原样保留）
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
                'Content-Type' => 'application/json; charset=utf-8',
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
