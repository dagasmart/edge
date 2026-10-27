<?php
declare(strict_types=1);

use Swow\Channel;

/* =========================================================
 * Redis 连接池（Swow Channel 真协程池）
 * ========================================================= */

/** @var Channel|null $redisPool */
$redisPool = null;

/** @var int */
$redisPoolSize = 16;

/** @var int */
$redisDb = 2;

/** @var string */
$redisPassword = '';

function redisPoolInit(int $size = 16, int $db = 2): void
{
    global $redisPool, $redisPoolSize, $redisDb, $redisPassword;
    $redisPool = new Channel($size);
    $redisPoolSize = $size;
    $redisDb = $db;

    if (defined('REDIS_PASSWORD')) {
        $redisPassword = REDIS_PASSWORD;
    }

    $host = defined('REDIS_HOST') ? REDIS_HOST : '127.0.0.1';
    $port = defined('REDIS_PORT') ? (int)REDIS_PORT : 6379;

    $ok = 0;
    for ($i = 0; $i < $size; $i++) {
        $redis = new \Redis();
        try {
            $redis->connect($host, $port, 2.0);
            if ($redisPassword !== '') {
                $redis->auth($redisPassword);
            }
            if ($redisDb > 0) {
                $redis->select($redisDb);
            }
            $redisPool->push($redis); // ✅ Channel::push，不是 enqueue
            $ok++;
        } catch (\Throwable $e) {
            fwrite(STDERR, "[redis] init failed: " . $e->getMessage() . "\n");
        }
    }
    fwrite(STDERR, "[EdgeAuth] Redis pool initialized: {$ok}/{$size}\n");
}

function redisPoolPop(float $timeout = 3): ?\Redis
{
    global $redisPool;
    if ($redisPool === null) {
        redisPoolInit();
    }
    try {
        return $redisPool->pop($timeout);
    } catch (\Throwable) {
        return null;
    }
}

function redisPoolPush(\Redis $conn): void
{
    global $redisPool, $redisDb, $redisPassword;
    if ($redisPool === null) {
        return;
    }

    $host = defined('REDIS_HOST') ? REDIS_HOST : '127.0.0.1';
    $port = defined('REDIS_PORT') ? (int)REDIS_PORT : 6379;

    try {
        $conn->ping();
        $redisPool->push($conn);
        return;
    } catch (\Throwable) {
        // dead, recreate
    }

    try {
        $new = new \Redis();
        $new->connect($host, $port, 2.0);
        if ($redisPassword !== '') {
            $new->auth($redisPassword);
        }
        if ($redisDb > 0) {
            $new->select($redisDb);
        }
        $redisPool->push($new);
    } catch (\Throwable $e2) {
        fwrite(STDERR, "[redis] replace failed: " . $e2->getMessage() . "\n");
    }
}

function withRedis(callable $fn): mixed
{
    $conn = redisPoolPop();
    if ($conn === null) {
        throw new \RuntimeException('Redis pool exhausted');
    }
    try {
        return $fn($conn);
    } finally {
        redisPoolPush($conn);
    }
}

/* =========================================================
 * 业务 Redis 操作
 * ========================================================= */

function redisGet(string $key): mixed
{
    return withRedis(fn(\Redis $conn) => $conn->get($key));
}

function redisSetex(string $key, int $ttl, string $value): bool
{
    return withRedis(fn(\Redis $conn) => (bool)$conn->setex($key, $ttl, $value));
}

function redisExists(string $key): bool
{
    return withRedis(fn(\Redis $conn) => (bool)$conn->exists($key));
}

function redisDel(string ...$keys): int
{
    return withRedis(fn(\Redis $conn) => $conn->del(...$keys));
}

function redisSetNxEx(string $key, string $value, int $ttl): bool
{
    return withRedis(fn(\Redis $conn) => (bool)$conn->set($key, $value, ['NX', 'EX' => $ttl]));
}

/* =========================================================
 * L1 本地缓存（带上限）
 * ========================================================= */

/** @var \ArrayObject|null */
$localCache = null;

function localCacheInit(): void
{
    global $localCache;
    $localCache = new \ArrayObject();
}

function localCacheGet(string $key): mixed
{
    global $localCache;
    if ($localCache === null || !isset($localCache[$key])) {
        return null;
    }
    $entry = $localCache[$key];
    if ($entry['expires'] < time()) {
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
    // 防膨胀：超 10000 条清 20%
    if ($localCache->count() > 10000) {
        $i = 0;
        foreach ($localCache as $k => $v) {
            unset($localCache[$k]);
            if (++$i > 2000) break;
        }
    }
    $localCache[$key] = [
        'value'   => $value,
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
    global $redisPool, $redisPoolSize;
    if ($redisPool === null) {
        return ['total' => 0, 'free' => 0];
    }
    return [
        'total' => $redisPoolSize ?? 0,
        'free'  => $redisPool->getLength(), // ✅ Channel 用 getLength()
    ];
}

/* =========================================================
 * 并发熔断
 * ========================================================= */

/** @var Channel|null */
$globalConcurrency = null;

function concurrencyInit(int $max): void
{
    global $globalConcurrency;
    $globalConcurrency = new Channel($max);
    for ($i = 0; $i < $max; $i++) {
        $globalConcurrency->push(1);
    }
}

function concurrencyAcquire(float $timeout = 2.0): bool
{
    global $globalConcurrency;
    if ($globalConcurrency === null) {
        return true;
    }
    try {
        $globalConcurrency->pop($timeout);
        return true;
    } catch (\Throwable) {
        return false;
    }
}

function concurrencyRelease(): void
{
    global $globalConcurrency;
    if ($globalConcurrency !== null) {
        $globalConcurrency->push(1);
    }
}

/* =========================================================
 * 客户端失败熔断
 * ========================================================= */

function isClientBlocked(string $clientId): bool
{
    $v = redisGet("block:{$clientId}");
    return $v !== null && $v !== false;
}

function recordClientFail(string $clientId): void
{
    $key = "fail:{$clientId}";
    withRedis(function (\Redis $conn) use ($key, $clientId) {
        $c = $conn->incr($key);
        if ($c === 1) {
            $conn->expire($key, 300);
        }
        if ($c >= 20) {
            $conn->setex("block:{$clientId}", 600, '1');
            fwrite(STDERR, "[block] {$clientId}\n");
        }
    });
}

/* =========================================================
 * 指标
 * ========================================================= */

$metrics = [
    'auth_total' => 0,
    'auth_ok'    => 0,
    'auth_fail'  => 0,
    'cache_hit'  => 0,
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
    return $metrics;
}

/* =========================================================
 * 限流
 * ========================================================= */

function rateLimited(string $key): bool
{
    $max = defined('RATE_MAX') ? RATE_MAX : 10;
    $window = defined('RATE_TTL') ? RATE_TTL : 60;
    try {
        return withRedis(function (\Redis $conn) use ($key, $max, $window) {
            $c = $conn->incr($key);
            if ($c === 1) {
                $conn->expire($key, $window);
            }
            return $c > $max;
        });
    } catch (\Throwable) {
        return false;
    }
}

function hitRate(string $key, int $ttl = 60): int
{
    try {
        return withRedis(fn(\Redis $conn) => $conn->incr($key));
    } catch (\Throwable) {
        return 0;
    }
}

function resetRate(string $key): void
{
    try {
        withRedis(fn(\Redis $conn) => $conn->del($key));
    } catch (\Throwable) {}
}
