<?php

declare(strict_types=1);

use Swow\Channel;

/* =========================================================
 * Redis 连接池（Swow Channel 真协程池）
 * ========================================================= */

/** @var Channel|null $redisPool */
$redisPool = null;

$redisPoolSize = 16;

$redisDb = 2;

$redisPassword = '';

function redisPoolInit(int $size = 256, int $db = 2): void
{
    global $redisPool, $redisPoolSize, $redisDb, $redisPassword;

    $redisPool = new Channel($size);
    $redisPoolSize = $size;
    $redisDb = $db;

    if (defined('REDIS_PASSWORD')) {
        $redisPassword = REDIS_PASSWORD;
    }

    $host = defined('REDIS_HOST') ? REDIS_HOST : '127.0.0.1';
    $port = defined('REDIS_PORT') ? (int) REDIS_PORT : 6379;

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
            $redisPool->push($redis);
            $ok++;
        } catch (\Throwable $e) {
            fwrite(STDERR, '[redis] init failed: ' . $e->getMessage() . PHP_EOL);
        }
    }
    fwrite(STDERR, '[EdgeAuth] Redis pool initialized: ' . $ok . '/' . $size . PHP_EOL);
}

function redisPoolPop(float $timeout = 1.0): ?\Redis
{
    global $redisPool;
    if ($redisPool === null) {
        redisPoolInit();
    }
    try {
        // ✅ 转成毫秒 int
        $ms = (int) ($timeout * 1000);
        return $redisPool->pop($ms);
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
    $port = defined('REDIS_PORT') ? (int) REDIS_PORT : 6379;

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
        fwrite(STDERR, '[redis] replace failed: ' . $e2->getMessage() . PHP_EOL);
        metricsInc('redis_pool_exhausted');
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
    return withRedis(static fn(\Redis $conn): mixed => $conn->get($key));
}

function redisSetex(string $key, int $ttl, string $value): bool
{
    return withRedis(static fn(\Redis $conn): bool => (bool) $conn->setex($key, $ttl, $value));
}

function redisExists(string $key): bool
{
    return withRedis(static fn(\Redis $conn): bool => (bool) $conn->exists($key));
}

function redisDel(string ...$keys): int
{
    return withRedis(static fn(\Redis $conn): int => $conn->del(...$keys));
}

function redisSetNxEx(string $key, string $value, int $ttl): bool
{
    return withRedis(static fn(\Redis $conn): bool => (bool) $conn->set($key, $value, ['NX', 'EX' => $ttl]));
}

/* =========================================================
 * L1 本地缓存（带上限，超了拒绝写入，靠 TTL 淘汰）
 * ========================================================= */

/** @var ArrayObject|null $localCache */
$localCache = null;

function localCacheInit(): void
{
    global $localCache;
    $localCache = new ArrayObject();
}

function localCacheGet(string $key): mixed
{
    global $localCache;
    if ($localCache === null || !isset($localCache[$key])) {
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
    // ✅ 超上限直接拒绝，不误删热点 key
    if ($localCache->count() >= 10000) {
        return;
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
        'total' => $redisPoolSize,
        'free'  => $redisPool->getLength(),
    ];
}

/* =========================================================
 * 并发控制（Atomic 信号量，兼容所有 Swow 版本）
 * ========================================================= */

$globalConcurrency = null;

$concurrencyMax = 10000;

function concurrencyInit(int $max): void
{
    global $globalConcurrency, $concurrencyMax;
    $concurrencyMax = $max;
    if (class_exists('Swow\Atomic')) {
        $globalConcurrency = new \Swow\Atomic(0);
    } else {
        $globalConcurrency = null;
        fwrite(STDERR, '[EdgeAuth] WARNING: Swow\Atomic not found, concurrency limit DISABLED' . PHP_EOL);
    }
}

function concurrencyAcquire(float $timeout = 2.0): bool
{
    global $globalConcurrency, $concurrencyMax;
    if ($globalConcurrency === null) {
        return true;
    }
    $current = $globalConcurrency->add(1);
    if ($current > $concurrencyMax) {
        $globalConcurrency->sub(1);
        return false;
    }
    return true;
}

function concurrencyRelease(): void
{
    global $globalConcurrency;
    $globalConcurrency?->sub(1);
}

/* =========================================================
 * 客户端失败熔断（Lua 原子版）
 * ========================================================= */

function isClientBlocked(string $clientId): bool
{
    $v = redisGet('block:' . $clientId);
    return $v !== null && $v !== false;
}

function recordClientFail(string $clientId): void
{
    $key = 'fail:' . $clientId;
    withRedis(static function (\Redis $conn) use ($key, $clientId): void {
        // ✅ Lua 原子：INCR + EXPIRE 不中断，消除幽灵 key
        $script = 'local c = redis.call("INCR", KEYS[1])
if c == 1 then redis.call("EXPIRE", KEYS[1], ARGV[1]) end
return c';
        $c = (int) $conn->eval($script, [$key, 300], 1);

        if ($c >= 20) {
            $conn->setex('block:' . $clientId, 600, '1');
            fwrite(STDERR, '[block] ' . $clientId . PHP_EOL);
        }
    });
}

/* =========================================================
 * 指标
 * ========================================================= */

$metrics = [
    'auth_total'           => 0,
    'auth_ok'              => 0,
    'auth_fail'            => 0,
    'cache_hit'            => 0,
    'redis_pool_exhausted' => 0,
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
    return $metrics ?? [];
}

/* =========================================================
 * 限流（fail-open：Redis 挂了不拦截）
 * ========================================================= */

function rateLimited(string $key): bool
{
    $max = defined('RATE_MAX') ? RATE_MAX : 10;
    $window = defined('RATE_TTL') ? RATE_TTL : 60;
    try {
        return withRedis(static function (\Redis $conn) use ($key, $max, $window): bool {
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
        return withRedis(static fn(\Redis $conn): int => (int) $conn->incr($key));
    } catch (\Throwable) {
        return 0;
    }
}

function resetRate(string $key): void
{
    try {
        withRedis(static fn(\Redis $conn): bool => (bool) $conn->del($key));
    } catch (\Throwable) {
        // ignore
    }
}
