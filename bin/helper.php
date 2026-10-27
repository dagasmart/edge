<?php
declare(strict_types=1);

use Swow\Coroutine;

/* =========================================================
 * Redis 连接池（pphpredis + Swow 协程安全）
 * ========================================================= */

/** @var \SplQueue|null */
$redisPool = null;

/** @var int */
$redisPoolSize = 16;

/** @var int Redis 库号 */
$redisDb = 2;

/** @var string Redis 密码 */
$redisPassword = '';

function redisPoolInit(int $size = 16, int $db = 2): void
{
    global $redisPool, $redisPoolSize, $redisDb, $redisPassword;
    $redisPool = new \SplQueue();
    $redisPoolSize = $size;
    $redisDb = $db;

    // 从 server.php 定义的常量取
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
            $redisPool->enqueue($redis);
            $ok++;
            fwrite(STDOUT, "[redis] connected ({$ok}/{$size})\n");
        } catch (\Throwable $e) {
            fwrite(STDERR, "[redis] init failed: " . $e->getMessage() . "\n");
        }
    }

    fwrite(STDERR, "[EdgeAuth] Redis pool initialized: {$ok}/{$size}\n");
    fwrite(STDERR, "[EdgeAuth] Redis pool={$size} db={$redisDb} host={$host} port={$port}\n");
}

function redisPoolPop(float $timeout = 3.0): ?\Redis
{
    global $redisPool;
    if ($redisPool === null) {
        redisPoolInit();
    }

    $start = microtime(true);
    while (true) {
        if (!$redisPool->isEmpty()) {
            return $redisPool->dequeue();
        }
        $elapsed = microtime(true) - $start;
        if ($elapsed >= $timeout) {
            fwrite(STDERR, "[redis] pool pop timeout\n");
            return null;
        }
        if (class_exists(Coroutine::class) && Coroutine::getCurrent() !== null) {
            Coroutine::yield();
            usleep(1000);
        } else {
            usleep(10000);
        }
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
        $redisPool->enqueue($conn);
    } catch (\Throwable $e) {
        try {
            $new = new \Redis();
            $new->connect($host, $port, 2.0);
            if ($redisPassword !== '') {
                $new->auth($redisPassword);
            }
            if ($redisDb > 0) {
                $new->select($redisDb);
            }
            $redisPool->enqueue($new);
            fwrite(STDERR, "[redis] replaced dead connection\n");
        } catch (\Throwable $e2) {
            fwrite(STDERR, "[redis] failed to replace: " . $e2->getMessage() . "\n");
        }
    }
}

/* =========================================================
 * RAII 包裹器
 * ========================================================= */

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

function redisSet(string $key, string $value): bool
{
    return withRedis(fn(\Redis $conn) => (bool) $conn->set($key, $value));
}

function redisSetex(string $key, int $ttl, string $value): bool
{
    return withRedis(fn(\Redis $conn) => (bool) $conn->setex($key, $ttl, $value));
}

function redisExists(string $key): bool
{
    return withRedis(fn(\Redis $conn) => (bool) $conn->exists($key));
}

function redisDel(string ...$keys): int
{
    return withRedis(fn(\Redis $conn) => $conn->del(...$keys));
}

function redisSetNxEx(string $key, string $value, int $ttl): bool
{
    return withRedis(fn(\Redis $conn) => (bool) $conn->set($key, $value, ['NX', 'EX' => $ttl]));
}

/* =========================================================
 * L1 本地缓存
 * ========================================================= */

/** @var \ArrayObject|null */
$localCache = null;

function localCacheInit(): void
{
    global $localCache;
    $localCache = new \ArrayObject();
    fwrite(STDERR, "[EdgeAuth] Local L1 cache (ArrayObject, no-lock) initialized\n");
}

function localCacheGet(string $key): mixed
{
    global $localCache;
    if ($localCache === null) {
        return null;
    }
    if (!isset($localCache[$key])) {
        return null;
    }
    $entry = $localCache[$key];
    if (!is_array($entry) || !isset($entry['expires'])) {
        return null;
    }
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
    $localCache[$key] = [
        'value' => $value,
        'expires' => time() + $ttl,
    ];
}

function localCacheDelete(string $key): void
{
    global $localCache;
    if ($localCache === null) {
        return;
    }
    unset($localCache[$key]);
}

function localCacheStats(): int
{
    global $localCache;
    if ($localCache === null) {
        return 0;
    }
    return $localCache->count();
}

/* =========================================================
 * 连接池状态
 * ========================================================= */

function redisPoolStats(): array
{
    global $redisPool;
    if ($redisPool === null) {
        return ['total' => 0, 'free' => 0];
    }
    return [
        'total' => $GLOBALS['redisPoolSize'] ?? 0,
        'free'  => $redisPool->count(),
    ];
}

/* =========================================================
 * 限流辅助函数
 * ========================================================= */

function rateLimited(string $key): bool
{
    $max = defined('RATE_MAX') ? RATE_MAX : 10;
    $window = defined('RATE_TTL') ? RATE_TTL : 60;
    try {
        return withRedis(function (\Redis $conn) use ($key, $max, $window) {
            $current = $conn->incr($key);
            if ($current === 1) {
                $conn->expire($key, $window);
            }
            return $current > $max;
        });
    } catch (\Throwable $e) {
        fwrite(STDERR, "[rateLimit] " . $e->getMessage() . "\n");
        return false;
    }
}

function hitRate(string $key, int $ttl = 60): int
{
    try {
        return withRedis(function (\Redis $conn) use ($key, $ttl) {
            $current = $conn->incr($key);
            if ($current === 1) {
                $conn->expire($key, $ttl);
            }
            return $current;
        });
    } catch (\Throwable $e) {
        fwrite(STDERR, "[hitRate] " . $e->getMessage() . "\n");
        return 0;
    }
}

function resetRate(string $key): void
{
    try {
        withRedis(fn(\Redis $conn) => $conn->del($key));
    } catch (\Throwable $e) {
        fwrite(STDERR, "[resetRate] " . $e->getMessage() . "\n");
    }
}

/* =========================================================
 * 常量定义（唯一入口，用 defined 保护）
 * server.php 里已经 define 过了，这里全部 if(!defined) 兜底
 * ========================================================= */

if (!defined('LOCAL_CACHE_TTL')) { define('LOCAL_CACHE_TTL', 60); }
if (!defined('TIME_DRIFT'))     { define('TIME_DRIFT', 300); }
if (!defined('RATE_LIMIT_MAX')) { define('RATE_LIMIT_MAX', 10); }
if (!defined('RATE_LIMIT_WINDOW')) { define('RATE_LIMIT_WINDOW', 60); }
