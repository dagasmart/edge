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
$redisHost = '127.0.0.1';
$redisPort = 6379;

function redisPoolInit(int $size = 256, int $db = 2): void
{
    global $redisPool, $redisPoolSize, $redisDb, $redisPassword, $redisHost, $redisPort;

    $redisPool = new Channel($size);
    $redisPoolSize = $size;
    $redisDb = $db;

    if (defined('REDIS_PASSWORD')) {
        $redisPassword = REDIS_PASSWORD;
    }
    if (defined('REDIS_HOST')) {
        $redisHost = REDIS_HOST;
    }
    if (defined('REDIS_PORT')) {
        $redisPort = (int) REDIS_PORT;
    }

    $ok = 0;
    for ($i = 0; $i < $size; $i++) {
        $redis = new \Redis();
        try {
            $redis->connect($redisHost, $redisPort, 2.0);
            if ($redisPassword !== '') {
                $authOk = $redis->auth($redisPassword);
                if (!$authOk) {
                    fwrite(STDERR, '[redis] AUTH FAILED for ' . $redisHost . ':' . $redisPort . ' (password length=' . strlen($redisPassword) . ')' . PHP_EOL);
                    continue;
                }
            }
            if ($redisDb > 0) {
                $selectOk = $redis->select($redisDb);
                if (!$selectOk) {
                    fwrite(STDERR, '[redis] SELECT DB ' . $redisDb . ' FAILED' . PHP_EOL);
                    continue;
                }
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
        $ms = (int) ($timeout * 1000);
        return $redisPool->pop($ms);
    } catch (\Throwable) {
        return null;
    }
}

function redisPoolPush(\Redis $conn): void
{
    global $redisPool, $redisDb, $redisPassword, $redisHost, $redisPort;

    if ($redisPool === null) {
        return;
    }

    try {
        $conn->ping();
        $redisPool->push($conn);
        return;
    } catch (\Throwable) {
        // dead, recreate
    }

    try {
        $new = new \Redis();
        $new->connect($redisHost, $redisPort, 2.0);
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
 * 业务 Redis 操作 —— 每次操作前强制 select(db)
 * ========================================================= */

function redisGet(string $key): mixed
{
    global $redisDb;
    return withRedis(static function (\Redis $conn) use ($key, $redisDb): mixed {
        if ($redisDb > 0) {
            $conn->select($redisDb);
        }
        $val = $conn->get($key);
        return $val !== false ? (string) $val : null;
    });
}

function redisSetex(string $key, int $ttl, string $value): bool
{
    global $redisDb;
    return withRedis(static function (\Redis $conn) use ($key, $ttl, $value, $redisDb): bool {
        if ($redisDb > 0) {
            $conn->select($redisDb);
        }
        return (bool) $conn->setex($key, $ttl, $value);
    });
}

function redisExists(string $key): bool
{
    global $redisDb;
    return withRedis(static function (\Redis $conn) use ($key, $redisDb): bool {
        if ($redisDb > 0) {
            $conn->select($redisDb);
        }
        return (bool) $conn->exists($key);
    });
}

function redisDel(string ...$keys): int
{
    global $redisDb;
    return withRedis(static function (\Redis $conn) use ($keys, $redisDb): int {
        if ($redisDb > 0) {
            $conn->select($redisDb);
        }
        return $conn->del(...$keys);
    });
}

function redisSetNxEx(string $key, string $value, int $ttl): bool
{
    global $redisDb;
    return withRedis(static function (\Redis $conn) use ($key, $value, $ttl, $redisDb): bool {
        if ($redisDb > 0) {
            $conn->select($redisDb);
        }
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
 * 并发控制
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
    return $v !== null;
}

function recordClientFail(string $clientId): void
{
    $key = 'fail:' . $clientId;
    withRedis(static function (\Redis $conn) use ($key, $clientId): void {
        global $redisDb;
        if ($redisDb > 0) {
            $conn->select($redisDb);
        }
        $script = 'local c = redis.call("INCR", KEYS[1])
if c == 1 then redis.call("EXPIRE", KEYS[1], ARGV[1]) end
return c';
        $c = (int) $conn->eval($script, [$key, 300], 1);
        if ($c >= 20) {
            $conn->setex('block:' . $clientId, 600, '1');
            fwrite(STDERR, '[block] ' . $clientId . ' (fail count=' . $c . ')' . PHP_EOL);
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
 * 限流
 * ========================================================= */

function rateLimited(string $key): bool
{
    $max = defined('RATE_MAX') ? RATE_MAX : 10;
    $window = defined('RATE_TTL') ? RATE_TTL : 60;
    try {
        return withRedis(static function (\Redis $conn) use ($key, $max, $window): bool {
            global $redisDb;
            if ($redisDb > 0) {
                $conn->select($redisDb);
            }
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
        return withRedis(static function (\Redis $conn) use ($key, $ttl): int {
            global $redisDb;
            if ($redisDb > 0) {
                $conn->select($redisDb);
            }
            return (int) $conn->incr($key);
        });
    } catch (\Throwable) {
        return 0;
    }
}

function resetRate(string $key): void
{
    try {
        withRedis(static function (\Redis $conn) use ($key): void {
            global $redisDb;
            if ($redisDb > 0) {
                $conn->select($redisDb);
            }
            $conn->del($key);
        });
    } catch (\Throwable) {
        // ignore
    }
}

/* =========================================================
 * Base64URL 安全解码
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

/* =========================================================
 * HTTP 响应工具
 * ========================================================= */

if (!function_exists('respondJson')) {
    function respondJson($conn, $data, int $status = 200): void
    {
        $body = json_encode($data, JSON_UNESCAPED_SLASHES | JSON_UNESCAPED_UNICODE);
        if ($body === false) {
            $body = '{"error":"json_encode_failed"}';
            $status = 500;
        }

        $conn->respond(
            $body,
            $status,
            ['Content-Type' => 'application/json; charset=utf-8']
        );
    }
}

/* =========================================================
 * 调试日志
 * ========================================================= */

if (!function_exists('debugLog')) {
    function debugLog(string $msg): void
    {
        if (defined('DEBUG') && DEBUG === false) {
            return;
        }
        fwrite(STDERR, '[EdgeAuth] ' . $msg . PHP_EOL);
    }
}
