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
 * 并发控制 —— Swow\Channel 信号量（不依赖 Swow\Atomic）
 * 修正：类名 Swow\Channel（非 Swow\Coroutine\Channel）
 * ========================================================= */

$globalConcurrency = null;
$concurrencyMax = 10000;

function concurrencyInit(int $max): void
{
    global $globalConcurrency, $concurrencyMax;
    $concurrencyMax = $max;
    $globalConcurrency = new \Swow\Channel($max);
    for ($i = 0; $i < $max; $i++) {
        $globalConcurrency->push(1, 1);
    }
    fwrite(STDOUT, '[EdgeAuth] Concurrency: Swow\Channel semaphore (max=' . $max . ')' . PHP_EOL);
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
    $free = $globalConcurrency->getLength();
    return [
        'max'  => $concurrencyMax,
        'used' => $concurrencyMax - $free,
        'free' => $free,
    ];
}

/* =========================================================
 * 设备级并发分桶限流（方案 A：进程内 Channel，多 worker 各自独立）
 * REUSEPORT 模式下 4 worker 各维护独立桶，
 * MAX_CONCURRENCY_PER_DEVICE=2 → 等效全局 ~8 并发/设备
 * 修正：Swow\Channel 类名 + acquire 中不刷新 last_used
 * ========================================================= */

$__edge_device_semaphores = [];
$__edge_device_sem_max = 5;
$__edge_device_sem_ttl = 300;

function deviceConcurrencyInit(int $perDevice, int $cleanupTtl = 300): void
{
    global $__edge_device_sem_max, $__edge_device_sem_ttl;
    $__edge_device_sem_max = $perDevice;
    $__edge_device_sem_ttl = $cleanupTtl;
}

function deviceConcurrencyAcquire(string $clientId): ?string
{
    global $__edge_device_semaphores, $__edge_device_sem_max;

    if (!isset($__edge_device_semaphores[$clientId])) {
        $ch = new \Swow\Channel($__edge_device_sem_max);
        for ($i = 0; $i < $__edge_device_sem_max; $i++) {
            $ch->push(1, 1);
        }
        $__edge_device_semaphores[$clientId] = [
            'channel'   => $ch,
            'last_used' => time(),
        ];
    }

    // 注意：acquire 不刷新 last_used，只在 release 时刷新
    // 这样 cleanup 的语义是"最后一次请求完成后的空闲时间"

    try {
        $got = $__edge_device_semaphores[$clientId]['channel']->pop(1);
        return $got ? $clientId : null;
    } catch (Throwable) {
        return null;
    }
}

function deviceConcurrencyRelease(?string $clientId): void
{
    global $__edge_device_semaphores;

    if ($clientId === null || !isset($__edge_device_semaphores[$clientId])) {
        return;
    }

    try {
        $__edge_device_semaphores[$clientId]['channel']->push(1, 1);
        $__edge_device_semaphores[$clientId]['last_used'] = time();
    } catch (Throwable) {
        // 桶满忽略
    }
}

/**
 * 清理过期空桶
 * @return void
 */
function deviceConcurrencyCleanup(): void
{
    global $__edge_device_semaphores, $__edge_device_sem_ttl;

    static $lastCleanup = 0;
    $now = time();
    // ---------------------------------------------------------------------------
    // 节流间隔说明（按需调整），默认60s：                                              |
    // | 方案         | 间隔 | 理由                                                 |
    // |-------------|------|-----------------------------------------------------|
    // | 太保守       | 60s  | 别清理太频繁，过期桶最多多活 60s                          |
    // | 保守但合理    | 10s  | 平衡频率和开销，过期桶最多多活 10s                        |
    // | ★ 推荐       | 5s   | handler 入口已在调，5s 节流即高流量也只每 5s 真正扫一次，   |
    // |             |      | 几乎零成本，过期桶最多多占 5s                            |
    // | 激进         | 1s   | 完全没问题，但 debugLog 可能刷得频繁                     |
    // 当前取值：5s（推荐档）                                                         |
    // ----------------------------------------------------------------------------
    if ($now - $lastCleanup < 5) {  // ← 60 改 5
        return;
    }
    $lastCleanup = $now;

    $removed = 0;
    foreach ($__edge_device_semaphores as $cid => $bucket) {
        if ($now - $bucket['last_used'] > $__edge_device_sem_ttl) {
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

    $stats = [];
    foreach ($__edge_device_semaphores as $cid => $bucket) {
        $free = $bucket['channel']->getLength();
        $stats[$cid] = [
            'max'  => $__edge_device_sem_max,
            'used' => $__edge_device_sem_max - $free,
            'free' => $free,
        ];
    }
    return $stats;
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
    'auth_total'                  => 0,
    'auth_ok'                     => 0,
    'auth_fail'                   => 0,
    'cache_hit'                   => 0,
    'redis_pool_exhausted'        => 0,
    'device_concurrency_exceeded' => 0,
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
