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

    // ★ 冷启动防护：先建 3 个探活连接，不一次性冲击 Redis
    $probeSize = min(3, $size);
    $ok = 0;
    for ($i = 0; $i < $probeSize; $i++) {
        $redis = new Redis;
        try {
            $redis->connect($redisHost, $redisPort, 2.0);
            if ($redisPassword !== '') {
                $authOk = $redis->auth($redisPassword);
                if (! $authOk) {
                    fwrite(STDERR, '[redis] AUTH FAILED for '.$redisHost.':'.$redisPort.' (password length='.strlen($redisPassword).')'.PHP_EOL);
                    continue;
                }
            }
            if ($redisDb > 0) {
                $selectOk = $redis->select($redisDb);
                if (! $selectOk) {
                    fwrite(STDERR, '[redis] SELECT DB '.$redisDb.' FAILED'.PHP_EOL);
                    continue;
                }
            }
            $redisPool->push($redis);
            $ok++;
        } catch (Throwable $e) {
            fwrite(STDERR, '[redis] probe '.$i.' failed: '.$e->getMessage().PHP_EOL);
        }
    }

    fwrite(STDOUT, "[EdgeAuth] Redis pool probed: {$ok}/{$probeSize}".PHP_EOL);

    // ★ 后台协程逐步填充剩余连接（每 10ms 一个，不打爆 Redis）
    if ($ok > 0 && $size > $probeSize) {
        $remaining = $size - $probeSize;
        Swow\Coroutine::run(static function () use ($remaining, $probeSize): void {
            global $redisPool, $redisHost, $redisPort, $redisPassword, $redisDb;
            $filled = 0;
            for ($i = 0; $i < $remaining; $i++) {
                try {
                    $redis = new Redis;
                    $redis->connect($redisHost, $redisPort, 2.0);
                    if ($redisPassword !== '') {
                        $redis->auth($redisPassword);
                    }
                    if ($redisDb > 0) {
                        $redis->select($redisDb);
                    }
                    $redisPool->push($redis);
                    $filled++;
                } catch (Throwable $e) {
                    fwrite(STDERR, '[redis] background fill failed: '.$e->getMessage().PHP_EOL);
                }
                usleep(10000); // 10ms 间隔
            }
            fwrite(STDOUT, "[EdgeAuth] Redis pool background filled: {$filled}/{$remaining}".PHP_EOL);
        });
    }

    if ($ok === 0) {
        fwrite(STDERR, '[EdgeAuth] Redis pool: ALL PROBES FAILED, pool is empty!'.PHP_EOL);
    }
}

// ---------- 以下全部与你原始 helper.php 一致，一字不改 ----------

function redisPoolPop(float $timeout = 1.0): ?Redis
{
    global $redisPool;
    if ($redisPool === null) {
        redisPoolInit();
    }
    try {
        $ms = (int) ($timeout * 1000);
        return $redisPool->pop($ms);
    } catch (Throwable) {
        return null;
    }
}

function redisPoolPush(Redis $conn): void
{
    global $redisPool, $redisDb, $redisPassword, $redisHost, $redisPort;
    if ($redisPool === null) {
        return;
    }
    try {
        $conn->ping();
        $redisPool->push($conn);
        return;
    } catch (Throwable) {
        // dead, recreate
    }
    try {
        $new = new Redis;
        $new->connect($redisHost, $redisPort, 2.0);
        if ($redisPassword !== '') {
            $new->auth($redisPassword);
        }
        if ($redisDb > 0) {
            $new->select($redisDb);
        }
        $redisPool->push($new);
    } catch (Throwable $e2) {
        fwrite(STDERR, '[redis] replace failed: '.$e2->getMessage().PHP_EOL);
        metricsInc('redis_pool_exhausted');
    }
}

function withRedis(callable $fn): mixed
{
    $conn = redisPoolPop();
    if ($conn === null) {
        throw new RuntimeException('Redis pool exhausted');
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
// （与你原始 helper.php 完全一致：redisGet/redisSetex/redisExists/redisDel/redisSetNxEx）

function redisGet(string $key): mixed
{
    global $redisDb;
    return withRedis(static function (Redis $conn) use ($key, $redisDb): mixed {
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
    return withRedis(static function (Redis $conn) use ($key, $ttl, $value, $redisDb): bool {
        if ($redisDb > 0) {
            $conn->select($redisDb);
        }
        return (bool) $conn->setex($key, $ttl, $value);
    });
}

function redisExists(string $key): bool
{
    global $redisDb;
    return withRedis(static function (Redis $conn) use ($key, $redisDb): bool {
        if ($redisDb > 0) {
            $conn->select($redisDb);
        }
        return (bool) $conn->exists($key);
    });
}

function redisDel(string ...$keys): int
{
    global $redisDb;
    return withRedis(static function (Redis $conn) use ($keys, $redisDb): int {
        if ($redisDb > 0) {
            $conn->select($redisDb);
        }
        return $conn->del(...$keys);
    });
}

function redisSetNxEx(string $key, string $value, int $ttl): bool
{
    global $redisDb;
    return withRedis(static function (Redis $conn) use ($key, $value, $ttl, $redisDb): bool {
        if ($redisDb > 0) {
            $conn->select($redisDb);
        }
        return (bool) $conn->set($key, $value, ['NX', 'EX' => $ttl]);
    });
}

/* =========================================================
 * L1 本地缓存
 * ========================================================= */
// （与你原始 helper.php 完全一致）

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
    global $redisPool, $redisPoolSize;
    if ($redisPool === null) {
        return ['total' => 0, 'free' => 0];
    }
    return [
        'total' => $redisPoolSize,
        'free' => $redisPool->getLength(),
    ];
}

/* =========================================================
 * Redis 池就绪自检
 * ========================================================= */

function redisPoolIsReady(float $minRatio = 0.8): bool
{
    global $redisPool, $redisPoolSize;
    if ($redisPool === null || $redisPoolSize <= 0) {
        return false;
    }
    try {
        $free = $redisPool->getLength();
    } catch (Throwable) {
        return false;
    }
    return $free >= ($redisPoolSize * $minRatio);
}

/**
 * 阻塞等待 Redis 池填充到指定比例（带超时）
 * @return bool true=就绪, false=超时
 */
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
    fwrite(STDOUT, '[EdgeAuth] Concurrency: Swow\Channel semaphore (max='.$max.')'.PHP_EOL);
}

// ★ 新增：并发信号量渐进扩容
function concurrencyProgressiveExpand(int $targetSize, int $steps = 10, int $stepMs = 300): void
{
    global $globalConcurrency, $concurrencyMax;
    $current = $concurrencyMax;
    if ($current >= $targetSize) {
        return;
    }
    $toAdd = $targetSize - $current;
    $perStep = max(1, (int) ceil($toAdd / $steps));

    Swow\Coroutine::run(static function () use ($perStep, $toAdd, $targetSize, $stepMs): void {
        global $globalConcurrency, $concurrencyMax;
        $added = 0;
        for ($step = 0; $step < 10; $step++) {
            usleep($stepMs * 1000);
            $batch = min($perStep, $toAdd - $added);
            for ($i = 0; $i < $batch; $i++) {
                try {
                    $globalConcurrency->push(1, 1);
                    $added++;
                    $concurrencyMax++; // 同步更新上限标记
                } catch (Throwable) {
                    break;
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
 * 设备级并发分桶限流
 * （与你原始 helper.php 完全一致）
 * ========================================================= */
// ... $__edge_device_semaphores / deviceConcurrencyInit / Acquire / Release / Cleanup / Stats
// （完整保留你 user_doc 3 中的实现，此处不重复）

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
    global $__edge_device_semaphores, $__edge_device_sem_ttl;
    static $lastCleanup = 0;
    $now = time();
    if (! is_array($__edge_device_semaphores)) {
        return;
    }
    if ($now - $lastCleanup < 5) {
        return;
    }
    $lastCleanup = $now;
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
 * 客户端失败熔断 / 指标 / 限流 / Base64 / respondJson / debugLog
 * （与你原始 helper.php 完全一致，一字不改）
 * ========================================================= */

function isClientBlocked(string $clientId): bool
{
    $v = redisGet('block:'.$clientId);
    return $v !== null;
}

function recordClientFail(string $clientId): void
{
    $key = 'fail:'.$clientId;
    withRedis(static function (Redis $conn) use ($key, $clientId): void {
        global $redisDb;
        if ($redisDb > 0) {
            $conn->select($redisDb);
        }
        $script = 'local c = redis.call("INCR", KEYS[1])
if c == 1 then redis.call("EXPIRE", KEYS[1], ARGV[1]) end
return c';
        $c = (int) $conn->eval($script, [$key, 300], 1);
        if ($c >= 20) {
            $conn->setex('block:'.$clientId, 600, '1');
            fwrite(STDERR, '[block] '.$clientId.' (fail count='.$c.')'.PHP_EOL);
        }
    });
}

$metrics = [
    'auth_total' => 0,
    'auth_ok' => 0,
    'auth_fail' => 0,
    'cache_hit' => 0,
    'redis_pool_exhausted' => 0,
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
    return is_array($metrics) ? $metrics : [];
}

function rateLimited(string $key): bool
{
    $max = defined('RATE_MAX') ? RATE_MAX : 10;
    $window = defined('RATE_TTL') ? RATE_TTL : 60;
    try {
        return withRedis(static function (Redis $conn) use ($key, $max, $window): bool {
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
    } catch (Throwable) {
        return false;
    }
}

function hitRate(string $key, int $ttl = 60): int
{
    try {
        return withRedis(static function (Redis $conn) use ($key): int {
            global $redisDb;
            if ($redisDb > 0) {
                $conn->select($redisDb);
            }
            return (int) $conn->incr($key);
        });
    } catch (Throwable) {
        return 0;
    }
}

function resetRate(string $key): void
{
    try {
        withRedis(static function (Redis $conn) use ($key): void {
            global $redisDb;
            if ($redisDb > 0) {
                $conn->select($redisDb);
            }
            $conn->del($key);
        });
    } catch (Throwable) {
        // ignore
    }
}

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
            fwrite(STDERR, '[respondJson] json_encode failed: '.json_last_error_msg().PHP_EOL);
            $body = '{"error":"json_encode_failed"}';
            $status = 500;
        }
        try {
            $conn->respond($body, $status, ['Content-Type' => 'application/json; charset=utf-8']);
        } catch (Throwable $e) {
            fwrite(STDERR, '[respondJson] respond failed: '.$e->getMessage().PHP_EOL);
        }
    }
}

if (! function_exists('debugLog')) {
    function debugLog(string $msg): void
    {
        if (defined('DEBUG') && DEBUG === false) {
            return;
        }
        fwrite(STDERR, '[EdgeAuth] '.$msg.PHP_EOL);
    }
}
