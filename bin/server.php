<?php

declare(strict_types=1);

use DagaSmart\Edge\Support\EdgeAuthContext;
use DagaSmart\Edge\Support\RedisShardManager;
use Swow\Coroutine;
use Swow\Psr7\Server\Server;
use Swow\Signal;
use Swow\Socket;

require_once __DIR__.'/bootstrap.php';

// ---------- 配置常量 ----------
if (! defined('AUTH_PORT')) {
    define('AUTH_PORT', 9503);
}

// ---- 单实例兼容（未配置 EDGE_REDIS_SHARDS 时使用）----
if (! defined('REDIS_HOST')) {
    define('REDIS_HOST', '127.0.0.1');
}
if (! defined('REDIS_PORT')) {
    define('REDIS_PORT', 16379);
}
if (! defined('REDIS_PASSWORD')) {
    define('REDIS_PASSWORD', 'uApDVQD7');
}
if (! defined('REDIS_DB')) {
    define('REDIS_DB', 2);
}
if (! defined('REDIS_POOL_SIZE')) {
    define('REDIS_POOL_SIZE', 512);
}

// ---- 分片 Redis（优先）----
if (! defined('EDGE_REDIS_SHARDS')) {
    define('EDGE_REDIS_SHARDS', '');
}
if (! defined('EDGE_REDIS_POOL_SIZE')) {
    define('EDGE_REDIS_POOL_SIZE', 512);
}
if (! defined('EDGE_REDIS_DB')) {
    define('EDGE_REDIS_DB', 2);
}

if (! defined('MAX_CONCURRENCY')) {
    define('MAX_CONCURRENCY', 20000);
}
if (! defined('MAX_CONCURRENCY_PER_DEVICE')) {
    define('MAX_CONCURRENCY_PER_DEVICE', 2);
}
if (! defined('LOCAL_CACHE_TTL')) {
    define('LOCAL_CACHE_TTL', 60);
}
if (! defined('TIME_DRIFT')) {
    define('TIME_DRIFT', 300);
}
if (! defined('BACKLOG')) {
    define('BACKLOG', 4096);
}
if (! defined('ACCEPT_TIMEOUT_MS')) {
    define('ACCEPT_TIMEOUT_MS', 1000);
}
if (! defined('AUTH_HTTP_PREFIX')) {
    define('AUTH_HTTP_PREFIX', '/admin-api/extension/edge');
}

// ---------- 加载业务文件 ----------
require_once __DIR__.'/helper.php';
require_once __DIR__.'/hmac.php';
require_once __DIR__.'/auth.php';

// ---------- 初始化 ----------
$ready = false;
$poolReady = false;
$startTime = microtime(true);
$GLOBALS['startTime'] = $startTime;

try {
    RedisShardManager::init(EDGE_REDIS_POOL_SIZE, EDGE_REDIS_DB);

    $st = redisPoolStats();
    $mode = $st['mode'] ?? 'unknown';
    $shardInfo = $st['shards'] ?? [];

    $totalFree = 0;
    $totalCap = 0;
    foreach ($shardInfo as $shard) {
        $totalFree += $shard['free'] ?? 0;
        $totalCap += $shard['capacity'] ?? 0;
    }

    fwrite(
        STDOUT,
        sprintf(
            '[EdgeAuth] Redis pool initialized: mode=%s, shards=%d, free=%d/%d'.PHP_EOL,
            $mode,
            $st['shard_count'] ?? 1,
            $totalFree,
            $totalCap
        )
    );
} catch (Throwable $e) {
    fwrite(STDERR, '[EdgeAuth] Redis INIT FAILED: '.$e->getMessage().PHP_EOL);
}

localCacheInit();

$initialConcurrency = max(10, (int) ceil(MAX_CONCURRENCY / 5));
concurrencyInit($initialConcurrency);

if ($initialConcurrency < MAX_CONCURRENCY) {
    concurrencyProgressiveExpand(MAX_CONCURRENCY, 10, 300);
}

deviceConcurrencyInit(MAX_CONCURRENCY_PER_DEVICE, 300);

$initTime = microtime(true) - $startTime;
fwrite(STDOUT, '[EdgeAuth] Server READY (cold start, Redis pool filling in background)'.PHP_EOL);
fwrite(STDOUT, '[EdgeAuth] Init took '.round($initTime, 2).'s'.PHP_EOL);

// ---------- Server ----------
$server = new Server;

$server->setTcpNodelay(true)
    ->setTcpKeepAlive(true, 60)
    ->setRecvMessageTimeout(ACCEPT_TIMEOUT_MS);

$isWindows = strtoupper(substr(PHP_OS, 0, 3)) === 'WIN';
$bindFlags = $isWindows ? Socket::BIND_FLAG_NONE : Socket::BIND_FLAG_REUSEPORT;

$server->bind('0.0.0.0', AUTH_PORT, $bindFlags)
    ->listen(BACKLOG);

fwrite(
    STDOUT,
    '[EdgeAuth] Listening on 0.0.0.0:'.AUTH_PORT
    .($isWindows ? ' (single-process, no REUSEPORT)' : ' (REUSEPORT)').PHP_EOL
);

$workerId = $GLOBALS['worker_id'] ?? 0;
if (! $isWindows && $workerId > 0) {
    $delay = $workerId * 0.5;
    fwrite(STDOUT, "[EdgeAuth] Worker {$workerId} warm-up delay: {$delay}s".PHP_EOL);
    usleep((int) ($delay * 1000000));
}

$running = true;

$signalHandler = static function (int $signo) use (&$running, &$server): void {
    $name = match ($signo) {
        SIGTERM => 'SIGTERM',
        SIGINT => 'SIGINT',
        default => "signal {$signo}",
    };
    fwrite(STDOUT, "[EdgeAuth] {$name} received via Swow\Signal, shutting down".PHP_EOL);
    $running = false;

    try {
        $server->close();
    } catch (Throwable) {
    }
};

Coroutine::run(static function () use ($signalHandler): void {
    try {
        Signal::wait(SIGTERM);
    } catch (Throwable) {
    }
    $signalHandler(SIGTERM);
});

Coroutine::run(static function () use ($signalHandler): void {
    try {
        Signal::wait(SIGINT);
    } catch (Throwable) {
    }
    $signalHandler(SIGINT);
});

// ---------- 主循环 ----------
while ($running) {
    try {
        $server->setRecvMessageTimeout(1000);
        $conn = $server->acceptConnection();
    } catch (Throwable $e) {
        if (! $running) {
            break;
        }

        $msg = $e->getMessage();
        if (! str_contains($msg, 'closed') && ! str_contains($msg, 'reset') && ! str_contains($msg, 'timed out')) {
            fwrite(STDERR, '[accept] '.$msg.PHP_EOL);
        }

        continue;
    }

    Coroutine::run(static function () use ($conn, &$ready, $startTime): void {
        try {
            while (true) {
                try {
                    $req = $conn->recvHttpRequest();
                } catch (Throwable $e) {
                    $msg = $e->getMessage();
                    if (! str_contains($msg, 'closed') && ! str_contains($msg, 'reset') && ! str_contains($msg, 'timed out')) {
                        fwrite(STDERR, '[recv] '.$msg.PHP_EOL);
                    }
                    break;
                }

                // =========================================================
                // ★ 冷启动门禁（简化版）：全部 deny 直到池子 ready
                // =========================================================
                if (! $ready) {
                    if (redisPoolIsReady(0.8)) {
                        $ready = true;

                        // ---- 标准化 Gate OPEN 日志（只输出，不写入）----
                        $elapsed = round(microtime(true) - $GLOBALS['startTime'] ?? $startTime, 2);
                        $wid = $GLOBALS['worker_id'] ?? 0;
                        $stats = redisPoolStats();
                        $shards = $stats['shards'] ?? [];

                        $totalFree = 0;
                        $totalCap = 0;
                        foreach ($shards as $s) {
                            $totalFree += $s['free'] ?? 0;
                            $totalCap += $s['capacity'] ?? 0;
                        }

                        fwrite(STDOUT, sprintf(
                            '[EdgeAuth] [Worker %d] Gate OPEN | elapsed=%.2fs | pool=%d/%d (%.0f%%) | mode=%s'.PHP_EOL,
                            $wid,
                            $elapsed,
                            $totalFree,
                            $totalCap,
                            $totalCap > 0 ? ($totalFree / $totalCap * 100) : 0,
                            $stats['mode'] ?? 'unknown'
                        ));
                        // ---- 结束 ----
                    } else {
                        // 未就绪：所有请求直接 deny，NanoMQ 不会重试
                        respondJson($conn, [
                            'result'        => 'deny',
                            'is_superuser'  => false,
                            'code'          => 1,
                            'message'       => 'service_warming_up',
                            'data'          => [
                                'result' => 'deny',
                                'reason' => 'service_warming_up',
                            ],
                        ], 200);

                        // 短连接直接断开，避免空转
                        if (! $conn->shouldKeepAlive()) {
                            break;
                        }
                        continue;
                    }
                }

                $path = $req->getUri()->getPath();
                $path = '/'.ltrim($path, '/');
                $path = rtrim($path, '/');
                if ($path === '' || $path === '/') {
                    $path = '/';
                }

                try {
                    switch ($path) {
                        case '/admin-api/extension/edge/auth':
                            handleAuth($conn, $req);
                            break;

                        case '/admin-api/extension/edge/disconnect':
                            handleDisconnect($conn, $req);
                            break;

                        case '/admin-api/extension/edge/release':
                            handleRelease($conn, $req);
                            break;

                        case '/admin-api/extension/edge/super':
                            handleSuper($conn, $req);
                            break;

                        case '/admin-api/extension/edge/acl':
                            handleAcl($conn, $req);
                            break;

                        case '/admin-api/extension/edge/health':
                            $stats = redisPoolStats();
                            $uptime = microtime(true) - $GLOBALS['startTime'];

                            $shards = $stats['shards'] ?? [];
                            $totalFree = 0;
                            $totalCap = 0;
                            foreach ($shards as $shard) {
                                $totalFree += $shard['free'] ?? 0;
                                $totalCap += $shard['capacity'] ?? 0;
                            }

                            $poolRatio = $totalCap > 0
                                ? ($totalFree / $totalCap)
                                : 0;

                            $redisInfo = [
                                'mode' => $stats['mode'] ?? 'unknown',
                                'shard_count' => $stats['shard_count'] ?? 1,
                                'total' => $totalCap,
                                'free' => $totalFree,
                                'fill_ratio' => round($poolRatio, 2),
                                'shards' => array_map(
                                    static fn (array $s): array => [
                                        'free' => $s['free'] ?? 0,
                                        'capacity' => $s['capacity'] ?? 0,
                                        'host' => $s['config']['host'] ?? '',
                                        'port' => $s['config']['port'] ?? 0,
                                        'db' => $s['config']['db'] ?? 0,
                                    ],
                                    $shards
                                ),
                            ];

                            if (! $ready) {
                                respondJson($conn, [
                                    'status' => 'warming',
                                    'phase' => 'cold_start',
                                    'uptime_sec' => round($uptime, 2),
                                    'redis' => $redisInfo,
                                    'concurrency' => concurrencyStats(),
                                    'cache' => ['size' => localCacheStats()],
                                    'message' => 'Service is warming up, Redis pool still filling',
                                ], 503);
                            }

                            if ($poolRatio < 0.5) {
                                respondJson($conn, [
                                    'status' => 'degraded',
                                    'phase' => 'pool_draining',
                                    'uptime_sec' => round($uptime, 2),
                                    'redis' => $redisInfo,
                                    'concurrency' => concurrencyStats(),
                                    'cache' => ['size' => localCacheStats()],
                                ], 200);
                            }

                            respondJson($conn, [
                                'status' => 'ok',
                                'phase' => 'ready',
                                'uptime_sec' => round($uptime, 2),
                                'redis' => $redisInfo,
                                'concurrency' => concurrencyStats(),
                                'cache' => ['size' => localCacheStats()],
                            ], 200);
                            break;

                        case '/admin-api/extension/edge/metrics':
                            $m = metricsSnapshot();
                            $m['device_concurrency'] = deviceConcurrencyStats();
                            $m['global_concurrency'] = concurrencyStats();
                            $m['redis'] = redisPoolStats();
                            respondJson($conn, $m, 200);
                            break;

                        default:
                            respondJson($conn, [
                                'service' => 'EdgeAuth',
                                'status' => 'running',
                                'base' => AUTH_HTTP_PREFIX,
                                'method' => $req->getMethod(),
                                'path' => $path,
                                'endpoints' => [
                                    'auth' => AUTH_HTTP_PREFIX.'/auth (POST)',
                                    'super' => AUTH_HTTP_PREFIX.'/super (POST)',
                                    'acl' => AUTH_HTTP_PREFIX.'/acl (POST)',
                                    'disconnect' => AUTH_HTTP_PREFIX.'/disconnect (POST)',
                                    'release' => AUTH_HTTP_PREFIX.'/release (POST)',
                                    'health' => AUTH_HTTP_PREFIX.'/health (GET)',
                                    'metrics' => AUTH_HTTP_PREFIX.'/metrics (GET)',
                                ],
                            ], 200);
                            break;
                    }
                } catch (Throwable $innerE) {
                    fwrite(STDERR, '[handler-error] '.$innerE->getMessage().PHP_EOL);
                    try {
                        respondJson($conn, ['error' => 'internal', 'msg' => $innerE->getMessage()], 500);
                    } catch (Throwable) {
                        break;
                    }
                }

                if (! $conn->shouldKeepAlive()) {
                    break;
                }
            }
        } catch (Throwable $e) {
            $msg = $e->getMessage();
            if (! str_contains($msg, 'closed') && ! str_contains($msg, 'reset') && ! str_contains($msg, 'timed out')) {
                fwrite(STDERR, '[coroutine-fatal] '.$msg.PHP_EOL);
            }
        } finally {
            EdgeAuthContext::clear();

            try {
                $conn->close();
            } catch (Throwable) {
            }
        }
    });
}

fwrite(STDOUT, '[EdgeAuth] shutdown gracefully'.PHP_EOL);
