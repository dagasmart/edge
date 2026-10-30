<?php
declare(strict_types=1);

use Swow\Coroutine;
use Swow\Psr7\Server\Server;
use Swow\Socket;

require_once __DIR__.'/bootstrap.php';

// ---------- 配置常量 ----------
if (! defined('AUTH_PORT')) { define('AUTH_PORT', 9503); }
if (! defined('REDIS_HOST')) { define('REDIS_HOST', '127.0.0.1'); }
if (! defined('REDIS_PORT')) { define('REDIS_PORT', 16379); }
if (! defined('REDIS_PASSWORD')) { define('REDIS_PASSWORD', 'uApDVQD7'); }
if (! defined('REDIS_DB')) { define('REDIS_DB', 2); }
if (! defined('REDIS_POOL_SIZE')) { define('REDIS_POOL_SIZE', 512); }
if (! defined('MAX_CONCURRENCY')) { define('MAX_CONCURRENCY', 20000); }
if (! defined('MAX_CONCURRENCY_PER_DEVICE')) { define('MAX_CONCURRENCY_PER_DEVICE', 2); }
if (! defined('LOCAL_CACHE_TTL')) { define('LOCAL_CACHE_TTL', 60); }
if (! defined('TIME_DRIFT')) { define('TIME_DRIFT', 300); }
if (! defined('BACKLOG')) { define('BACKLOG', 4096); }
if (! defined('AUTH_HTTP_PREFIX')) { define('AUTH_HTTP_PREFIX', '/admin-api/extension/edge'); }

// ---------- 加载业务文件 ----------
require_once __DIR__.'/helper.php';
require_once __DIR__.'/hmac.php';
require_once __DIR__.'/auth.php';

// ---------- 初始化 ----------
$ready = false;              // 冷启动门禁
$poolReady = false;          // Redis 池就绪标记（实时更新）
$startTime = microtime(true);

try {
    fwrite(STDOUT, '[EdgeAuth] Redis pool: '.REDIS_POOL_SIZE.'-'.REDIS_DB.PHP_EOL);
    redisPoolInit(REDIS_POOL_SIZE, REDIS_DB);
    $st = redisPoolStats();
    fwrite(STDOUT, '[EdgeAuth] Redis pool: '.$st['free'].'/'.$st['total'].PHP_EOL);
} catch (Throwable $e) {
    fwrite(STDERR, '[EdgeAuth] Redis DEGRADED: '.$e->getMessage().PHP_EOL);
}

localCacheInit();

// ★ 冷启动：先以 1/5 容量启动信号量
$initialConcurrency = max(10, (int) ceil(MAX_CONCURRENCY / 5));
concurrencyInit($initialConcurrency);

// ★ 后台渐进扩容到满容量
if ($initialConcurrency < MAX_CONCURRENCY) {
    concurrencyProgressiveExpand(MAX_CONCURRENCY, 10, 300);
}

deviceConcurrencyInit(MAX_CONCURRENCY_PER_DEVICE, 300);

// ---------- 冷启动：不阻塞等待，靠门禁 + health 实时感知 ----------
$initTime = microtime(true) - $startTime;
fwrite(STDOUT, '[EdgeAuth] Server READY (cold start, Redis pool filling in background)'.PHP_EOL);
fwrite(STDOUT, '[EdgeAuth] Pool: 3 probed, background filling to '.REDIS_POOL_SIZE.' total'.PHP_EOL);
fwrite(STDOUT, '[EdgeAuth] Init took '.round($initTime, 2).'s'.PHP_EOL);

// ---------- Server ----------
$server = new Server;
$server->setTcpNodelay(true)
    ->setTcpKeepAlive(true, 60)
    ->setRecvMessageTimeout(30000);

$isWindows = strtoupper(substr(PHP_OS, 0, 3)) === 'WIN';
$bindFlags = $isWindows ? Socket::BIND_FLAG_NONE : Socket::BIND_FLAG_REUSEPORT;

$server->bind('0.0.0.0', AUTH_PORT, $bindFlags)
    ->listen(BACKLOG);

fwrite(STDOUT, '[EdgeAuth] Listening on 0.0.0.0:'.AUTH_PORT
    .($isWindows ? ' (single-process, no REUSEPORT)' : ' (REUSEPORT)').PHP_EOL);

// ---------- Worker 错峰就绪 ----------
$workerId = $GLOBALS['worker_id'] ?? 0;
if (!$isWindows && $workerId > 0) {
    $delay = $workerId * 0.5;
    fwrite(STDOUT, "[EdgeAuth] Worker {$workerId} warm-up delay: {$delay}s".PHP_EOL);
    usleep((int)($delay * 1000000));
}

// ---------- 优雅退出 ----------
$running = true;
if (function_exists('pcntl_signal')) {
    pcntl_async_signals(true);
    pcntl_signal(SIGTERM, function () use (&$running): void {
        fwrite(STDOUT, '[EdgeAuth] SIGTERM received, shutting down'.PHP_EOL);
        $running = false;
    });
    pcntl_signal(SIGINT, function () use (&$running): void {
        fwrite(STDOUT, '[EdgeAuth] SIGINT received, shutting down'.PHP_EOL);
        $running = false;
    });
}

// ---------- 主循环 ----------
while ($running) {
    try {
        $conn = $server->acceptConnection();
    } catch (Throwable $e) {
        $msg = $e->getMessage();
        if (! str_contains($msg, 'closed') && ! str_contains($msg, 'reset') && ! str_contains($msg, 'timed out')) {
            fwrite(STDERR, '[accept] '.$msg.PHP_EOL);
        }
        continue;
    }

    Coroutine::run(static function () use ($conn, &$ready, &$poolReady, $startTime): void {
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

                // ★★★ 冷启动门禁：渐进放行 ★★★
                if (!$ready) {
                    $uptime = microtime(true) - $startTime;

                    // ★ 实时判断池子是否满 80%
                    if (redisPoolIsReady(0.8)) {
                        $ready = true;
                        $poolReady = true;
                        fwrite(STDOUT, '[EdgeAuth] Pool reached 80%, gate OPEN'.PHP_EOL);
                    } else {
                        // 池子未满：按时间比例放行
                        $allowRatio = min(1.0, $uptime / 3.0 * 0.3);
                        if ($uptime < 3.0 && mt_rand(1, 100) / 100 > $allowRatio) {
                            respondJson($conn, [
                                'result' => 'deny',
                                'reason' => 'service_warming_up',
                                'retry_after' => 1,
                            ], 503);
                            continue;
                        }
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
                        case '/admin-api/extension/edge/super':
                            handleSuper($conn, $req);
                            break;
                        case '/admin-api/extension/edge/acl':
                            handleAcl($conn, $req);
                            break;

                        // ---------- 健康检查（三态，实时感知）----------
                        case '/admin-api/extension/edge/health':
                            $stats = redisPoolStats();
                            $uptime = microtime(true) - $startTime;
                            $poolRatio = REDIS_POOL_SIZE > 0
                                ? ($stats['free'] / REDIS_POOL_SIZE)
                                : 0;

                            if (!$ready) {
                                respondJson($conn, [
                                    'status' => 'warming',
                                    'phase' => 'cold_start',
                                    'uptime_sec' => round($uptime, 2),
                                    'redis' => [
                                        'total' => $stats['total'],
                                        'free' => $stats['free'],
                                        'fill_ratio' => round($poolRatio, 2),
                                        'target_size' => REDIS_POOL_SIZE,
                                    ],
                                    'concurrency' => concurrencyStats(),
                                    'cache' => ['size' => localCacheStats()],
                                    'message' => 'Service is warming up, Redis pool still filling',
                                ], 503);
                            } elseif ($poolRatio < 0.5) {
                                respondJson($conn, [
                                    'status' => 'degraded',
                                    'phase' => 'pool_draining',
                                    'uptime_sec' => round($uptime, 2),
                                    'redis' => [
                                        'total' => $stats['total'],
                                        'free' => $stats['free'],
                                        'fill_ratio' => round($poolRatio, 2),
                                    ],
                                    'concurrency' => concurrencyStats(),
                                    'cache' => ['size' => localCacheStats()],
                                ], 200);
                            } else {
                                respondJson($conn, [
                                    'status' => 'ok',
                                    'phase' => 'ready',
                                    'uptime_sec' => round($uptime, 2),
                                    'redis' => [
                                        'total' => $stats['total'],
                                        'free' => $stats['free'],
                                        'fill_ratio' => round($poolRatio, 2),
                                    ],
                                    'concurrency' => concurrencyStats(),
                                    'cache' => ['size' => localCacheStats()],
                                ], 200);
                            }
                            break;

                        case '/admin-api/extension/edge/metrics':
                            $m = metricsSnapshot();
                            $m['device_concurrency'] = deviceConcurrencyStats();
                            $m['global_concurrency'] = concurrencyStats();
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
            try {
                $conn->close();
            } catch (Throwable) {
            }
        }
    });
}

fwrite(STDOUT, '[EdgeAuth] shutdown gracefully'.PHP_EOL);
