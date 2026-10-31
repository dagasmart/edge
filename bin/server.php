<?php

declare(strict_types=1);

use Swow\Coroutine;
use Swow\Signal;
use Swow\Socket;
use Swow\SocketException;

// ---------- 配置常量 ----------
const AUTH_PORT = 9503;
const AUTH_HTTP_PREFIX = '/admin-api/extension/edge';
const REDIS_POOL_SIZE = 512;
const REDIS_POOL_PROBE = 3;
const REDIS_POOL_TIMEOUT = 2.0;
const REDIS_POOL_FILL_INTERVAL = 10000; // 10ms，微秒
const REDIS_RETRY_INTERVAL = 100000;    // 100ms，微秒
const CONCURRENCY_LIMIT = 4000;
const DEVICE_CONCURRENCY_LIMIT = 16;
const LOCAL_CACHE_SIZE = 5000;
const LOCAL_CACHE_TTL = 30;
const METRICS_RESET_INTERVAL = 300;
const SESSION_TTL = 3600;
const AUTH_TIMEOUT = 3.0;
const SUPER_TIMEOUT = 3.0;
const ACL_TIMEOUT = 3.0;

// ---------- 全局状态 ----------
$running = true;
$ready = false;
$poolReady = false;
$startTime = microtime(true);

// ---------- 信号处理：只置标志，不依赖二次 wait ----------
// 用循环 wait，确保每次信号都能被消费
Coroutine::run(static function () use (&$running): void {
    while ($running) {
        try {
            Signal::wait([SIGTERM, SIGINT]);
        } catch (Throwable) {
            break;
        }
        $running = false;
        // 不要 break，让协程退出循环自然结束
    }
});

// ---------- 启动服务器 ----------
$server = new Socket(Socket::TYPE_TCP);
$server->setReusePort(true)->bind('0.0.0.0', AUTH_PORT)->listen();

fwrite(STDOUT, '[EdgeAuth] Listening on 0.0.0.0:'.AUTH_PORT.' (REUSEPORT)'.PHP_EOL);

// ---------- 主循环：用 try/catch 接 close() 抛出的异常 ----------
while ($running) {
    try {
        $conn = $server->acceptConnection();
    } catch (SocketException $e) {
        // close() 被调用时 accept 会抛异常，这里统一处理
        if (! $running) {
            fwrite(STDOUT, '[EdgeAuth] Server closed, exiting main loop'.PHP_EOL);
            break;
        }
        // 非关闭导致的异常，记录并继续
        $msg = $e->getMessage();
        if (! str_contains($msg, 'closed') && ! str_contains($msg, 'reset')) {
            fwrite(STDERR, '[accept] '.$msg.PHP_EOL);
        }
        continue;
    } catch (Throwable $e) {
        if (! $running) {
            break;
        }
        fwrite(STDERR, '[accept-fatal] '.$e->getMessage().PHP_EOL);
        continue;
    }

    Coroutine::run(static function () use ($conn, &$ready, &$poolReady, $startTime, &$running): void {
        try {
            while ($running) {
                try {
                    $req = $conn->recvHttpRequest();
                } catch (Throwable $e) {
                    $msg = $e->getMessage();
                    if (! str_contains($msg, 'closed') && ! str_contains($msg, 'reset') && ! str_contains($msg, 'timed out')) {
                        fwrite(STDERR, '[recv] '.$msg.PHP_EOL);
                    }
                    break;
                }

                if (! $ready) {
                    $uptime = microtime(true) - $startTime;

                    if (redisPoolIsReady(0.8)) {
                        $ready = true;
                        $poolReady = true;
                        fwrite(STDOUT, '[EdgeAuth] Pool reached 80%, gate OPEN'.PHP_EOL);
                    } else {
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
                        case '/admin-api/extension/edge/health':
                            $stats = redisPoolStats();
                            $uptime = microtime(true) - $startTime;
                            $poolRatio = REDIS_POOL_SIZE > 0 ? ($stats['free'] / REDIS_POOL_SIZE) : 0;

                            if (! $ready) {
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
                $authedUsername = EdgeAuthContext::getUsername();
                $authedClientId = EdgeAuthContext::getClientId();
                if ($authedUsername !== null && $authedClientId !== null) {
                    releaseUserOnline($authedUsername, $authedClientId);
                }
            } catch (Throwable $e) {
                fwrite(STDERR, '[release-online] '.$e->getMessage().PHP_EOL);
            } finally {
                EdgeAuthContext::clear();
            }

            try {
                $conn->close();
            } catch (Throwable) {
            }
        }
    });
}

// ---------- 优雅关闭：关闭监听 socket，打断 accept，等待连接处理完 ----------
$server->close();

// 给正在处理的协程一点时间完成
$waitStart = microtime(true);
while (microtime(true) - $waitStart < 2.0) {
    // 短暂让出 CPU，让协程有机会完成
    Coroutine::yield();
}

fwrite(STDOUT, '[EdgeAuth] shutdown gracefully'.PHP_EOL);
