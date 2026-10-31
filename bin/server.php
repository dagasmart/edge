<?php

declare(strict_types=1);

use Dagasmart\Edge\Support\EdgeAuthContext;
use Swow\Coroutine;
use Swow\Psr7\Server\Server;
use Swow\Socket;

/**
 * EdgeAuth Swow Server（master-socket / per-worker REUSEPORT 版）
 *
 * 关键设计：
 *  - 不使用 Server::bind(REUSEPORT)
 *  - 每个 worker 自己创建 TCP socket
 *  - 每个 socket 都开启 SO_REUSEPORT
 *  - 内核负责连接分发
 */

// ---------- bootstrap ----------
$bootstrap = __DIR__ . '/bootstrap.php';
if (is_file($bootstrap) && ! function_exists('redisConnect')) {
    require_once $bootstrap;
}

// ---------- 配置兜底 ----------
if (! defined('AUTH_PORT')) {
    define('AUTH_PORT', 9503);
}
if (! defined('REDIS_HOST')) {
    define('REDIS_HOST', '127.0.0.1');
}
if (! defined('REDIS_PORT')) {
    define('REDIS_PORT', 16379);
}
if (! defined('REDIS_PASSWORD')) {
    define('REDIS_PASSWORD', '');
}
if (! defined('REDIS_DB')) {
    define('REDIS_DB', 2);
}
if (! defined('REDIS_POOL_SIZE')) {
    define('REDIS_POOL_SIZE', 64);
}
if (! defined('MAX_CONCURRENCY')) {
    define('MAX_CONCURRENCY', 20000);
}
if (! defined('MAX_CONCURRENCY_PER_DEVICE')) {
    define('MAX_CONCURRENCY_PER_DEVICE', 5);
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
if (! defined('AUTH_HTTP_PREFIX')) {
    define('AUTH_HTTP_PREFIX', '/admin-api/extension/edge');
}

// ---------- 业务文件 ----------
require_once __DIR__ . '/helper.php';
require_once __DIR__ . '/hmac.php';
require_once __DIR__ . '/auth.php';

// ---------- 当前 worker 编号 ----------
$workerId = (int) ($GLOBALS['worker_id'] ?? 0);

// =========================================================
// ★ 核心：每个 worker 独立创建 REUSEPORT socket
// =========================================================
function edgeCreateListenSocket(int $port, int $backlog): Socket
{
    $sock = new Socket(Socket::TYPE_TCP);

    // ---- SO_REUSEPORT（多 worker 端口复用） ----
    if (! (strtoupper(substr(PHP_OS, 0, 3)) === 'WIN')) {
        if (method_exists($sock, 'setReusePort')) {
            $sock->setReusePort(true);
        } elseif (method_exists($sock, 'setOption')) {
            $sock->setOption(SOL_SOCKET, SO_REUSEPORT, 1);
        }
    }

    // ---- SO_REUSEADDR（避免 TIME_WAIT 重启失败） ----
    if (method_exists($sock, 'setOption')) {
        $sock->setOption(SOL_SOCKET, SO_REUSEADDR, 1);
    }

    $sock->bind('0.0.0.0', $port);
    $sock->listen($backlog);

    return $sock;
}

// =========================================================
// 把 socket 交给 Swow Server（三重兜底）
// =========================================================
function edgeAdoptSocket(Server $server, Socket $sock): void
{
    if (method_exists($server, 'adoptSocket')) {
        $server->adoptSocket($sock);

        return;
    }

    if (method_exists($server, 'adopt')) {
        $server->adopt($sock);

        return;
    }

    // 兜底：利用 Server::listen 内部逻辑接管
    // 适用于 Swow 早期版本
    $ref = new ReflectionProperty($server, 'socket');
    $ref->setAccessible(true);
    $ref->setValue($server, $sock);
}

// ---------- 初始化 ----------
$ready = false;
$poolReady = false;
$startTime = microtime(true);

try {
    fwrite(STDOUT, sprintf(
        "[EdgeAuth] [worker=%d] Redis pool: %d-%d\n",
        $workerId,
        REDIS_POOL_SIZE,
        REDIS_DB
    ));

    redisPoolInit(REDIS_POOL_SIZE, REDIS_DB);

    $st = redisPoolStats();
    fwrite(STDOUT, sprintf(
        "[EdgeAuth] [worker=%d] Redis pool: %d/%d\n",
        $workerId,
        $st['free'],
        $st['total']
    ));
} catch (Throwable $e) {
    fwrite(STDERR, sprintf(
        "[EdgeAuth] [worker=%d] Redis DEGRADED: %s\n",
        $workerId,
        $e->getMessage()
    ));
}

localCacheInit();

// ★ 冷启动：先以 1/5 容量启动信号量
$initialConcurrency = max(10, (int) ceil(MAX_CONCURRENCY / 5));
concurrencyInit($initialConcurrency);

if ($initialConcurrency < MAX_CONCURRENCY) {
    concurrencyProgressiveExpand(MAX_CONCURRENCY, 10, 300);
}

deviceConcurrencyInit(MAX_CONCURRENCY_PER_DEVICE, 300);

// ---------- 错峰预热 ----------
if ($workerId > 0) {
    $delay = $workerId * 0.3;
    fwrite(STDOUT, sprintf(
        "[EdgeAuth] [worker=%d] warm-up delay: %.2fs\n",
        $workerId,
        $delay
    ));
    usleep((int) ($delay * 1_000_000));
}

// ---------- Server ----------
$server = new Server;
$server->setTcpNodelay(true)
    ->setTcpKeepAlive(true, 60)
    ->setRecvMessageTimeout(30000);

$listenSocket = edgeCreateListenSocket(AUTH_PORT, BACKLOG);
edgeAdoptSocket($server, $listenSocket);

fwrite(STDOUT, sprintf(
    "[EdgeAuth] [worker=%d] Listening on 0.0.0.0:%d (REUSEPORT, pid=%d)\n",
    $workerId,
    AUTH_PORT,
    getmypid()
));

// ---------- 优雅退出 ----------
$running = true;
if (function_exists('pcntl_signal')) {
    pcntl_async_signals(true);
    pcntl_signal(SIGTERM, static function () use (&$running, $workerId): void {
        fwrite(STDOUT, sprintf(
            "[EdgeAuth] [worker=%d] SIGTERM received, shutting down\n",
            $workerId
        ));
        $running = false;
    });
    pcntl_signal(SIGINT, static function () use (&$running, $workerId): void {
        fwrite(STDOUT, sprintf(
            "[EdgeAuth] [worker=%d] SIGINT received, shutting down\n",
            $workerId
        ));
        $running = false;
    });
}

// ---------- 主循环 ----------
while ($running) {
    try {
        $conn = $server->acceptConnection();
    } catch (Throwable $e) {
        $msg = $e->getMessage();
        if (! str_contains($msg, 'closed')
            && ! str_contains($msg, 'reset')
            && ! str_contains($msg, 'timed out')) {
            fwrite(STDERR, sprintf(
                "[EdgeAuth] [worker=%d] [accept] %s\n",
                $workerId,
                $msg
            ));
        }
        continue;
    }

    Coroutine::run(static function () use ($conn, &$ready, &$poolReady, $startTime, $workerId): void {
        try {
            while (true) {
                try {
                    $req = $conn->recvHttpRequest();
                } catch (Throwable $e) {
                    $msg = $e->getMessage();
                    if (! str_contains($msg, 'closed')
                        && ! str_contains($msg, 'reset')
                        && ! str_contains($msg, 'timed out')) {
                        fwrite(STDERR, sprintf(
                            "[EdgeAuth] [worker=%d] [recv] %s\n",
                            $workerId,
                            $msg
                        ));
                    }
                    break;
                }

                // ★ 冷启动门禁
                if (! $ready) {
                    $uptime = microtime(true) - $startTime;

                    if (redisPoolIsReady(0.8)) {
                        $ready = true;
                        $poolReady = true;
                        fwrite(STDOUT, sprintf(
                            "[EdgeAuth] [worker=%d] Pool reached 80%%, gate OPEN\n",
                            $workerId
                        ));
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
                $path = '/' . ltrim($path, '/');
                $path = rtrim($path, '/');
                if ($path === '') {
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
                            $poolRatio = REDIS_POOL_SIZE > 0
                                ? ($stats['free'] / REDIS_POOL_SIZE)
                                : 0;

                            if (! $ready) {
                                respondJson($conn, [
                                    'status' => 'warming',
                                    'phase' => 'cold_start',
                                    'worker_id' => $workerId,
                                    'uptime_sec' => round($uptime, 2),
                                    'redis' => [
                                        'total' => $stats['total'],
                                        'free' => $stats['free'],
                                        'fill_ratio' => round($poolRatio, 2),
                                        'target_size' => REDIS_POOL_SIZE,
                                    ],
                                    'concurrency' => concurrencyStats(),
                                    'cache' => ['size' => localCacheStats()],
                                    'message' => 'Service is warming up',
                                ], 503);
                            } elseif ($poolRatio < 0.5) {
                                respondJson($conn, [
                                    'status' => 'degraded',
                                    'phase' => 'pool_draining',
                                    'worker_id' => $workerId,
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
                                    'worker_id' => $workerId,
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
                            $m['worker_id'] = $workerId;
                            $m['device_concurrency'] = deviceConcurrencyStats();
                            $m['global_concurrency'] = concurrencyStats();
                            respondJson($conn, $m, 200);
                            break;

                        default:
                            respondJson($conn, [
                                'service' => 'EdgeAuth',
                                'status' => 'running',
                                'worker_id' => $workerId,
                                'base' => AUTH_HTTP_PREFIX,
                                'method' => $req->getMethod(),
                                'path' => $path,
                                'endpoints' => [
                                    'auth' => AUTH_HTTP_PREFIX . '/auth (POST)',
                                    'super' => AUTH_HTTP_PREFIX . '/super (POST)',
                                    'acl' => AUTH_HTTP_PREFIX . '/acl (POST)',
                                    'health' => AUTH_HTTP_PREFIX . '/health (GET)',
                                    'metrics' => AUTH_HTTP_PREFIX . '/metrics (GET)',
                                ],
                            ], 200);
                            break;
                    }
                } catch (Throwable $innerE) {
                    fwrite(STDERR, sprintf(
                        "[EdgeAuth] [worker=%d] [handler-error] %s\n",
                        $workerId,
                        $innerE->getMessage()
                    ));
                    try {
                        respondJson($conn, [
                            'error' => 'internal',
                            'msg' => $innerE->getMessage(),
                        ], 500);
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
            if (! str_contains($msg, 'closed')
                && ! str_contains($msg, 'reset')
                && ! str_contains($msg, 'timed out')) {
                fwrite(STDERR, sprintf(
                    "[EdgeAuth] [worker=%d] [coroutine-fatal] %s\n",
                    $workerId,
                    $msg
                ));
            }
        } finally {
            try {
                $authedUsername = EdgeAuthContext::getUsername();
                $authedClientId = EdgeAuthContext::getClientId();
                if ($authedUsername !== null && $authedClientId !== null) {
                    releaseUserOnline($authedUsername, $authedClientId);
                }
            } catch (Throwable $e) {
                fwrite(STDERR, sprintf(
                    "[EdgeAuth] [worker=%d] [release-online] %s\n",
                    $workerId,
                    $e->getMessage()
                ));
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

fwrite(STDOUT, sprintf(
    "[EdgeAuth] [worker=%d] shutdown gracefully\n",
    $workerId
));
