<?php

declare(strict_types=1);

use Swow\Coroutine;
use Swow\Psr7\Server\Server;
use Swow\Socket;

require_once __DIR__.'/bootstrap.php'; // 第一行引入
// 后面的 define 检查会自然跳过，因为 bootstrap 已经有 define

// ---------- 配置常量 （bootstrap.php已配置全局常量声明，以下define部分可删可留）----------
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
    define('REDIS_PASSWORD', 'uApDVQD7');
}
if (! defined('REDIS_DB')) {
    define('REDIS_DB', 2);
}
if (! defined('REDIS_POOL_SIZE')) {
    define('REDIS_POOL_SIZE', 256);
}
if (! defined('MAX_CONCURRENCY')) {
    define('MAX_CONCURRENCY', 10000);
}
// REUSEPORT 模式下 4 worker 各独立计数，设 2 等效全局 ~8 并发/设备
if (! defined('MAX_CONCURRENCY_PER_DEVICE')) {
    define('MAX_CONCURRENCY_PER_DEVICE', 2);
}
if (! defined('CACHE_TTL')) {
    define('CACHE_TTL', 300);
}
if (! defined('LOCAL_CACHE_TTL')) {
    define('LOCAL_CACHE_TTL', 60);
}
if (! defined('RATE_MAX')) {
    define('RATE_MAX', 10);
}
if (! defined('RATE_TTL')) {
    define('RATE_TTL', 60);
}
if (! defined('TIME_DRIFT')) {
    define('TIME_DRIFT', 300);
}
if (! defined('BACKLOG')) {
    define('BACKLOG', 4096);
}
// 路由基准前缀
if (! defined('AUTH_HTTP_PREFIX')) {
    define('AUTH_HTTP_PREFIX', '/admin-api/extension/edge/gateway');
}

// ---------- 加载业务文件 ----------
require_once __DIR__.'/helper.php';
require_once __DIR__.'/hmac.php';
require_once __DIR__.'/auth.php';

// ---------- 初始化 ----------
$redisOk = false;

try {
    fwrite(STDOUT, '[EdgeAuth] Redis pool: '.REDIS_POOL_SIZE.'-'.REDIS_DB.PHP_EOL);
    redisPoolInit(REDIS_POOL_SIZE, REDIS_DB);
    $st = redisPoolStats();
    $redisOk = ($st['free'] ?? 0) > 0;
    fwrite(STDOUT, '[EdgeAuth] Redis pool: '.$st['free'].'/'.$st['total'].PHP_EOL);
} catch (Throwable $e) {
    fwrite(STDERR, '[EdgeAuth] Redis DEGRADED: '.$e->getMessage().PHP_EOL);
}

localCacheInit();
concurrencyInit(MAX_CONCURRENCY);
deviceConcurrencyInit(MAX_CONCURRENCY_PER_DEVICE, 300);

// ---------- Server ----------
$server = new Server;

$server->setTcpNodelay(true)
    ->setTcpKeepAlive(true, 60)
    ->setRecvMessageTimeout(30000);

// REUSEPORT：4 个 worker 进程全部监听同一端口，内核层 round-robin 分发
// 修正：BIND_FLAG_REUSEPORT（原 BIND_FLAG_NONE 为默认值 0，无实际效果）
$isWindows = strtoupper(substr(PHP_OS, 0, 3)) === 'WIN';
$bindFlags = $isWindows
    ? Socket::BIND_FLAG_NONE
    : Socket::BIND_FLAG_REUSEPORT;

$server->bind('0.0.0.0', AUTH_PORT, $bindFlags)
    ->listen(BACKLOG);

fwrite(STDOUT, '[EdgeAuth] Listening on 0.0.0.0:'.AUTH_PORT
    .($isWindows ? ' (single-process, no REUSEPORT)' : ' (REUSEPORT)')
    .PHP_EOL);

fwrite(STDOUT, '[EdgeAuth] Route base: '.AUTH_HTTP_PREFIX.PHP_EOL);
fwrite(STDOUT, '[EdgeAuth] Redis: '.($redisOk ? 'OK' : 'DEGRADED').PHP_EOL);
fwrite(STDOUT, '[EdgeAuth] Concurrency: max='.MAX_CONCURRENCY.' global, '.MAX_CONCURRENCY_PER_DEVICE.'/device'.PHP_EOL);

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
    fwrite(STDOUT, '[EdgeAuth] Signal handler: pcntl'.PHP_EOL);
} else {
    fwrite(STDOUT, '[EdgeAuth] Signal handler: none (no pcntl)'.PHP_EOL);
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
            fwrite(STDERR, '[accept] '.$msg.PHP_EOL);
        }

        continue;
    }

    Coroutine::run(static function () use ($conn): void {
        try {
            while (true) {
                try {
                    $req = $conn->recvHttpRequest();
                } catch (Throwable $e) {
                    $msg = $e->getMessage();
                    if (! str_contains($msg, 'closed')
                        && ! str_contains($msg, 'reset')
                        && ! str_contains($msg, 'timed out')) {
                        fwrite(STDERR, '[recv] '.$msg.PHP_EOL);
                    }
                    break;
                }

                $path = $req->getUri()->getPath();
                $path = '/'.ltrim($path, '/');
                $path = rtrim($path, '/');
                if ($path === '' || $path === '/') {
                    $path = '/';
                }

                fwrite(STDERR, '[route] path=['.$path.'] method='.$req->getMethod().PHP_EOL);

                try {
                    switch ($path) {
                        case '/admin-api/extension/edge/gateway/auth':
                            handleAuth($conn, $req);
                            break;

                        case '/admin-api/extension/edge/gateway/super':
                            handleSuper($conn, $req);
                            break;

                        case '/admin-api/extension/edge/gateway/acl':
                            handleAcl($conn, $req);
                            break;

                        case '/admin-api/extension/edge/gateway/health':
                            $stats = redisPoolStats();
                            respondJson($conn, [
                                'status' => ($stats['free'] ?? 0) > 0 ? 'ok' : 'degraded',
                                'redis' => $stats,
                                'cache' => ['size' => localCacheStats()],
                            ], 200);
                            break;

                        case '/admin-api/extension/edge/gateway/metrics':
                            $m = metricsSnapshot();
                            $m['device_concurrency'] = deviceConcurrencyStats();
                            $m['global_concurrency'] = concurrencyStats();
                            respondJson($conn, $m, 200);
                            break;

                        default:
                            fwrite(STDERR, '[route] 404 for ['.$path.']'.PHP_EOL);
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
                                'message' => 'EdgeAuth is running. Call a specific endpoint.',
                            ], 200);
                            break;
                    }
                } catch (Throwable $innerE) {
                    fwrite(STDERR, '[handler-error] '.$innerE->getMessage().PHP_EOL);
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
