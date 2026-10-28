<?php

declare(strict_types=1);

use Swow\Coroutine;
use Swow\Psr7\Server\Server;

// ---------- 配置常量 ----------
if (!defined('AUTH_PORT')) {
    define('AUTH_PORT', 9503);
}
if (!defined('REDIS_HOST')) {
    define('REDIS_HOST', '127.0.0.1');
}
if (!defined('REDIS_PORT')) {
    define('REDIS_PORT', 16379);
}
if (!defined('REDIS_PASSWORD')) {
    define('REDIS_PASSWORD', 'uApDVQD7');
}
if (!defined('REDIS_DB')) {
    define('REDIS_DB', 2);
}
if (!defined('REDIS_POOL_SIZE')) {
    define('REDIS_POOL_SIZE', 256);
}
if (!defined('MAX_CONCURRENCY')) {
    define('MAX_CONCURRENCY', 10000);
}
if (!defined('CACHE_TTL')) {
    define('CACHE_TTL', 300);
}
if (!defined('LOCAL_CACHE_TTL')) {
    define('LOCAL_CACHE_TTL', 60);
}
if (!defined('RATE_MAX')) {
    define('RATE_MAX', (int) ($_ENV['RATE_MAX'] ?? 10));
}
if (!defined('RATE_TTL')) {
    define('RATE_TTL', (int) ($_ENV['RATE_TTL'] ?? 60));
}
if (!defined('TIME_DRIFT')) {
    define('TIME_DRIFT', (int) ($_ENV['TIME_DRIFT'] ?? 300));
}
if (!defined('BACKLOG')) {
    define('BACKLOG', (int) ($_ENV['BACKLOG'] ?? 4096));
}

// ---------- 加载业务文件 ----------
require_once __DIR__ . '/helper.php';
require_once __DIR__ . '/hmac.php';
require_once __DIR__ . '/auth.php';

// ---------- 初始化 ----------
$redisOk = false;
try {
    redisPoolInit(REDIS_POOL_SIZE, REDIS_DB);
    $st = redisPoolStats();
    $redisOk = ($st['free'] ?? 0) > 0;
    fwrite(STDOUT, '[EdgeAuth] Redis pool: ' . $st['free'] . '/' . $st['total'] . PHP_EOL);
} catch (Throwable $e) {
    fwrite(STDERR, '[EdgeAuth] Redis DEGRADED: ' . $e->getMessage() . PHP_EOL);
}

localCacheInit();
concurrencyInit(MAX_CONCURRENCY);

// ---------- Server ----------
$server = new Server;

// ✅ Swow Psr7\Server 正确配置：链式方法 + listen(backlog)
$server->setTcpNodelay(true)
    ->setTcpKeepAlive(true, 60)
    ->setRecvMessageTimeout(30000);

$server->bind('0.0.0.0', AUTH_PORT, Server::BIND_FLAG_NONE)
    ->listen(BACKLOG);

fwrite(STDOUT, '[EdgeAuth] Listening on 0.0.0.0:' . AUTH_PORT . PHP_EOL);
fwrite(STDOUT, '[EdgeAuth] Redis: ' . ($redisOk ? 'OK' : 'DEGRADED') . PHP_EOL);

// ---------- 优雅退出（pcntl，不依赖 Swow\Signal 版本差异） ----------
$running = true;

if (function_exists('pcntl_signal')) {
    pcntl_async_signals(true);
    pcntl_signal(SIGTERM, function () use (&$running): void {
        fwrite(STDOUT, '[EdgeAuth] SIGTERM received, shutting down' . PHP_EOL);
        $running = false;
    });
    pcntl_signal(SIGINT, function () use (&$running): void {
        fwrite(STDOUT, '[EdgeAuth] SIGINT received, shutting down' . PHP_EOL);
        $running = false;
    });
    fwrite(STDOUT, '[EdgeAuth] Signal handler: pcntl' . PHP_EOL);
} else {
    fwrite(STDOUT, '[EdgeAuth] Signal handler: none (no pcntl)' . PHP_EOL);
}

// ---------- 主循环 ----------
while ($running) {
    try {
        $conn = $server->acceptConnection();
    } catch (Throwable $e) {
        if (!$running) {
            break;
        }
        continue;
    }

    Coroutine::run(static function () use ($conn): void {
        try {
            while (true) {
                $req = $conn->recvHttpRequest();
                $path = $req->getUri()->getPath();

                switch ($path) {
                    case '/mqtt/auth':
                        handleAuth($conn, $req);
                        break;

                    case '/health':
                        try {
                            $health = [
                                'status' => ($GLOBALS['redisOk'] ?? false) ? 'ok' : 'degraded',
                                'redis'  => redisPoolStats(),
                                'cache'  => ['size' => localCacheStats()],
                            ];
                            $conn->respond(
                                json_encode($health, JSON_UNESCAPED_SLASHES),
                                200,
                                ['Content-Type' => 'application/json']
                            );
                        } catch (Throwable $e) {
                            $conn->respond(
                                json_encode(['status' => 'error', 'msg' => $e->getMessage()], JSON_UNESCAPED_SLASHES),
                                500,
                                ['Content-Type' => 'application/json']
                            );
                        }
                        break;

                    case '/metrics':
                        $conn->respond(
                            json_encode(metricsSnapshot(), JSON_UNESCAPED_SLASHES),
                            200,
                            ['Content-Type' => 'application/json']
                        );
                        break;

                    default:
                        $conn->respond('', 404);
                }

                if (!$conn->shouldKeepAlive()) {
                    break;
                }
            }
        } catch (Throwable $e) {
            fwrite(STDERR, '[conn-fatal] ' . $e->getMessage() . PHP_EOL);
        } finally {
            try {
                $conn->close();
            } catch (Throwable) {
                // ignore
            }
        }
    });
}

fwrite(STDOUT, '[EdgeAuth] shutdown gracefully' . PHP_EOL);
