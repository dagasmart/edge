<?php

declare(strict_types=1);

use Swow\Coroutine;
use Swow\Psr7\Server\Server;
use Swow\Signal;

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
if (!defined('MAX_REQUESTS_PER_CONN')) {
    define('MAX_REQUESTS_PER_CONN', (int) ($_ENV['MAX_REQUESTS_PER_CONN'] ?? 10000));
}
if (!defined('MAX_CONNECTIONS')) {
    define('MAX_CONNECTIONS', (int) ($_ENV['MAX_CONNECTIONS'] ?? 5000));
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

// ✅ 必须设置，否则 500+ 连接 accept 队列满直接 RST
$server->set([
    'max_connections' => MAX_CONNECTIONS,
    'backlog'          => BACKLOG,
    'max_requests'     => 0,
]);

// TLS（可选）
if (defined('TLS_CERT') && defined('TLS_KEY') && file_exists(TLS_CERT) && file_exists(TLS_KEY)) {
    $server->setSslCertFile(TLS_CERT);
    $server->setSslKeyFile(TLS_KEY);
    fwrite(STDOUT, '[EdgeAuth] TLS enabled' . PHP_EOL);
}

$server->bind('0.0.0.0', AUTH_PORT, Server::BIND_FLAG_NONE)->listen();

fwrite(STDOUT, '[EdgeAuth] Listening on 0.0.0.0:' . AUTH_PORT . PHP_EOL);
fwrite(STDOUT, '[EdgeAuth] Redis: ' . ($redisOk ? 'OK' : 'DEGRADED') . PHP_EOL);

// ---------- 优雅退出（三级降级） ----------
$running = true;
$signalMode = 'none';

if (class_exists('Swow\Signal')) {
    try {
        $sigMask = Signal::TERM | Signal::INT;
        $signal = new Signal($sigMask);
        Coroutine::run(static function () use ($signal, &$running): void {
            $signal->wait();
            fwrite(STDOUT, '[EdgeAuth] received SIGTERM/SIGINT, shutting down' . PHP_EOL);
            $running = false;
        });
        $signalMode = 'swow';
    } catch (Throwable $e) {
        fwrite(STDERR, '[EdgeAuth] Swow\Signal unavailable (' . $e->getMessage() . '), falling back' . PHP_EOL);
    }
}

if ($signalMode === 'none' && function_exists('pcntl_signal')) {
    pcntl_async_signals(true);
    pcntl_signal(SIGTERM, function () use (&$running): void {
        fwrite(STDOUT, '[EdgeAuth] SIGTERM' . PHP_EOL);
        $running = false;
    });
    pcntl_signal(SIGINT, function () use (&$running): void {
        fwrite(STDOUT, '[EdgeAuth] SIGINT' . PHP_EOL);
        $running = false;
    });
    $signalMode = 'pcntl';
}

if ($signalMode === 'none') {
    fwrite(STDOUT, '[EdgeAuth] no signal handler (Windows dev mode), use Ctrl+C to stop' . PHP_EOL);
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
            $requests = 0;
            $maxRequests = MAX_REQUESTS_PER_CONN; // ✅ 从常量读，可 env 覆盖

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

                $requests++;
                if (!$conn->shouldKeepAlive() || $requests >= $maxRequests) {
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
