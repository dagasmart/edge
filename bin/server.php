<?php
declare(strict_types=1);

use Swow\Psr7\Server\Server;
use Swow\Socket;

// ---------- 配置常量 ----------
if (!defined('AUTH_PORT'))       define('AUTH_PORT',      9503);
if (!defined('REDIS_HOST'))      define('REDIS_HOST',     '127.0.0.1');
if (!defined('REDIS_PORT'))      define('REDIS_PORT',     16379);
if (!defined('REDIS_PASSWORD'))  define('REDIS_PASSWORD', 'uApDVQD7');
if (!defined('REDIS_DB'))        define('REDIS_DB',       2);
if (!defined('REDIS_POOL_SIZE')) define('REDIS_POOL_SIZE', 16);
if (!defined('CACHE_TTL'))       define('CACHE_TTL',      300);
if (!defined('RATE_TTL'))        define('RATE_TTL',       60);
if (!defined('RATE_MAX'))        define('RATE_MAX',       10);
if (!defined('TIME_DRIFT'))      define('TIME_DRIFT',     120);
if (!defined('LOCAL_CACHE_TTL')) define('LOCAL_CACHE_TTL', 60);
if (!defined('MAX_CONCURRENCY')) define('MAX_CONCURRENCY', 5000);

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
    fwrite(STDOUT, "[EdgeAuth] Redis pool: {$st['free']}/{$st['total']}\n");
} catch (\Throwable $e) {
    fwrite(STDERR, "[EdgeAuth] Redis DEGRADED: " . $e->getMessage() . "\n");
}

localCacheInit();
concurrencyInit(MAX_CONCURRENCY);

// ---------- Server ----------
$server = new Server();

// SO_REUSEPORT（Linux only，Windows 静默忽略）
try {
    $sock = $server->getSocket();
    if (method_exists($sock, 'setOption')) {
        @$sock->setOption(Socket::SOL_SOCKET, 15, 1); // SO_REUSEPORT
    }
} catch (\Throwable) {
    // ignore
}

// TLS（可选）
if (defined('TLS_CERT') && defined('TLS_KEY') && file_exists(TLS_CERT)) {
    $server->setSslCertFile(TLS_CERT);
    $server->setSslKeyFile(TLS_KEY);
    fwrite(STDOUT, "[EdgeAuth] TLS enabled\n");
}

// ✅ 正确：第三个参数只接受 int flags
$server->bind('0.0.0.0', AUTH_PORT, Server::BIND_FLAG_NONE)->listen();

fwrite(STDOUT, "[EdgeAuth] Listening on 0.0.0.0:" . AUTH_PORT . PHP_EOL);
fwrite(STDOUT, "[EdgeAuth] Redis: " . ($redisOk ? 'OK' : 'DEGRADED') . PHP_EOL);

// ---------- 优雅退出 ----------
$running = true;
if (function_exists('pcntl_signal')) {
    pcntl_async_signals(true);
    pcntl_signal(SIGTERM, function () use (&$running) {
        fwrite(STDOUT, "[EdgeAuth] SIGTERM\n");
        $running = false;
    });
    pcntl_signal(SIGINT, function () use (&$running) {
        fwrite(STDOUT, "[EdgeAuth] SIGINT\n");
        $running = false;
    });
}

while ($running) {
    try {
        $conn = $server->acceptConnection();
    } catch (\Throwable $e) {
        if (!$running) break;
        continue;
    }

    \Swow\Coroutine::run(static function () use ($conn): void {
        try {
            while (true) {
                $req = $conn->recvHttpRequest();
                $path = $req->getUri()->getPath();

                switch ($path) {
                    case '/mqtt/auth':
                        handleAuth($conn, $req);
                        break;
                    case '/health':
                        $conn->respond(
                            json_encode([
                                'status' => 'ok',
                                'redis'  => redisPoolStats(),
                                'cache'  => localCacheStats(),
                            ], JSON_UNESCAPED_SLASHES),
                            200,
                            ['Content-Type' => 'application/json']
                        );
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
        } catch (\Throwable $e) {
            fwrite(STDERR, "[conn-fatal] " . $e->getMessage() . "\n");
        } finally {
            try { $conn->close(); } catch (\Throwable) {}
        }
    });
}

fwrite(STDOUT, "[EdgeAuth] shutdown gracefully\n");
