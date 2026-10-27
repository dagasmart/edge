<?php
declare(strict_types=1);

use Swow\Psr7\Server\Server;

// ---------- 配置常量（defined 保护，防重复定义） ----------
if (!defined('AUTH_PORT'))       define('AUTH_PORT',      9503);
if (!defined('REDIS_HOST'))      define('REDIS_HOST',     '127.0.0.1');
if (!defined('REDIS_PORT'))      define('REDIS_PORT',     6379);
if (!defined('REDIS_USER'))      define('REDIS_USER',     '');
if (!defined('REDIS_PASSWORD'))  define('REDIS_PASSWORD', '123456');
if (!defined('REDIS_DB'))        define('REDIS_DB',       2);
if (!defined('REDIS_POOL_SIZE')) define('REDIS_POOL_SIZE', 16);
if (!defined('CACHE_TTL'))       define('CACHE_TTL',      300);
if (!defined('RATE_TTL'))        define('RATE_TTL',       60);
if (!defined('RATE_MAX'))        define('RATE_MAX',       10);
if (!defined('TIME_DRIFT'))      define('TIME_DRIFT',     300);
if (!defined('LOCAL_CACHE_TTL')) define('LOCAL_CACHE_TTL', 60);

// ---------- 全局致命错误兜底 ----------
register_shutdown_function(function () {
    $e = error_get_last();
    if ($e && in_array($e['type'], [E_ERROR, E_PARSE, E_CORE_ERROR, E_COMPILE_ERROR], true)) {
        fwrite(STDERR, sprintf(
            "[shutdown] %s in %s:%d\n",
            $e['message'], $e['file'], $e['line']
        ));
    }
});

set_exception_handler(function (\Throwable $e) {
    fwrite(STDERR, sprintf(
        "[uncaught] %s in %s:%d\n",
        $e->getMessage(), $e->getFile(), $e->getLine()
    ));
});

// ---------- 加载业务文件 ----------
require_once __DIR__ . '/helper.php';
require_once __DIR__ . '/hmac.php';
require_once __DIR__ . '/auth.php';

// ---------- 初始化 ----------
redisPoolInit();
localCacheInit();

// ---------- 启动 HTTP 服务 ----------
$server = new Server();
$server->bind('0.0.0.0', AUTH_PORT)->listen();

fwrite(STDOUT, "[EdgeAuth] Listening on 0.0.0.0:" . AUTH_PORT . PHP_EOL);
fwrite(STDOUT, "[EdgeAuth] Redis pool=" . REDIS_POOL_SIZE . " db=" . REDIS_DB . PHP_EOL);

// ---------- 优雅退出 ----------
$running = true;
if (function_exists('pcntl_signal')) {
    pcntl_async_signals(true);
    pcntl_signal(SIGTERM, function () use (&$running) {
        fwrite(STDOUT, "[EdgeAuth] SIGTERM received, stopping...\n");
        $running = false;
    });
    pcntl_signal(SIGINT, function () use (&$running) {
        fwrite(STDOUT, "[EdgeAuth] SIGINT received, stopping...\n");
        $running = false;
    });
}

while ($running) {
    try {
        $conn = $server->acceptConnection();
    } catch (\Throwable $e) {
        if (!$running) break;
        fwrite(STDERR, "[accept] " . $e->getMessage() . "\n");
        continue;
    }

    \Swow\Coroutine::run(static function () use ($conn): void {
        try {
            while (true) {
                $req = $conn->recvHttpRequest();
                $path = $req->getUri()->getPath();

                try {
                    switch ($path) {
                        case '/mqtt/auth':
                            handleAuth($conn, $req);
                            break;
                        case '/health':
                            $conn->respond(
                                json_encode([
                                    'status' => 'ok',
                                    'pool'   => redisPoolStats(),
                                    'cache'  => localCacheStats(),
                                ], JSON_UNESCAPED_SLASHES),
                                200,
                                ['Content-Type' => 'application/json']
                            );
                            break;
                        default:
                            $conn->respond('', 404);
                            break;
                    }
                } catch (\Throwable $e) {
                    fwrite(STDERR, sprintf(
                        "[handler] %s in %s:%d\n",
                        $e->getMessage(), $e->getFile(), $e->getLine()
                    ));
                    try { $conn->respond('', 401); } catch (\Throwable) {}
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
