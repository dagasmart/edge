<?php
declare(strict_types=1);

use Swow\Psr7\Server\Server;

// ---------- 配置常量 ----------
if (!defined('AUTH_PORT'))       define('AUTH_PORT',      9503);
if (!defined('REDIS_HOST'))      define('REDIS_HOST',     '127.0.0.1');
if (!defined('REDIS_PORT'))      define('REDIS_PORT',     16379);
if (!defined('REDIS_PASSWORD'))  define('REDIS_PASSWORD', 'uApDVQD7');
if (!defined('REDIS_DB'))        define('REDIS_DB',       2);
if (!defined('REDIS_POOL_SIZE')) define('REDIS_POOL_SIZE', 64);

// ✅ 并发上限与 Redis 池大小对齐，避免 503 误报（压测可调大，但保持 ≈ 池×2）
if (!defined('MAX_CONCURRENCY')) define('MAX_CONCURRENCY', REDIS_POOL_SIZE * 2);

if (!defined('CACHE_TTL'))       define('CACHE_TTL',      300);
if (!defined('LOCAL_CACHE_TTL')) define('LOCAL_CACHE_TTL', 60);

// 压测时临时改成（生产务必回退）：
if (!defined('RATE_MAX'))   define('RATE_MAX',  100000); // 原来 10
if (!defined('RATE_TTL'))   define('RATE_TTL',  60);
if (!defined('TIME_DRIFT')) define('TIME_DRIFT', 300);   // 原来 120，给生成器更大窗口

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

// 注意：不手动设置 SO_REUSEPORT
// - Linux：Swow Server 内部已处理，无需手动设置
// - Windows：不支持，Swow\Socket::setOption() 调用会抛 Error
// 之前的 try/catch 也无法捕获 Engine 层面的 Error，因此直接省略

// TLS（可选）
if (defined('TLS_CERT') && defined('TLS_KEY') && file_exists(TLS_CERT)) {
    $server->setSslCertFile(TLS_CERT);
    $server->setSslKeyFile(TLS_KEY);
    fwrite(STDOUT, "[EdgeAuth] TLS enabled\n");
}

// 第三个参数只接受 int flags
$server->bind('0.0.0.0', AUTH_PORT, Server::BIND_FLAG_NONE)->listen();

fwrite(STDOUT, "[EdgeAuth] Listening on 0.0.0.0:" . AUTH_PORT . PHP_EOL);
fwrite(STDOUT, "[EdgeAuth] Redis: " . ($redisOk ? 'OK' : 'DEGRADED') . PHP_EOL);

// ---------- 优雅退出（跨平台安全版） ----------
// Windows：Swow\Signal 构造抛异常（安全限制）、pcntl 无效 → 降级为 none，Ctrl+C 停止
// Linux：Swow\Signal 生效；不可用时回退 pcntl
$running = true;
$signalMode = 'none';

if (class_exists('Swow\Signal')) {
    try {
        $sigMask = \Swow\Signal::TERM | \Swow\Signal::INT;
        $signal  = new \Swow\Signal($sigMask);
        \Swow\Coroutine::run(static function () use ($signal, &$running): void {
            $signal->wait();
            fwrite(STDOUT, "[EdgeAuth] received SIGTERM/SIGINT, shutting down\n");
            $running = false;
        });
        $signalMode = 'swow';
    } catch (\Throwable $e) {
        fwrite(STDERR, "[EdgeAuth] Swow\Signal unavailable ({$e->getMessage()}), falling back\n");
    }
}

if ($signalMode === 'none' && function_exists('pcntl_signal')) {
    pcntl_async_signals(true);
    pcntl_signal(SIGTERM, function () use (&$running) {
        fwrite(STDOUT, "[EdgeAuth] SIGTERM\n");
        $running = false;
    });
    pcntl_signal(SIGINT, function () use (&$running) {
        fwrite(STDOUT, "[EdgeAuth] SIGINT\n");
        $running = false;
    });
    $signalMode = 'pcntl';
}

if ($signalMode === 'none') {
    fwrite(STDOUT, "[EdgeAuth] no signal handler (Windows dev mode), use Ctrl+C to stop\n");
}

// ---------- 主循环 ----------
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
