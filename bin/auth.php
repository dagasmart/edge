<?php

declare(strict_types=1);

use DagaSmart\Edge\Support\EdgeAuthContext;
use Swow\Psr7\Message\ServerRequest;
use Swow\Psr7\Server\ServerConnection;

/* =========================================================
 * EdgeAuth HTTP 路由处理 - 对齐 NanoMQ auth.http
 * 认证方式：client_id + username + password（bcrypt）
 *
 * 在线锁模式（由 .env MQTT_AUTH_ONLINE_MODE 控制）：
 *   - client_id  → 按 client_id 锁（多设备共享账号，各自独立）
 *   - username   → 按 username 锁（严格单点登录）
 *   - both       → 双重校验
 * ========================================================= */

/* ==================== 响应 ==================== */

function authRespond(ServerConnection $conn, bool $allow, string $reason, int $httpStatus = 0): void
{
    $status = $httpStatus !== 0
        ? $httpStatus
        : ($allow ? 200 : 403);

    respondJson($conn, [
        'result'  => $allow ? 'allow' : 'deny',
        'code'    => $allow ? 0 : 1,
        'message' => $allow ? 'success' : $reason,
        'data'    => [
            'result' => $allow ? 'allow' : 'deny',
            'reason' => $reason,
        ],
    ], $status);
}

/* ==================== 模式读取 ==================== */

/**
 * 获取在线锁模式
 * 优先级：.env MQTT_AUTH_ONLINE_MODE → config/edge.php auth.online_mode → 默认 client_id
 */
function getOnlineMode(): string
{
    $env = getenv('MQTT_AUTH_ONLINE_MODE');
    if ($env !== false && in_array($env, ['client_id', 'username', 'both'], true)) {
        return $env;
    }

    if (function_exists('config')) {
        $mode = config('edge.auth.online_mode');
        if (in_array($mode, ['client_id', 'username', 'both'], true)) {
            return $mode;
        }
    }

    return 'client_id';
}

/* ==================== 密钥获取 ==================== */

function getDeviceSecret(string $clientId, string $username): ?string
{
    // 1. 严格按 client_id + username 拼 key
    $key = 'device:secret:' . $clientId . ':' . $username;
    $val = withRedisForKey($key, static function (Redis $conn) use ($key): mixed {
        $v = $conn->get($key);
        return $v !== false ? (string) $v : null;
    });
    if ($val !== null && $val !== '') {
        return $val;
    }

    // 2. Redis miss → 回源 PG
    $secret = fallbackToPg($clientId, $username);
    if ($secret !== null) {
        try {
            withRedisForKey($key, static function (Redis $conn) use ($key, $secret): void {
                $conn->setex($key, 300, $secret);
            });
        } catch (Throwable) {
        }
        return $secret;
    }

    return null;
}

function fallbackToPg(string $clientId, string $username): ?string
{
    $baseUrl = authFallbackUrl();
    if ($baseUrl === '') {
        debugLog('fallbackToPg: EDGE_AUTH_FALLBACK_URL not configured');
        return null;
    }

    $token = (string) getenv('EDGE_INTERNAL_TOKEN');
    if ($token === '') {
        debugLog('fallbackToPg: EDGE_INTERNAL_TOKEN not configured');
        return null;
    }

    $url = rtrim($baseUrl, '/')
        . '/extension/edge/auth/fallback'
        . '?client_id=' . rawurlencode($clientId)
        . '&username=' . rawurlencode($username)
        . '&service=nano';

    try {
        $ch = curl_init($url);
        curl_setopt_array($ch, [
            CURLOPT_RETURNTRANSFER => true,
            CURLOPT_TIMEOUT       => 2,
            CURLOPT_CONNECTTIMEOUT => 1,
            CURLOPT_HTTPHEADER    => [
                'X-Edge-Internal: ' . $token,
                'Accept: application/json',
            ],
        ]);

        $response  = curl_exec($ch);
        $httpCode = curl_getinfo($ch, CURLINFO_HTTP_CODE);
        $error    = curl_error($ch);
        curl_close($ch);

        if ($error !== '') {
            fwrite(STDERR, '[fallbackToPg] curl error: ' . $error . PHP_EOL);
            return null;
        }

        if ($httpCode !== 200 || !is_string($response) || $response === '') {
            debugLog(sprintf(
                'fallbackToPg: upstream returned %d for client=%s user=%s',
                $httpCode,
                $clientId,
                $username
            ));
            return null;
        }

        $data = json_decode($response, true, 512, JSON_THROW_ON_ERROR);
        if (!is_array($data) || !isset($data['password'])) {
            return null;
        }

        return (string) $data['password'];
    } catch (Throwable $e) {
        fwrite(STDERR, '[fallbackToPg] ' . $e->getMessage() . PHP_EOL);
        return null;
    }
}

function authFallbackUrl(): string
{
    $url = (string) getenv('EDGE_AUTH_FALLBACK_URL');
    if ($url !== '') {
        return $url;
    }

    $prefix = defined('AUTH_HTTP_PREFIX') ? AUTH_HTTP_PREFIX : '';
    if ($prefix === '') {
        return '';
    }

    if (preg_match('#^https?://#i', $prefix)) {
        $parts = parse_url($prefix);
        if (isset($parts['host'])) {
            $host = $parts['host'];
            $port = $parts['port'] ?? null;
            $scheme = ($parts['scheme'] ?? 'http');
            if ($port !== null) {
                return $scheme . '://' . $host . ':' . $port;
            }
            return $scheme . '://' . $host;
        }
        return '';
    }

    $host = (string) getenv('EDGE_FALLBACK_HOST');
    if ($host === '') {
        $host = '127.0.0.1';
    }

    $port = (int) getenv('EDGE_FALLBACK_PORT');
    if ($port <= 0) {
        $port = 8000;
    }

    return 'http://' . $host . ':' . $port;
}

/* ==================== 认证 ==================== */

function authenticateDevice(string $clientId, string $username, string $password): array
{
    $hash = getDeviceSecret($clientId, $username);
    if ($hash === null) {
        return ['ok' => false, 'reason' => 'no secret'];
    }

    if (!password_verify($password, $hash)) {
        return ['ok' => false, 'reason' => 'password mismatch'];
    }

    return ['ok' => true, 'reason' => 'ok'];
}

/* ==================== 在线锁 - client_id 模式 ==================== */

function acquireClientOnline(string $clientId, int $ttl = 60): bool
{
    $key = 'auth:online:' . $clientId;

    try {
        $ok = withRedisForKey($key, static function (Redis $conn) use ($key, $ttl) {
            return $conn->set($key, '1', ['NX', 'EX' => $ttl]);
        });

        if (!$ok) {
            // 已存在（同 client_id 重连），续期
            withRedisForKey($key, static function (Redis $conn) use ($key, $ttl) {
                $conn->expire($key, $ttl);
            });
        }

        return true;
    } catch (Throwable $e) {
        fwrite(STDERR, '[acquireClientOnline] ' . $e->getMessage() . PHP_EOL);
        return false;
    }
}

function releaseClientOnline(string $clientId): void
{
    $key = 'auth:online:' . $clientId;
    try {
        withRedisForKey($key, static function (Redis $conn) use ($key): void {
            $conn->del($key);
        });
    } catch (Throwable $e) {
        fwrite(STDERR, '[releaseClientOnline] ' . $e->getMessage() . PHP_EOL);
    }
}

/* ==================== 在线锁 - username 模式 ==================== */

function acquireUserOnline(string $username, string $clientId, int $ttl = 60): bool
{
    $key = 'auth:online:' . $username;

    $script = <<<'LUA'
local cur = redis.call('GET', KEYS[1])
if cur == false then
    redis.call('SET', KEYS[1], ARGV[1], 'EX', ARGV[2])
    return 1
end
if cur == ARGV[1] then
    redis.call('EXPIRE', KEYS[1], ARGV[2])
    return 1
end
return 0
LUA;

    try {
        $ret = withRedisForKey($key, static function (Redis $conn) use ($script, $key, $username, $clientId, $ttl) {
            return (int) $conn->eval($script, [$key, $username, $clientId, $ttl], 1);
        });

        if ($ret === 0) {
            fwrite(STDOUT, sprintf(
                '[acquireUserOnline] DENY conflict clientid=%s username=%s (already online elsewhere)' . PHP_EOL,
                $clientId,
                $username
            ));
        }

        return $ret === 1;
    } catch (Throwable $e) {
        fwrite(STDERR, '[acquireUserOnline] ' . $e->getMessage() . PHP_EOL);
        return false;
    }
}

function releaseUserOnline(string $username): void
{
    $key = 'auth:online:' . $username;
    try {
        withRedisForKey($key, static function (Redis $conn) use ($key): void {
            $conn->del($key);
        });
    } catch (Throwable $e) {
        fwrite(STDERR, '[releaseUserOnline] ' . $e->getMessage() . PHP_EOL);
    }
}

/* ==================== 在线锁 - both 模式辅助 ==================== */

function trackClientToUser(string $clientId, string $username): void
{
    $key = 'auth:client_user_map:' . $clientId;
    try {
        withRedisForKey($key, static function (Redis $conn) use ($key, $username): void {
            $conn->setex($key, 300, $username);
        });
    } catch (Throwable) {
    }
}

function untrackClientToUser(string $clientId): void
{
    $key = 'auth:client_user_map:' . $clientId;
    try {
        withRedisForKey($key, static function (Redis $conn) use ($key): void {
            $conn->del($key);
        });
    } catch (Throwable) {
    }
}

/* ==================== 在线锁统一入口 ==================== */

function acquireOnlineLock(string $clientId, string $username, int $ttl = 60): bool
{
    $mode = getOnlineMode();

    switch ($mode) {
        case 'username':
            return acquireUserOnline($username, $clientId, $ttl);

        case 'both':
            $userOk = acquireUserOnline($username, $clientId, $ttl);
            if (!$userOk) {
                return false;
            }
            trackClientToUser($clientId, $username);
            return true;

        case 'client_id':
        default:
            return acquireClientOnline($clientId, $ttl);
    }
}

function releaseOnlineLock(string $clientId, string $username): void
{
    $mode = getOnlineMode();

    switch ($mode) {
        case 'username':
            releaseUserOnline($username);
            break;

        case 'both':
            releaseUserOnline($username);
            releaseClientOnline($clientId);
            untrackClientToUser($clientId);
            break;

        case 'client_id':
        default:
            releaseClientOnline($clientId);
            break;
    }
}

/* ==================== Disconnect webhook ==================== */

function handleDisconnect(ServerConnection $conn, ServerRequest $req): void
{
    $body = (string) $req->getBody();
    $contentType = strtolower($req->getHeaderLine('Content-Type'));

    if (str_contains($contentType, 'application/json')) {
        $data = json_decode($body, true) ?? [];
    } else {
        $data = [];
        parse_str($body, $data);
    }

    $clientId = trim((string) (
        $data['clientid'] ?? $data['client_id'] ?? $data['clientId'] ?? ''
    ));
    $username = trim((string) (
        $data['username'] ?? $data['user'] ?? ''
    ));

    if ($clientId === '') {
        respondJson($conn, [
            'result' => 'error',
            'reason' => 'missing client_id',
        ], 400);
        return;
    }

    releaseOnlineLock($clientId, $username);

    fwrite(STDOUT, sprintf(
        '[EdgeAuth] [disconnect] clientId=%s username=%s online lock released' . PHP_EOL,
        $clientId,
        $username
    ));

    respondJson($conn, [
        'result'   => 'ok',
        'released' => true,
    ], 200);
}

/* ==================== 手动释放端点 ==================== */

function handleRelease(ServerConnection $conn, ServerRequest $req): void
{
    $body = (string) $req->getBody();
    $contentType = strtolower($req->getHeaderLine('Content-Type'));

    if (str_contains($contentType, 'application/json')) {
        $data = json_decode($body, true) ?? [];
    } else {
        $data = [];
        parse_str($body, $data);
    }

    $clientId = trim((string) (
        $data['client_id'] ?? $data['clientid'] ?? $data['clientId'] ?? ''
    ));
    $username = trim((string) (
        $data['username'] ?? $data['user'] ?? ''
    ));

    if ($clientId === '') {
        respondJson($conn, [
            'result' => 'error',
            'reason' => 'missing client_id',
        ], 400);
        return;
    }

    releaseOnlineLock($clientId, $username);

    fwrite(STDOUT, sprintf(
        '[EdgeAuth] [release] clientId=%s username=%s lock manually released' . PHP_EOL,
        $clientId,
        $username
    ));

    respondJson($conn, [
        'result'   => 'ok',
        'released' => true,
    ], 200);
}

/* ==================== 认证主逻辑 ==================== */

function handleAuth(ServerConnection $conn, ServerRequest $req): void
{
    $body = (string) $req->getBody();
    $contentType = strtolower($req->getHeaderLine('Content-Type'));

    if (str_contains($contentType, 'application/json')) {
        $data = json_decode($body, true) ?? [];
    } else {
        $data = [];
        parse_str($body, $data);
    }

    $clientId = trim((string) (
        $data['clientid'] ?? $data['client_id'] ?? $data['clientId'] ?? ''
    ));
    $username = trim((string) (
        $data['username'] ?? $data['user'] ?? ''
    ));
    $password = trim((string) ($data['password'] ?? ''));

    if ($clientId === '' || $username === '') {
        metricsInc('auth_fail');
        authRespond($conn, false, 'missing client_id / username');
        return;
    }

    if (isClientBlocked($clientId)) {
        metricsInc('auth_fail');
        authRespond($conn, false, 'client_blocked');
        return;
    }

    if ($password === '') {
        metricsInc('auth_fail');
        authRespond($conn, false, 'missing password');
        return;
    }

    if (!concurrencyAcquire()) {
        metricsInc('auth_fail');
        authRespond($conn, false, 'overloaded');
        return;
    }

    $deviceToken = deviceConcurrencyAcquire($clientId);
    if ($deviceToken === null) {
        concurrencyRelease();
        metricsInc('auth_fail');
        metricsInc('device_concurrency_exceeded');
        authRespond($conn, false, 'device_overloaded');
        return;
    }

    $rateKey = 'rate:' . $clientId;
    if (rateLimited($rateKey)) {
        deviceConcurrencyRelease($deviceToken);
        concurrencyRelease();
        metricsInc('auth_fail');
        authRespond($conn, false, 'rate_limited');
        return;
    }

    try {
        $result = authenticateDevice($clientId, $username, $password);
        $authenticated = $result['ok'] ?? false;
        $reason = $result['reason'] ?? 'unknown';

        if (!$authenticated) {
            if (in_array($reason, ['password mismatch', 'no secret'], true)) {
                recordClientFail($clientId);
            }
            metricsInc('auth_fail');
            authRespond($conn, false, $reason);
            return;
        }

        // 认证通过，清除 fail/block 计数
        $failKey  = 'fail:' . $clientId;
        $blockKey = 'block:' . $clientId;
        try {
            withRedisForKey($failKey, static function (Redis $conn) use ($failKey, $blockKey): void {
                $conn->del($failKey, $blockKey);
            });
        } catch (Throwable) {
        }

        EdgeAuthContext::setAuth($username, $clientId);

        // 获取在线锁（模式由 MQTT_AUTH_ONLINE_MODE 控制）
        $onlineOk = acquireOnlineLock($clientId, $username, 60);
        if (!$onlineOk) {
            deviceConcurrencyRelease($deviceToken);
            concurrencyRelease();
            metricsInc('auth_fail');
            metricsInc('auth_conflict');
            authRespond($conn, false, 'already_online_elsewhere');
            return;
        }

        metricsInc('auth_ok');
        authRespond($conn, true, 'ok');
    } finally {
        deviceConcurrencyRelease($deviceToken);
        concurrencyRelease();
        // online 锁不在这里释放，由 disconnect webhook 或 handleRelease 显式释放
        // TTL 60s 兜底防止 disconnect 丢失
        EdgeAuthContext::clear();
    }
}

/* ==================== 其他路由 ==================== */

function handleSuper(ServerConnection $conn, ServerRequest $req): void
{
    authRespond($conn, false, 'not_implemented');
}

function handleAcl(ServerConnection $conn, ServerRequest $req): void
{
    authRespond($conn, false, 'not_implemented');
}
