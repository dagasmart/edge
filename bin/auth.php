<?php

declare(strict_types=1);

use DagaSmart\Edge\Support\EdgeAuthContext;
use Swow\Psr7\Message\ServerRequest;
use Swow\Psr7\Server\ServerConnection;

/* =========================================================
 * EdgeAuth HTTP 路由处理 - 对齐 NanoMQ auth.http
 * 认证方式：username + password（bcrypt）
 *
 * 改动记录：
 * 1. acquireUserOnline() 返回值检查，单点登录拒绝时正确返回 deny
 * 2. online 锁 TTL 从 300s 缩短为 60s，retry 窗口 30s → 10s
 * 3. 新增 handleRelease() 手动释放端点
 * ========================================================= */

function authRespond(ServerConnection $conn, bool $allow, string $reason, int $httpStatus = 0): void
{
    $status = $httpStatus !== 0
        ? $httpStatus
        : ($allow ? 200 : 403);

    respondJson($conn, [
        'result' => $allow ? 'allow' : 'deny',
        'code' => $allow ? 0 : 1,
        'message' => $allow ? 'success' : $reason,
        'data' => [
            'result' => $allow ? 'allow' : 'deny',
            'reason' => $reason,
        ],
    ], $status);
}

/**
 * 获取设备密钥
 *
 * ★ 所有 Redis 操作显式按 key 路由，确保分片模式下落到正确 shard
 */
function getDeviceSecret(string $clientId, string $username): ?string
{
    // 1. 优先完整格式
    $key = 'device:secret:'.$clientId.':'.$username;
    $val = withRedisForKey($key, static function (Redis $conn) use ($key): mixed {
        $v = $conn->get($key);

        return $v !== false ? (string) $v : null;
    });
    if ($val !== null && $val !== '') {
        return $val;
    }

    // 2. 兜底：兼容旧格式 device:secret:{username}
    $legacyKey = 'device:secret:'.$username;
    $val = withRedisForKey($legacyKey, static function (Redis $conn) use ($legacyKey): mixed {
        $v = $conn->get($legacyKey);

        return $v !== false ? (string) $v : null;
    });
    if ($val !== null && $val !== '') {
        // 回填完整格式，下次直接命中
        try {
            withRedisForKey($key, static function (Redis $conn) use ($key, $val): void {
                $conn->setex($key, 300, $val);
            });
        } catch (Throwable) {
            // 回填失败不影响本次认证
        }

        return $val;
    }

    // 3. Redis 双 miss → 回源 PG
    $secret = fallbackToPg($clientId, $username);
    if ($secret !== null) {
        try {
            withRedisForKey($key, static function (Redis $conn) use ($key, $secret): void {
                $conn->setex($key, 300, $secret);
            });
            withRedisForKey($legacyKey, static function (Redis $conn) use ($legacyKey, $secret): void {
                $conn->setex($legacyKey, 300, $secret);
            });
        } catch (Throwable) {
            // 回填失败不影响本次认证
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
        .'/extension/edge/auth/fallback'
        .'?client_id='.rawurlencode($clientId)
        .'&username='.rawurlencode($username)
        .'&service=nano';

    try {
        $ch = curl_init($url);
        curl_setopt_array($ch, [
            CURLOPT_RETURNTRANSFER => true,
            CURLOPT_TIMEOUT => 2,
            CURLOPT_CONNECTTIMEOUT => 1,
            CURLOPT_HTTPHEADER => [
                'X-Edge-Internal: '.$token,
                'Accept: application/json',
            ],
        ]);

        $response = curl_exec($ch);
        $httpCode = curl_getinfo($ch, CURLINFO_HTTP_CODE);
        $error = curl_error($ch);
        curl_close($ch);

        if ($error !== '') {
            fwrite(STDERR, '[fallbackToPg] curl error: '.$error.PHP_EOL);

            return null;
        }

        if ($httpCode !== 200 || ! is_string($response) || $response === '') {
            debugLog(sprintf(
                'fallbackToPg: upstream returned %d for client=%s user=%s',
                $httpCode,
                $clientId,
                $username
            ));

            return null;
        }

        $data = json_decode($response, true, 512, JSON_THROW_ON_ERROR);
        if (! is_array($data) || ! isset($data['password'])) {
            return null;
        }

        return (string) $data['password'];
    } catch (Throwable $e) {
        fwrite(STDERR, '[fallbackToPg] '.$e->getMessage().PHP_EOL);

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
                return $scheme.'://'.$host.':'.$port;
            }

            return $scheme.'://'.$host;
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

    return 'http://'.$host.':'.$port;
}

function authenticateDevice(string $clientId, string $username, string $password): array
{
    $hash = getDeviceSecret($clientId, $username);
    if ($hash === null) {
        return ['ok' => false, 'reason' => 'no secret'];
    }

    if (! password_verify($password, $hash)) {
        return ['ok' => false, 'reason' => 'password mismatch'];
    }

    return ['ok' => true, 'reason' => 'ok'];
}

/* =========================================================
 * 严格单点登录（Lua 原子，后登录 deny 不踢前）
 *
 * ★ 分片改造：Lua 涉及 GET/SET/DEL 多 key 原子性，
 * 必须按 username 路由到同一个 shard，不能跨 shard。
 *
 * ★ TTL 改为 60s（异常断线后最多 60s 即可换设备登录）
 * ★ retry 窗口改为 10s（被 deny 的设备 10s 后可重试）
 * ========================================================= */

function acquireUserOnline(
    string $username,
    string $clientId,
    int $ttl = 60,
    int $retryWindow = 10
): bool {
    $key = 'auth:online:'.$username;
    $retryKey = 'auth:retry:'.$username.':'.$clientId;

    $script = <<<'LUA'
local cur = redis.call('GET', KEYS[1])

if cur == false then
    redis.call('SET', KEYS[1], ARGV[1], 'EX', ARGV[2])
    redis.call('DEL', KEYS[2])
    return 1
end

if cur == ARGV[1] then
    redis.call('EXPIRE', KEYS[1], ARGV[2])
    redis.call('DEL', KEYS[2])
    return 1
end

local retrying = redis.call('GET', KEYS[2])
if retrying ~= false then
    return -1
end

redis.call('SET', KEYS[2], '1', 'EX', ARGV[3])
return 0
LUA;

    try {
        $ret = withRedisForKey(
            $key,
            static function (Redis $conn) use (
                $key,
                $retryKey,
                $clientId,
                $ttl,
                $retryWindow,
                $script
            ): int {
                return (int) $conn->eval(
                    $script,
                    [$key, $retryKey, $clientId, $ttl, $retryWindow],
                    2
                );
            }
        );

        if ($ret === -1) {
            fwrite(STDERR, sprintf(
                '[acquireUserOnline] silent-window clientid=%s username=%s%s',
                $clientId,
                $username,
                PHP_EOL
            ));
        } elseif ($ret === 0) {
            fwrite(STDERR, sprintf(
                '[acquireUserOnline] DENY conflict clientid=%s username=%s (already online elsewhere)%s',
                $clientId,
                $username,
                PHP_EOL
            ));
        }

        return $ret === 1;
    } catch (Throwable $e) {
        fwrite(STDERR, '[acquireUserOnline] '.$e->getMessage().PHP_EOL);

        return false;
    }
}

function releaseUserOnline(string $username, string $clientId): void
{
    $key = 'auth:online:'.$username;
    $retryKey = 'auth:retry:'.$username.':'.$clientId;

    $script = <<<'LUA'
if redis.call('GET', KEYS[1]) == ARGV[1] then
    redis.call('DEL', KEYS[1])
end
redis.call('DEL', KEYS[2])
return 0
LUA;

    try {
        withRedisForKey(
            $key,
            static function (Redis $conn) use ($key, $retryKey, $clientId, $script): void {
                $conn->eval($script, [$key, $retryKey, $clientId], 2);
            }
        );
    } catch (Throwable $e) {
        fwrite(STDERR, '[releaseUserOnline] '.$e->getMessage().PHP_EOL);
    }
}

/* =========================================================
 * Disconnect webhook handler
 * ★ NanoMQ 断开连接时调用，释放 online 锁
 * ========================================================= */

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

    if ($clientId === '' || $username === '') {
        respondJson($conn, [
            'result' => 'error',
            'reason' => 'missing client_id / username',
        ], 400);

        return;
    }

    releaseUserOnline($username, $clientId);

    fwrite(STDOUT, sprintf(
        '[EdgeAuth] [disconnect] SN=%s clientId=%s online lock released'.PHP_EOL,
        $username,
        $clientId
    ));

    respondJson($conn, [
        'result' => 'ok',
        'released' => true,
    ], 200);
}

/* =========================================================
 * Release 手动释放端点
 * ★ 当 disconnect webhook 丢失或设备管理平台需要主动踢人时使用
 * ★ 调用方式：POST /admin-api/extension/edge/release
 *   body: {"username": "fxc", "client_id": "xxx"}
 * ========================================================= */

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
        $data['clientid'] ?? $data['client_id'] ?? $data['clientId'] ?? ''
    ));
    $username = trim((string) (
        $data['username'] ?? $data['user'] ?? ''
    ));

    if ($username === '' || $clientId === '') {
        respondJson($conn, [
            'result' => 'error',
            'reason' => 'missing username / client_id',
        ], 400);

        return;
    }

    // 幂等：无论锁是否存在都尝试释放，不报错
    releaseUserOnline($username, $clientId);

    fwrite(STDOUT, sprintf(
        '[EdgeAuth] [release] manual release username=%s clientId=%s%s',
        $username,
        $clientId,
        PHP_EOL
    ));

    respondJson($conn, [
        'result' => 'ok',
        'released' => true,
    ], 200);
}

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

    if (! concurrencyAcquire()) {
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

    $rateKey = 'rate:'.$clientId;
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

        if (! $authenticated) {
            if (in_array($reason, ['password mismatch', 'no secret'], true)) {
                recordClientFail($clientId);
            }
            metricsInc('auth_fail');
            authRespond($conn, false, $reason);

            return;
        }

        // ★ 按 clientId 路由清除 fail/block 计数
        $failKey = 'fail:'.$clientId;
        $blockKey = 'block:'.$clientId;
        try {
            withRedisForKey($failKey, static function (Redis $conn) use ($failKey, $blockKey): void {
                $conn->del($failKey, $blockKey);
            });
        } catch (Throwable) {
        }

        EdgeAuthContext::setAuth($username, $clientId);

        // ★ 认证成功，获取 online 锁（TTL=60s 兜底，retry=10s）
        $onlineOk = acquireUserOnline($username, $clientId, 60, 10);
        if (! $onlineOk) {
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

        // ★ 不再在这里释放 online 锁！
        // 锁由 disconnect webhook（handleDisconnect）显式释放
        // Lua 脚本中 SET EX 60 提供 TTL 兜底，防止 disconnect 丢失导致永久死锁

        EdgeAuthContext::clear();
    }
}

function handleSuper(ServerConnection $conn, ServerRequest $req): void
{
    authRespond($conn, false, 'not_implemented');
}

function handleAcl(ServerConnection $conn, ServerRequest $req): void
{
    authRespond($conn, false, 'not_implemented');
}
