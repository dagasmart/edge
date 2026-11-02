<?php

declare(strict_types=1);

use Dagasmart\Edge\Support\EdgeAuthContext;
use Swow\Psr7\Message\ServerRequest;
use Swow\Psr7\Server\ServerConnection;

/* =========================================================
 * EdgeAuth HTTP 路由处理 - 对齐 NanoMQ auth.http
 * 认证方式：username + password（bcrypt）
 * 密钥来源：Redis device:secret:{clientId}:{username}
 * HMAC / 单点登录 / 重放检测保留代码，不参与主认证路径
 * ========================================================= */

/**
 * 获取设备密钥
 *
 * ★ 对齐 Redis 认证模式：直接读取 Redis，不经过本地 L1 缓存
 * 确保设备密钥轮换后立即生效，避免缓存 TTL 导致的旧密钥残留
 */
function getDeviceSecret(string $clientId, string $username): ?string
{
    $key = 'device:secret:'.$clientId.':'.$username;

    $val = redisGet($key);

    return ($val !== null && $val !== '') ? $val : null;
}

function authenticateDevice(string $clientId, string $username, string $password): array
{
    $hash = getDeviceSecret($clientId, $username);
    if ($hash === null) {
        return ['ok' => false, 'reason' => 'no secret'];
    }

    // ★ NanoMQ 对齐：只做 bcrypt 校验，不做 rehash、不写 Redis
    if (! password_verify($password, $hash)) {
        return ['ok' => false, 'reason' => 'password mismatch'];
    }

    return ['ok' => true, 'reason' => 'ok'];
}

/* =========================================================
 * 严格单点登录（Lua 原子，后登录 deny 不踢前）
 * 对齐 NanoMQ 后不再调用，保留供 HMAC/管理端复用
 * ========================================================= */

function acquireUserOnline(
    string $username,
    string $clientId,
    int $ttl = 300,
    int $retryWindow = 30
): bool {
    $key = 'auth:online:'.$username;
    $retryKey = 'auth:retry:'.$username.':'.$clientId;

    $script = <<<'LUA'
local cur = redis.call('GET', KEYS[1])

-- 无人占用：抢占
if cur == false then
    redis.call('SET', KEYS[1], ARGV[1], 'EX', ARGV[2])
    redis.call('DEL', KEYS[2])
    return 1
end

-- 自己已占用：续期
if cur == ARGV[1] then
    redis.call('EXPIRE', KEYS[1], ARGV[2])
    redis.call('DEL', KEYS[2])
    return 1
end

-- 别人占用：检查静默窗口
local retrying = redis.call('GET', KEYS[2])
if retrying ~= false then
    return -1  -- 静默中，快速拒绝
end

-- 首次冲突：设置静默窗口
redis.call('SET', KEYS[2], '1', 'EX', ARGV[3])
return 0
LUA;

    try {
        $ret = withRedis(
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
        withRedis(
            static function (Redis $conn) use ($key, $retryKey, $clientId, $script): void {
                $conn->eval($script, [$key, $retryKey, $clientId], 2);
            }
        );
    } catch (Throwable $e) {
        fwrite(STDERR, '[releaseUserOnline] '.$e->getMessage().PHP_EOL);
    }
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
        respondJson($conn, ['result' => 'deny', 'reason' => 'missing client_id / username'], 200);

        return;
    }

    if (isClientBlocked($clientId)) {
        metricsInc('auth_fail');
        respondJson($conn, [
            'result' => 'deny',
            'reason' => 'client_blocked',
            'retry_after' => 600,
        ], 200);

        return;
    }

    if ($password === '') {
        metricsInc('auth_fail');
        respondJson($conn, ['result' => 'deny', 'reason' => 'missing password'], 200);

        return;
    }

    if (! concurrencyAcquire()) {
        metricsInc('auth_fail');
        respondJson($conn, ['result' => 'deny', 'reason' => 'overloaded'], 200);

        return;
    }

    $deviceToken = deviceConcurrencyAcquire($clientId);
    if ($deviceToken === null) {
        concurrencyRelease();
        metricsInc('auth_fail');
        metricsInc('device_concurrency_exceeded');
        respondJson($conn, [
            'result' => 'deny',
            'reason' => 'device_overloaded',
            'detail' => 'max '.MAX_CONCURRENCY_PER_DEVICE.' concurrent requests per device',
        ], 200);

        return;
    }

    $rateKey = 'rate:'.$clientId;
    if (rateLimited($rateKey)) {
        deviceConcurrencyRelease($deviceToken);
        concurrencyRelease();
        metricsInc('auth_fail');
        respondJson($conn, ['result' => 'deny', 'reason' => 'rate_limited'], 200);

        return;
    }

    try {
        // ★ 对齐 NanoMQ：只走 bcrypt 密码校验，密钥直接读 Redis
        $result = authenticateDevice($clientId, $username, $password);
        $authenticated = $result['ok'] ?? false;
        $reason = $result['reason'] ?? 'unknown';

        if (! $authenticated) {
            if (in_array($reason, ['password mismatch', 'no secret'], true)) {
                recordClientFail($clientId);
            }
            metricsInc('auth_fail');
            respondJson($conn, ['result' => 'deny', 'reason' => $reason], 200);

            return;
        }

        // ★ 认证成功：清除失败计数和封禁
        resetRate($rateKey);
        try {
            withRedis(static function (Redis $conn) use ($clientId): void {
                $conn->del('fail:'.$clientId, 'block:'.$clientId);
            });
        } catch (Throwable) {
        }

        EdgeAuthContext::setAuth($username, $clientId);

        metricsInc('auth_ok');
        respondJson($conn, ['result' => 'allow', 'reason' => $reason], 200);

    } finally {
        deviceConcurrencyRelease($deviceToken);
        concurrencyRelease();

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
    }
}

function handleSuper(ServerConnection $conn, ServerRequest $req): void
{
    respondJson($conn, ['result' => 'deny', 'reason' => 'not_implemented'], 501);
}

function handleAcl(ServerConnection $conn, ServerRequest $req): void
{
    respondJson($conn, ['result' => 'deny', 'reason' => 'not_implemented'], 501);
}
