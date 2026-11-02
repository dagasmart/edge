<?php

declare(strict_types=1);

use Dagasmart\Edge\Support\EdgeAuthContext;
use Swow\Psr7\Message\ServerRequest;
use Swow\Psr7\Server\ServerConnection;

/* =========================================================
 * EdgeAuth HTTP 路由处理 - 对齐 NanoMQ auth.http
 * 认证方式：username + password（bcrypt）
 * 密钥来源：Redis device:secret:{username}
 * HMAC / 单点登录 / 重放检测保留代码，不参与主认证路径
 * ========================================================= */

/**
 * amis 兼容响应封装
 *
 * - 顶层 result：NanoMQ/EMQX HTTP 认证契约
 * - data.result：amis 管理端兼容
 */
function authRespond(ServerConnection $conn, bool $allow, string $reason, int $httpStatus = 0): void
{
    // NanoMQ 老版本：只看状态码
    $status = $httpStatus !== 0
        ? $httpStatus
        : ($allow ? 200 : 403); // 失败必须非 200

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

/**
 * 获取设备密钥
 *
 * ★ 对齐 Laravel EdgeAuthorizeService::syncRedisOnSave 写入格式
 * key 格式：device:secret:{client_id}:{username}
 */
function getDeviceSecret(string $clientId, string $username): ?string
{
    // 优先完整格式（与 Laravel 写入侧一致）
    $key = 'device:secret:' . $clientId . ':' . $username;
    $val = redisGet($key);
    if ($val !== null && $val !== '') {
        return $val;
    }

    // 兜底：兼容旧格式 device:secret:{username}
    $legacyKey = 'device:secret:' . $username;
    $val = redisGet($legacyKey);

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

        resetRate($rateKey);
        try {
            withRedis(static function (Redis $conn) use ($clientId): void {
                $conn->del('fail:'.$clientId, 'block:'.$clientId);
            });
        } catch (Throwable) {
        }

        EdgeAuthContext::setAuth($username, $clientId);

        metricsInc('auth_ok');
        authRespond($conn, true, 'ok');

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
    authRespond($conn, false, 'not_implemented');
}

function handleAcl(ServerConnection $conn, ServerRequest $req): void
{
    authRespond($conn, false, 'not_implemented');
}
