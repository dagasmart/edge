<?php

declare(strict_types=1);

use DagaSmart\Edge\Support\EdgeAuthContext;
use Swow\Psr7\Message\ServerRequest;
use Swow\Psr7\Server\ServerConnection;

/* =========================================================
 * EdgeAuth HTTP 路由处理 - 方案 A（bcrypt + HMAC 双路径）
 * ========================================================= */

function getDeviceSecret(string $clientId, string $username): ?string
{
    $key = 'device:secret:' . $clientId . ':' . $username;
    $val = redisGet($key);

    return ($val !== null && $val !== '') ? $val : null;
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

    if (password_needs_rehash($hash, PASSWORD_BCRYPT)) {
        $newHash = password_hash($password, PASSWORD_BCRYPT);
        $key = 'device:secret:' . $clientId . ':' . $username;
        redisSetex($key, 0, $newHash);
    }

    return ['ok' => true, 'reason' => 'ok'];
}

/* =========================================================
 * 单点登录互斥（Lua 原子抢占 / 原子释放）
 * ========================================================= */

function acquireUserOnline(string $username, string $clientId, int $ttl = 300): bool
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
        $ret = withRedis(
            static function (Redis $conn) use ($key, $clientId, $ttl, $script): bool {
                return (bool) $conn->eval($script, [$key, $clientId, $ttl], 1);
            }
        );
        return $ret;
    } catch (Throwable $e) {
        fwrite(STDERR, '[acquireUserOnline] ' . $e->getMessage() . PHP_EOL);
        return false;
    }
}

function releaseUserOnline(string $username, string $clientId): void
{
    $key = 'auth:online:' . $username;

    $script = <<<'LUA'
if redis.call('GET', KEYS[1]) == ARGV[1] then
    return redis.call('DEL', KEYS[1])
end
return 0
LUA;

    try {
        withRedis(
            static function (Redis $conn) use ($key, $clientId, $script): void {
                $conn->eval($script, [$key, $clientId], 1);
            }
        );
    } catch (Throwable $e) {
        fwrite(STDERR, '[releaseUserOnline] ' . $e->getMessage() . PHP_EOL);
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
    $signature = trim((string) ($data['signature'] ?? $data['token'] ?? ''));

    error_log(sprintf(
        '[auth-debug] clientid=%s username=%s password_len=%d signature_len=%d',
        $clientId, $username, strlen($password), strlen($signature)
    ));

    if ($clientId === '' || $username === '') {
        metricsInc('auth_fail');
        respondJson($conn, [
            'result' => 'deny',
            'reason' => 'missing client_id / username',
        ], 401);

        return;
    }

    // ★ 入口级拦截：已封禁客户端不占并发、不查 secret、不走限流
    if (isClientBlocked($clientId)) {
        metricsInc('auth_fail');
        respondJson($conn, [
            'result' => 'deny',
            'reason' => 'client_blocked',
            'retry_after' => 600,
        ], 429);

        return;
    }

    if ($password === '' && $signature === '') {
        metricsInc('auth_fail');
        respondJson($conn, [
            'result' => 'deny',
            'reason' => 'missing password or signature',
        ], 401);

        return;
    }

    if (! concurrencyAcquire()) {
        metricsInc('auth_fail');
        respondJson($conn, ['result' => 'deny', 'reason' => 'overloaded'], 503);

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
            'detail' => 'max ' . MAX_CONCURRENCY_PER_DEVICE . ' concurrent requests per device',
        ], 429);

        return;
    }

    $rateKey = 'rate:' . $clientId;
    if (rateLimited($rateKey)) {
        deviceConcurrencyRelease($deviceToken);
        concurrencyRelease();
        metricsInc('auth_fail');
        respondJson($conn, ['result' => 'deny', 'reason' => 'rate_limited'], 429);

        return;
    }

    try {
        $useHmac = ($signature !== '');

        if ($useHmac) {
            $authenticated = hmacAuthenticate($clientId, $username, $signature);

            if (! $authenticated) {
                $reason = EdgeAuthContext::getAuthReason() ?: 'hmac_failed';
                recordClientFail($clientId);
                metricsInc('auth_fail');
                respondJson($conn, ['result' => 'deny', 'reason' => $reason], 401);

                return;
            }

            $reason = 'ok';
        } else {
            $result = authenticateDevice($clientId, $username, $password);
            $authenticated = $result['ok'] ?? false;
            $reason = $result['reason'] ?? 'unknown';

            if (! $authenticated) {
                if (in_array($reason, ['password mismatch', 'no secret'], true)) {
                    recordClientFail($clientId);
                }
                metricsInc('auth_fail');
                respondJson($conn, ['result' => 'deny', 'reason' => $reason], 401);

                return;
            }
        }

        if (! acquireUserOnline($username, $clientId, 300)) {
            metricsInc('auth_fail');
            metricsInc('auth_conflict');
            respondJson($conn, [
                'result' => 'deny',
                'reason' => 'already_logged_in',
                'detail' => '同一账号已在其他设备登录',
            ], 401);

            return;
        }

        // ★ 认证成功：清除失败/封禁计数，防止偶发输错被误封
        resetRate($rateKey);
        try {
            withRedis(static function (Redis $conn) use ($clientId): void {
                $conn->del('fail:' . $clientId, 'block:' . $clientId);
            });
        } catch (Throwable) {
        }

        EdgeAuthContext::setAuth($username, $clientId);

        metricsInc('auth_ok');
        respondJson($conn, [
            'result' => 'allow',
            'reason' => $reason,
        ], 200);

    } finally {
        deviceConcurrencyRelease($deviceToken);
        concurrencyRelease();

        // ★ 释放在线锁（只删自己的，Lua 原子）
        try {
            $authedUsername = EdgeAuthContext::getUsername();
            $authedClientId = EdgeAuthContext::getClientId();
            if ($authedUsername !== null && $authedClientId !== null) {
                releaseUserOnline($authedUsername, $authedClientId);
            }
        } catch (Throwable $e) {
            fwrite(STDERR, '[release-online] ' . $e->getMessage() . PHP_EOL);
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
