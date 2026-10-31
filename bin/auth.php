<?php

declare(strict_types=1);

use Dagasmart\Edge\Support\EdgeAuthContext;
use Swow\Psr7\Message\ServerRequest;
use Swow\Psr7\Server\ServerConnection;

/* =========================================================
 * EdgeAuth HTTP 路由处理 - 方案 A（bcrypt + HMAC 双路径）
 *
 * 约定：
 *   Redis key = device:secret:{client_id}:{username}
 *   Redis val = bcrypt hash
 *   验证方式  = password_verify
 * ========================================================= */

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

    if (! password_verify($password, $hash)) {
        return ['ok' => false, 'reason' => 'password mismatch'];
    }

    // 自动升级 bcrypt cost
    if (password_needs_rehash($hash, PASSWORD_BCRYPT)) {
        $newHash = password_hash($password, PASSWORD_BCRYPT);
        $key = 'device:secret:'.$clientId.':'.$username;
        redisSetex($key, 0, $newHash); // 0 = 永不过期
    }

    return ['ok' => true, 'reason' => 'ok'];
}

/* =========================================================
 * 单点登录互斥
 * ========================================================= */

function acquireUserOnline(string $username, string $clientId, int $ttl = 300): bool
{
    $key = 'auth:online:'.$username;

    $existing = redisGet($key);
    if ($existing !== null) {
        if ($existing === $clientId) {
            redisSetex($key, $ttl, $clientId);

            return true;
        }

        return false;
    }

    $ok = redisSetNxEx($key, $clientId, $ttl);
    if ($ok) {
        return true;
    }

    $existing = redisGet($key);

    return $existing === $clientId;
}

function releaseUserOnline(string $username, string $clientId): void
{
    $key = 'auth:online:'.$username;
    $existing = redisGet($key);

    if ($existing === $clientId) {
        redisDel($key);
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

    // 方案 A：password 和 signature 分开
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
            'detail' => 'max '.MAX_CONCURRENCY_PER_DEVICE.' concurrent requests per device',
        ], 429);

        return;
    }

    $rateKey = 'rate:'.$clientId;
    if (rateLimited($rateKey)) {
        deviceConcurrencyRelease($deviceToken);
        concurrencyRelease();
        metricsInc('auth_fail');
        respondJson($conn, ['result' => 'deny', 'reason' => 'rate_limited'], 429);

        return;
    }

    if (isClientBlocked($clientId)) {
        deviceConcurrencyRelease($deviceToken);
        concurrencyRelease();
        metricsInc('auth_fail');
        respondJson($conn, ['result' => 'deny', 'reason' => 'blocked'], 401);

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

        // 写入协程上下文，供 server.php finally 释放
        EdgeAuthContext::setAuth($username, $clientId);

        metricsInc('auth_ok');
        resetRate($rateKey);
        respondJson($conn, [
            'result' => 'allow',
            'reason' => $reason,
        ], 200);

    } finally {
        deviceConcurrencyRelease($deviceToken);
        concurrencyRelease();
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
