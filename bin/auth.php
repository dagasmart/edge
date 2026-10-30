<?php
declare(strict_types=1);

use Swow\Psr7\Message\ServerRequest;
use Swow\Psr7\Server\ServerConnection;

/* =========================================================
 * EdgeAuth HTTP 路由处理 - 生产版
 *
 * 约定：
 *   Redis key = device:secret:{client_id}:{username}
 *   Redis val = bcrypt hash (与 Laravel DB password 字段一致)
 *   password  = 明文密码
 *   验证方式  = password_verify
 * ========================================================= */

/**
 * 从 Redis 获取设备 bcrypt hash
 */
function getDeviceSecret(string $clientId, string $username): ?string
{
    $key = 'device:secret:' . $clientId . ':' . $username;
    $val = redisGet($key);

    return ($val !== null && $val !== '') ? $val : null;
}

/**
 * 认证设备
 *
 * @return array{ok:bool,reason:string}
 */
function authenticateDevice(string $clientId, string $username, string $password): array
{
    $hash = getDeviceSecret($clientId, $username);
    if ($hash === null) {
        return ['ok' => false, 'reason' => 'no secret'];
    }

    // bcrypt 验证（恒定时间，防时序攻击）
    if (!password_verify($password, $hash)) {
        return ['ok' => false, 'reason' => 'password mismatch'];
    }

    // 可选：自动升级哈希成本（如果以后调整了 bcrypt cost）
    if (password_needs_rehash($hash, PASSWORD_BCRYPT)) {
        $newHash = password_hash($password, PASSWORD_BCRYPT);
        $key = 'device:secret:' . $clientId . ':' . $username;
        redisSet($key, $newHash);
    }

    return ['ok' => true, 'reason' => 'ok'];
}

/**
 * POST /admin-api/extension/edge/auth
 */
function handleAuth(ServerConnection $conn, ServerRequest $req): void
{
    // ---------- 请求解析 ----------
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
    $password = trim((string) (
        $data['password'] ?? $data['signature'] ?? $data['token'] ?? ''
    ));

    // ---------- 参数校验 ----------
    if ($clientId === '' || $username === '' || $password === '') {
        metricsInc('auth_fail');
        respondJson($conn, [
            'result' => 'deny',
            'reason' => 'missing client_id / username / password'
        ], 401);
        return;
    }

    // ---------- 并发控制（全局） ----------
    if (!concurrencyAcquire()) {
        metricsInc('auth_fail');
        respondJson($conn, ['result' => 'deny', 'reason' => 'overloaded'], 503);
        return;
    }

    // ---------- 设备级并发控制 ----------
    $deviceToken = deviceConcurrencyAcquire($clientId);
    if ($deviceToken === null) {
        concurrencyRelease();
        metricsInc('auth_fail');
        metricsInc('device_concurrency_exceeded');
        respondJson($conn, [
            'result' => 'deny',
            'reason' => 'device_overloaded',
            'detail' => 'max ' . MAX_CONCURRENCY_PER_DEVICE . ' concurrent requests per device'
        ], 429);
        return;
    }

    // ---------- 限流 ----------
    $rateKey = 'rate:' . $clientId;
    if (rateLimited($rateKey)) {
        deviceConcurrencyRelease($deviceToken);
        concurrencyRelease();
        metricsInc('auth_fail');
        respondJson($conn, ['result' => 'deny', 'reason' => 'rate_limited'], 429);
        return;
    }

    // ---------- 熔断 ----------
    if (isClientBlocked($clientId)) {
        deviceConcurrencyRelease($deviceToken);
        concurrencyRelease();
        metricsInc('auth_fail');
        respondJson($conn, ['result' => 'deny', 'reason' => 'blocked'], 401);
        return;
    }

    try {
        // ---------- 认证 ----------
        $result = authenticateDevice($clientId, $username, $password);
        $authenticated = $result['ok'] ?? false;
        $reason = $result['reason'] ?? 'unknown';

        if (!$authenticated) {
            // 记录失败次数（用于熔断）
            if (in_array($reason, ['password mismatch', 'no secret'], true)) {
                recordClientFail($clientId);
            }

            metricsInc('auth_fail');
            respondJson($conn, ['result' => 'deny', 'reason' => $reason], 401);
            return;
        }

        // ---------- 认证成功 ----------
        metricsInc('auth_ok');
        resetRate($rateKey);
        respondJson($conn, ['result' => 'allow'], 200);

    } finally {
        deviceConcurrencyRelease($deviceToken);
        concurrencyRelease();
    }
}

/**
 * POST /admin-api/extension/edge/super
 */
function handleSuper(ServerConnection $conn, ServerRequest $req): void
{
    respondJson($conn, ['result' => 'deny', 'reason' => 'not_implemented'], 501);
}

/**
 * POST /admin-api/extension/edge/acl
 */
function handleAcl(ServerConnection $conn, ServerRequest $req): void
{
    respondJson($conn, ['result' => 'deny', 'reason' => 'not_implemented'], 501);
}
