<?php
declare(strict_types=1);

use Swow\Psr7\Message\ServerRequest;
use Swow\Psr7\Server\ServerConnection;

/* =========================================================
 * EdgeAuth HTTP 路由处理
 * ========================================================= */

/**
 * POST /admin-api/extension/edge/auth
 */
function handleAuth(ServerConnection $conn, ServerRequest $req): void
{
    // ---------- 请求体解析 ----------
    $body = (string) $req->getBody();
    $contentType = strtolower($req->getHeaderLine('Content-Type'));

    if (str_contains($contentType, 'application/json')) {
        $data = json_decode($body, true) ?? [];
    } else {
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
        debugLog('MISSING_FIELDS: ct='.$contentType.' body_len='.strlen($body));
        metricsInc('auth_fail');
        respondJson($conn, ['result' => 'deny', 'reason' => 'missing_fields'], 401);

        return;
    }

    // ---------- 定期清理空闲设备桶 ----------
    deviceConcurrencyCleanup();

    // ---------- 全局并发控制 ----------
    if (! concurrencyAcquire()) {
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
        debugLog('DEVICE OVERLOADED: '.$clientId.' (max='.MAX_CONCURRENCY_PER_DEVICE.')');
        respondJson($conn, [
            'result' => 'deny',
            'reason' => 'device_overloaded',
            'detail' => 'max '.MAX_CONCURRENCY_PER_DEVICE.' concurrent requests per device',
        ], 429);

        return;
    }

    // ---------- 限流 ----------
    $rateKey = 'rate:'.$clientId;
    if (rateLimited($rateKey)) {
        deviceConcurrencyRelease($deviceToken);
        concurrencyRelease();
        metricsInc('auth_fail');
        respondJson($conn, ['result' => 'deny', 'reason' => 'rate_limited'], 429);

        return;
    }

    // ---------- 熔断检查 ----------
    if (isClientBlocked($clientId)) {
        debugLog('BLOCKED: '.$clientId);
        deviceConcurrencyRelease($deviceToken);
        concurrencyRelease();
        metricsInc('auth_fail');
        respondJson($conn, ['result' => 'deny', 'reason' => 'blocked'], 401);

        return;
    }

    try {
        // ---------- HMAC 认证 ----------
        $authenticated = hmacAuthenticate($clientId, $username, $password);
        $reason = detachAuthReason();

        if (! $authenticated) {
            $attackReasons = [
                'signature mismatch',
            ];

            $shouldCountFail = in_array($reason, $attackReasons, true);

            if ($shouldCountFail) {
                recordClientFail($clientId);
                debugLog('FAIL RECORDED: '.$clientId.' reason='.$reason);
            } else {
                debugLog('FAIL SKIPPED: '.$clientId.' reason='.$reason.' (not counted)');
            }

            metricsInc('auth_fail');
            respondJson($conn, ['result' => 'deny'], 401);

            return;
        }

        // ---------- 认证通过 ----------
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
