<?php

declare(strict_types=1);

use Swow\Psr7\Message\ServerRequest;
use Swow\Psr7\Server\ServerConnection;   // ✅ 修正：Server\ServerConnection

/* =========================================================
 * EdgeAuth HTTP 路由处理
 * ========================================================= */

/**
 * POST /extension/edge/gateway/auth
 */
function handleAuth(ServerConnection $conn, ServerRequest $req): void
{
    // ---------- 请求体解析：同时支持 JSON 和 form-urlencoded ----------
    $body = (string) $req->getBody();
    $contentType = strtolower($req->getHeaderLine('Content-Type'));

    if (str_contains($contentType, 'application/json')) {
        $data = json_decode($body, true) ?? [];
    } else {
        // form-urlencoded: clientid=device001&username=user001&password=...
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
        debugLog('MISSING_FIELDS: ct=' . $contentType . ' body_len=' . strlen($body));
        metricsInc('auth_fail');
        respondJson($conn, ['result' => 'deny', 'reason' => 'missing_fields'], 401);
        return;
    }

    // ---------- 限流 ----------
    $rateKey = 'rate:' . $clientId;
    if (rateLimited($rateKey)) {
        metricsInc('auth_fail');
        respondJson($conn, ['result' => 'deny', 'reason' => 'rate_limited'], 429);
        return;
    }

    // ---------- 熔断检查（快速拒绝，不走 HMAC） ----------
    if (isClientBlocked($clientId)) {
        debugLog('BLOCKED: ' . $clientId);
        metricsInc('auth_fail');
        respondJson($conn, ['result' => 'deny', 'reason' => 'blocked'], 401);
        return;
    }

    // ---------- 并发控制 ----------
    if (!concurrencyAcquire()) {
        metricsInc('auth_fail');
        respondJson($conn, ['result' => 'deny', 'reason' => 'overloaded'], 503);
        return;
    }

    try {
        // ---------- HMAC 认证 ----------
        $authenticated = hmacAuthenticate($clientId, $username, $password);
        $reason = detachAuthReason();

        if (!$authenticated) {
            // ★ 只对有攻击嫌疑的 reason 计 fail
            $attackReasons = [
                'signature mismatch',    // 格式全对但密钥错 → 试探攻击
            ];

            $shouldCountFail = in_array($reason, $attackReasons, true);

            if ($shouldCountFail) {
                recordClientFail($clientId);
                debugLog('FAIL RECORDED: ' . $clientId . ' reason=' . $reason);
            } else {
                debugLog('FAIL SKIPPED: ' . $clientId . ' reason=' . $reason . ' (not counted)');
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
        concurrencyRelease();
    }
}

/**
 * POST /extension/edge/gateway/super
 */
function handleSuper(ServerConnection $conn, ServerRequest $req): void
{
    respondJson($conn, ['result' => 'deny', 'reason' => 'not_implemented'], 501);
}

/**
 * POST /extension/edge/gateway/acl
 */
function handleAcl(ServerConnection $conn, ServerRequest $req): void
{
    respondJson($conn, ['result' => 'deny', 'reason' => 'not_implemented'], 501);
}
