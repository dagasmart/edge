<?php

declare(strict_types=1);

use Swow\Psr7\Server\ServerConnection;
use Swow\Psr7\Message\ServerRequest;

const AUTH_CACHE_VALID = '1';

function handleAuth(ServerConnection $conn, ServerRequest $req): void
{
    metricsInc('auth_total');

    if (!concurrencyAcquire(2.0)) {
        $conn->respond('', 503);
        return;
    }

    try {
        // ✅ 手动解析 application/x-www-form-urlencoded
        $rawBody = (string) $req->getBody()->getContents();
        parse_str($rawBody, $body);

        $clientId = (string) ($body['clientid'] ?? '');
        $username = (string) ($body['username'] ?? '');
        $password = (string) ($body['password'] ?? '');

        // ✅ IP：只从真实 TCP 连接取，不信任任何 Header
        try {
            $ip = (string) $conn->getPeerAddress();
        } catch (Throwable) {
            $ip = 'unknown';
        }

        if ($clientId === '' || $username === '' || $password === '') {
            $conn->respond('', 401);
            metricsInc('auth_fail');
            return;
        }

        if (isClientBlocked($clientId)) {
            $conn->respond('', 403);
            metricsInc('auth_fail');
            return;
        }

        if (rateLimited('rate:ip:' . $ip)) {
            $conn->respond('', 429);
            metricsInc('auth_fail');
            return;
        }

        // ✅ 缓存 key 包含 nonce，防重放绕过
        $passParts = explode('|', $password, 3);
        $nonce = $passParts[2] ?? '';
        $cacheKey = 'auth:hmac:' . $clientId . ':' . $username . ':' . $nonce;

        $cached = redisGet($cacheKey);

        if ($cached === AUTH_CACHE_VALID) {
            // 缓存命中仍然检查吊销
            if (redisExists('device:revoke:' . $clientId)) {
                redisDel($cacheKey);
                $conn->respond('', 401);
                metricsInc('auth_fail');
                return;
            }
            metricsInc('cache_hit');
            $conn->respond('', 200);
            metricsInc('auth_ok');
            return;
        }

        // 完整校验
        $ok = hmacAuthenticate($clientId, $username, $password);

        if ($ok) {
            redisSetex($cacheKey, CACHE_TTL, AUTH_CACHE_VALID);
            // ✅ 不重置限流，成功也计入窗口
            $conn->respond('', 200);
            metricsInc('auth_ok');
        } else {
            recordClientFail($clientId);
            $conn->respond('', 401);
            metricsInc('auth_fail');
        }
    } finally {
        concurrencyRelease();
    }
}
