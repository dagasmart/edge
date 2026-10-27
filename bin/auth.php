<?php
declare(strict_types=1);

use Swow\Psr7\Server\ServerConnection;
use Swow\Psr7\Message\ServerRequest;

function handleAuth(ServerConnection $conn, ServerRequest $req): void
{
    $body     = $req->getParsedBody();
    $clientId = (string)($body['clientid'] ?? '');
    $password = (string)($body['password'] ?? '');
    $ip       = (string)($body['ipaddr'] ?? '');

    if ($clientId === '' || $password === '') {
        $conn->respond('', 401);
        return;
    }

    // ---------- IP 限流 ----------
    if (rateLimited($ip)) {
        $conn->respond('', 429);
        return;
    }

    // ---------- 认证结果缓存（带吊销穿透） ----------
    $cacheKey = "auth:hmac:{$clientId}";
    $cached   = redisGet($cacheKey);

    if ($cached === '1') {
        // 穿透检查吊销：吊销优先于缓存
        if (redisExists("device:revoke:{$clientId}")) {
            redisDel($cacheKey);
            fwrite(STDERR, "[auth] cache bypass: revoked {$clientId}\n");
            $conn->respond('', 401);
            return;
        }
        // 未吊销，直接放行
        $conn->respond('', 200);
        return;
    }

    // ---------- 走完整 HMAC 校验 ----------
    $ok = hmacAuthenticate($clientId, $password);

    if ($ok) {
        redisSetex($cacheKey, CACHE_TTL, '1');
        resetRate($ip);
        $conn->respond('', 200);
    } else {
        hitRate($ip);
        $conn->respond('', 401);
    }
}
