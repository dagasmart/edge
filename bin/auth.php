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

    if (rateLimited($ip)) {
        $conn->respond('', 429);
        return;
    }

    // 认证结果缓存
    $cacheKey = "auth:hmac:{$clientId}";
    if (redisGet($cacheKey) === '1') {
        $conn->respond('', 200);
        return;
    }

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
