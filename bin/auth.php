<?php
declare(strict_types=1);

use Swow\Psr7\Server\ServerConnection;
use Swow\Psr7\Message\ServerRequest;

function handleAuth(ServerConnection $conn, ServerRequest $req): void
{
    metricsInc('auth_total');

    if (!concurrencyAcquire(2.0)) {
        $conn->respond('', 503);
        return;
    }

    try {
        $body     = $req->getParsedBody();
        $clientId = (string)($body['clientid'] ?? '');
        $username = (string)($body['username'] ?? '');
        $password = (string)($body['password'] ?? '');
        $ip       = (string)($body['ipaddr'] ?? '');

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

        if (rateLimited("rate:ip:{$ip}")) {
            $conn->respond('', 429);
            metricsInc('auth_fail');
            return;
        }

        // 缓存命中
        $cacheKey = "auth:hmac:{$clientId}:{$username}";
        $cached   = redisGet($cacheKey);

        if ($cached === '1') {
            if (redisExists("device:revoke:{$clientId}")) {
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
            redisSetex($cacheKey, CACHE_TTL, '1');
            resetRate("rate:ip:{$ip}");
            $conn->respond('', 200);
            metricsInc('auth_ok');
        } else {
            recordClientFail($clientId);
            hitRate("rate:ip:{$ip}");
            $conn->respond('', 401);
            metricsInc('auth_fail');
        }
    } finally {
        concurrencyRelease();
    }
}
