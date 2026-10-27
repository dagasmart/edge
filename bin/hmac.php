<?php
declare(strict_types=1);

use Swow\Coroutine;
use Swow\Channel;

function hmacAuthenticate(string $clientId, string $password): bool
{
    $parts = explode('|', $password, 3);
    if (count($parts) !== 3) {
        fwrite(STDERR, "[hmac] parts mismatch\n");
        return false;
    }

    [$b64Sign, $timestamp, $nonce] = array_map('trim', $parts);
    $sign = base64_decode($b64Sign, true);
    if ($sign === false || !ctype_digit($timestamp)) {
        fwrite(STDERR, "[hmac] base64 or timestamp invalid\n");
        return false;
    }

    if (abs((int)$timestamp - time()) > TIME_DRIFT) {
        fwrite(STDERR, "[hmac] timestamp drift\n");
        return false;
    }

    $payload = $clientId . '|' . $timestamp . '|' . $nonce;

    // ---------- L1 本地缓存：秘钥（双密钥支持） ----------
    $cacheKey = "secret:{$clientId}";
    $secret = localCacheGet($cacheKey);
    if ($secret === null) {
        // 先查主密钥
        $secret = redisGet("device:secret:{$clientId}");

        // 主密钥不存在，查过渡期旧密钥
        if (!is_string($secret) || $secret === '') {
            $secret = redisGet("device:secret:old:{$clientId}");
        }

        if (is_string($secret) && $secret !== '') {
            localCacheSet($cacheKey, $secret, LOCAL_CACHE_TTL);
        }
    }

    if (!is_string($secret) || $secret === '') {
        fwrite(STDERR, "[hmac] no secret for {$clientId}\n");
        return false;
    }

    // ---------- HMAC 校验 ----------
    $expect = hash_hmac('sha256', $payload, $secret, true);
    if (!hash_equals($expect, $sign)) {
        fwrite(STDERR, "[hmac] signature mismatch\n");
        return false;
    }

    // ---------- 并发查吊销 + nonce ----------
    $ch = new Channel(2);
    $revoked = false;
    $nonceOk = false;

    Coroutine::run(function () use ($clientId, $ch, &$revoked): void {
        try {
            $revoked = redisExists("device:revoke:{$clientId}");
        } catch (\Throwable $e) {
            fwrite(STDERR, "[hmac] revoke check: " . $e->getMessage() . "\n");
        }
        $ch->push('revoke');
    });

    Coroutine::run(function () use ($clientId, $nonce, $ch, &$nonceOk): void {
        try {
            $nonceOk = redisSetNxEx("device:nonce:{$clientId}", $nonce, 600);
        } catch (\Throwable $e) {
            fwrite(STDERR, "[hmac] nonce set: " . $e->getMessage() . "\n");
        }
        $ch->push('nonce');
    });

    // 等待完成（2秒超时）
    $done = 0;
    while ($done < 2) {
        try {
            $ch->pop(2);
            $done++;
        } catch (\Throwable) {
            break;
        }
    }

    if ($revoked) {
        fwrite(STDERR, "[hmac] revoked: {$clientId}\n");
        return false;
    }

    if (!$nonceOk) {
        fwrite(STDERR, "[hmac] nonce replay\n");
        return false;
    }

    return true;
}
