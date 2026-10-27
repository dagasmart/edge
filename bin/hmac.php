<?php
declare(strict_types=1);

use Swow\Channel;
use Swow\Coroutine;

function hmacAuthenticate(string $clientId, string $username, string $password): bool
{
    $parts = explode('|', $password, 3);
    if (count($parts) !== 3) {
        return false;
    }

    [$b64Sign, $timestamp, $nonce] = array_map('trim', $parts);
    $sign = base64_decode($b64Sign, true);
    if ($sign === false || !ctype_digit($timestamp)) {
        return false;
    }

    $drift = defined('TIME_DRIFT') ? TIME_DRIFT : 120;
    if (abs((int)$timestamp - time()) > $drift) {
        return false;
    }

    $payload = $clientId . '|' . $username . '|' . $timestamp . '|' . $nonce;

    // ---------- secret 获取 + 空值缓存 ----------
    $cacheKey = "secret:{$clientId}:{$username}";
    $secret = localCacheGet($cacheKey);

    if ($secret === null) {
        $secret = redisGet("device:secret:{$clientId}:{$username}");
        if (!is_string($secret) || $secret === '') {
            $secret = redisGet("device:secret:old:{$clientId}:{$username}");
        }
        if (is_string($secret) && $secret !== '') {
            localCacheSet($cacheKey, $secret, LOCAL_CACHE_TTL);
        } else {
            localCacheSet($cacheKey, '__NULL__', 30); // ✅ 空缓存防穿透
        }
    } elseif ($secret === '__NULL__') {
        return false;
    }

    if (!is_string($secret) || $secret === '') {
        return false;
    }

    // ---------- HMAC 校验 ----------
    $expect = hash_hmac('sha256', $payload, $secret, true);
    if (!hash_equals($expect, $sign)) {
        return false;
    }

    // ---------- 并发吊销 + nonce ----------
    $ch = new Channel(2);
    $revoked = false;
    $nonceOk = false;

    Coroutine::run(function () use ($clientId, $ch, &$revoked): void {
        try {
            $revoked = redisExists("device:revoke:{$clientId}");
        } catch (\Throwable) {}
        $ch->push('r');
    });

    Coroutine::run(function () use ($clientId, $username, $nonce, $ch, &$nonceOk): void {
        try {
            $nk = "device:nonce:{$clientId}:{$username}";
            $existing = redisGet($nk);
            if ($existing === $nonce) {
                $nonceOk = true; // ✅ 同 nonce 在有效期内放行
            } else {
                $nonceOk = redisSetNxEx($nk, $nonce, 120);
            }
        } catch (\Throwable) {}
        $ch->push('n');
    });

    $done = 0;
    while ($done < 2) {
        try {
            $ch->pop(2);
            $done++;
        } catch (\Throwable) {
            break;
        }
    }

    if ($revoked || !$nonceOk) {
        return false;
    }

    return true;
}
