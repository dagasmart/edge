<?php
declare(strict_types=1);

use Swow\Channel;
use Swow\Coroutine;

// ✅ 本地缓存空值标记常量
const LOCAL_CACHE_NULL_MARKER = '__NULL__';

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
            localCacheSet($cacheKey, LOCAL_CACHE_NULL_MARKER, 30); // 空缓存防穿透
        }
    } elseif ($secret === LOCAL_CACHE_NULL_MARKER) {
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

    // ---------- 并发吊销 + nonce（修复版）----------
    // ✅ Channel 容量设为 2，子协程 push 不会因满而阻塞抛异常
    $ch = new Channel(2);
    $results = ['r' => false, 'n' => false];

    Coroutine::run(function () use ($clientId, $ch, &$results): void {
        try {
            $results['r'] = (bool) redisExists("device:revoke:{$clientId}");
        } catch (\Throwable) {
            $results['r'] = false;
        } finally {
            // ✅ 确保必 push，主协程不会死等
            try { $ch->push('r'); } catch (\Throwable) {}
        }
    });

    Coroutine::run(function () use ($clientId, $username, $nonce, $ch, &$results): void {
        try {
            $nk = "device:nonce:{$clientId}:{$username}";
            $existing = redisGet($nk);
            if ($existing === $nonce) {
                $results['n'] = true; // 同 nonce 在有效期内放行
            } else {
                $results['n'] = redisSetNxEx($nk, $nonce, 120);
            }
        } catch (\Throwable) {
            $results['n'] = false;
        } finally {
            // ✅ 确保必 push，主协程不会死等
            try { $ch->push('n'); } catch (\Throwable) {}
        }
    });

    // 等待两个协程完成，最多等 2 秒
    $done = 0;
    while ($done < 2) {
        try {
            $ch->pop(2.0);
            $done++;
        } catch (\Throwable) {
            break; // 超时或异常
        }
    }

    // ✅ 只有两个都返回才信任结果；不完整则降级拒绝，避免基于脏状态误判
    if ($done < 2) {
        fwrite(STDERR, "[hmac] sub-coroutine incomplete ({$done}/2)\n");
        return false;
    }

    if ($results['r'] || !$results['n']) {
        return false;
    }

    return true;
}
