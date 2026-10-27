<?php

declare(strict_types=1);

use Swow\Channel;
use Swow\Coroutine;

if (!defined('LOCAL_CACHE_TTL')) {
    define('LOCAL_CACHE_TTL', 60);
}

const LOCAL_CACHE_NULL_MARKER = '__NULL__';

function hmacAuthenticate(string $clientId, string $username, string $password): bool
{
    $parts = explode('|', $password, 3);
    if (count($parts) !== 3) {
        return false;
    }

    [$b64Sign, $timestamp, $nonce] = array_map('trim', $parts);

    // ✅ P0 修复：URL-safe base64 反向替换（-_~ → +/=）
    $sign = base64_decode(strtr($b64Sign, '-_~', '+/='), true);
    if ($sign === false || !ctype_digit($timestamp)) {
        return false;
    }

    $drift = defined('TIME_DRIFT') ? TIME_DRIFT : 120;
    if (abs((int) $timestamp - time()) > $drift) {
        return false;
    }

    $payload = $clientId . '|' . $username . '|' . $timestamp . '|' . $nonce;

    // ---------- secret 获取 + 空值缓存 ----------
    $cacheKey = 'secret:' . $clientId . ':' . $username;
    $secret = localCacheGet($cacheKey);

    if ($secret === null) {
        $secret = redisGet('device:secret:' . $clientId . ':' . $username);
        if (!is_string($secret) || $secret === '') {
            $secret = redisGet('device:secret:old:' . $clientId . ':' . $username);
        }
        if (is_string($secret) && $secret !== '') {
            localCacheSet($cacheKey, $secret, LOCAL_CACHE_TTL);
        } else {
            localCacheSet($cacheKey, LOCAL_CACHE_NULL_MARKER, 30);
        }
    } elseif ($secret === LOCAL_CACHE_NULL_MARKER) {
        return false;
    }

    if (!is_string($secret) || $secret === '') {
        return false;
    }

    // ---------- HMAC 校验（时序安全） ----------
    $expect = hash_hmac('sha256', $payload, $secret, true);
    if (!hash_equals($expect, $sign)) {
        return false;
    }

    // ---------- 并发吊销 + nonce 防重放 ----------
    $chR = new Channel(1);
    $chN = new Channel(1);

    Coroutine::run(static function () use ($clientId, $chR): void {
        try {
            $r = (bool) redisExists('device:revoke:' . $clientId);
            $chR->push($r, 2.0);
        } catch (Throwable) {
            $chR->push(false, 2.0);
        }
    });

    Coroutine::run(static function () use ($clientId, $username, $nonce, $chN): void {
        try {
            $nk = 'device:nonce:' . $clientId . ':' . $username;
            $existing = redisGet($nk);
            if ($existing === $nonce) {
                $chN->push(true, 2.0);
            } else {
                // ✅ P1 修复：nonce TTL 覆盖完整时间窗口
                $nonceTtl = defined('TIME_DRIFT') ? TIME_DRIFT * 2 : 300;
                $chN->push(redisSetNxEx($nk, $nonce, $nonceTtl), 2.0);
            }
        } catch (Throwable) {
            $chN->push(false, 2.0);
        }
    });

    try {
        $r = $chR->pop(2.0);
        $n = $chN->pop(2.0);
    } catch (Throwable) {
        fwrite(STDERR, '[hmac] sub-coroutine incomplete' . PHP_EOL);
        return false;
    }

    if ($r === null || $n === null) {
        fwrite(STDERR, '[hmac] sub-coroutine timeout' . PHP_EOL);
        return false;
    }

    if ($r || !$n) {
        return false;
    }

    return true;
}
