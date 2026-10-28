<?php

declare(strict_types=1);

use Swow\Channel;
use Swow\Coroutine;

/* =========================================================
 * HMAC 认证入口（Swow 侧）
 * ========================================================= */

function hmacAuthenticate(string $clientId, string $username, string $password): bool
{
    debugLog('ENTER clientId=[' . $clientId . '] username=[' . $username . '] pw_len=' . strlen($password));

    $l1Key = 'secret:' . $clientId . ':' . $username;

    $result = \DagaSmart\Edge\Support\HmacValidator::verify(
        $clientId,
        $username,
        $password,

        // ★ secretResolver：自己管缓存，只用固定 key，不做 old 前缀猜测
        function (string $key) use ($l1Key, $clientId, $username): ?string {
            // 只响应 Validator 拼好的正式 key，忽略任何 old: 前缀的调用
            $expect = 'device:secret:' . $clientId . ':' . $username;
            if ($key !== $expect) {
                debugLog('secretResolver IGNORED key=[' . $key . '] (not canonical)');
                return null;
            }
            $secret = localCacheGet($l1Key);
            if (is_string($secret) && $secret !== '') {
                return $secret;
            }
            $secret = redisGet($expect);
            if (is_string($secret) && $secret !== '') {
                localCacheSet($l1Key, $secret, LOCAL_CACHE_TTL);
                debugLog('secretResolver CACHED clientId=[' . $clientId . ']');
            }
            return $secret;
        },

        // ★ 禁用 Validator 内部 L1：永远 miss，让它必须调 secretResolver
        fn(string $key) => null,

        // ★ 禁用 Validator 内部写缓存：防止它写 NULL_MARKER
        fn(string $key, $value, int $ttl) => null,

        defined('TIME_DRIFT') ? TIME_DRIFT : 300,
        LOCAL_CACHE_TTL
    );

    if (($result['valid'] ?? false) === false) {
        attachAuthReason($result['reason'] ?? 'unknown');
        debugLog('REJECT: ' . ($result['reason'] ?? 'unknown'));
        return false;
    }

    $nonce     = $result['nonce'];
    $timestamp = $result['timestamp'];
    debugLog('validator PASS → nonce=[' . $nonce . '] timestamp=[' . $timestamp . ']');

    $chR = new Channel(1);
    $chN = new Channel(1);

    Coroutine::run(static function () use ($clientId, $chR): void {
        try {
            $chR->push((bool) redisExists('device:revoke:' . $clientId), 2000);
        } catch (Throwable) { $chR->push(false, 2000); }
    });

    Coroutine::run(static function () use ($clientId, $username, $nonce, $chN): void {
        try {
            $nk = 'device:nonce:' . $clientId . ':' . $username;
            $existing = redisGet($nk);
            if ($existing === $nonce) {
                debugLog('nonce REPLAY detected for ' . $nk);
                $chN->push(false, 2000);
                return;
            }
            // ★ 用覆盖写替换 SET NX，key 存在且 nonce 不同也能成功
            $ttl = (defined('TIME_DRIFT') ? TIME_DRIFT : 300) * 2;
            if ($existing !== null) {
                redisDel($nk);
            }
            $ok = redisSetex($nk, $ttl, $nonce);
            debugLog('nonce WRITE nk=[' . $nk . '] ok=' . ($ok ? 'true' : 'false'));
            $chN->push($ok, 2000);
        } catch (Throwable) { $chN->push(false, 2000); }
    });

    try {
        $r = $chR->pop(2000);
        $n = $chN->pop(2000);
    } catch (Throwable) {
        attachAuthReason('sub-coroutine timeout');
        return false;
    }

    if ($r || !$n) {
        attachAuthReason($r ? 'revoked' : 'nonce_replay_or_set_failed');
        debugLog('REJECT: revoked=' . var_export($r, true) . ' nonce_ok=' . var_export($n, true));
        return false;
    }

    debugLog('ACCEPT: ' . $clientId);
    return true;
}

/* =========================================================
 * 协程上下文工具
 * ========================================================= */

function attachAuthReason(string $reason): void
{
    if (!class_exists('Swow\Coroutine')) {
        return;
    }
    $cid = \Swow\Coroutine::getCurrent()->getId();
    $GLOBALS['_auth_reason'][$cid] = $reason;
}

function detachAuthReason(): string
{
    if (!class_exists('Swow\Coroutine')) {
        return '';
    }
    $cid = \Swow\Coroutine::getCurrent()->getId();
    $reason = $GLOBALS['_auth_reason'][$cid] ?? '';
    unset($GLOBALS['_auth_reason'][$cid]);
    return $reason;
}
