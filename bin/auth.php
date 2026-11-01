<?php

declare(strict_types=1);

use Dagasmart\Edge\Support\EdgeAuthContext;
use Swow\Psr7\Message\ServerRequest;
use Swow\Psr7\Server\ServerConnection;

/* =========================================================
 * EdgeAuth HTTP 路由处理 - 方案 A（bcrypt + HMAC 双路径）
 * 在线锁：严格单点登录 + 冲突静默窗口（防互踢重试风暴）
 * ========================================================= */

function getDeviceSecret(string $clientId, string $username): ?string
{
    $key = 'device:secret:' . $clientId . ':' . $username;
    $val = redisGet($key);

    return ($val !== null && $val !== '') ? $val : null;
}

function authenticateDevice(string $clientId, string $username, string $password): array
{
    $hash = getDeviceSecret($clientId, $username);
    if ($hash === null) {
        return ['ok' => false, 'reason' => 'no secret'];
    }

    if (! password_verify($password, $hash)) {
        return ['ok' => false, 'reason' => 'password mismatch'];
    }

    if (password_needs_rehash($hash, PASSWORD_BCRYPT)) {
        $newHash = password_hash($password, PASSWORD_BCRYPT);
        $key = 'device:secret:' . $clientId . ':' . $username;
        redisSetex($key, 0, $newHash);
    }

    return ['ok' => true, 'reason' => 'ok'];
}

/* =========================================================
 * 严格单点登录（Lua 原子，后登录 deny 不踢前）
 * 返回值：true=允许, false=拒绝（含静默期内的快速拒绝）
 * ========================================================= */

function acquireUserOnline(
    string $username,
    string $clientId,
    int $ttl = 300,
    int $retryWindow = 30
): bool {
    $key = 'auth:online:' . $username;
    $retryKey = 'auth:retry:' . $username . ':' . $clientId;

    $script = <<<'LUA'
local cur = redis.call('GET', KEYS[1])

-- 无人占用：抢占
if cur == false then
    redis.call('SET', KEYS[1], ARGV[1], 'EX', ARGV[2])
    redis.call('DEL', KEYS[2])
    return 1
end

-- 自己已占用：续期
if cur == ARGV[1] then
    redis.call('EXPIRE', KEYS[1], ARGV[2])
    redis.call('DEL', KEYS[2])
    return 1
end

-- 别人占用：检查静默窗口
local retrying = redis.call('GET', KEYS[2])
if retrying ~= false then
    return -1  -- 静默中，快速拒绝
end

-- 首次冲突：设置静默窗口
redis.call('SET', KEYS[2], '1', 'EX', ARGV[3])
return 0
LUA;

    try {
        $ret = withRedis(
            static function (Redis $conn) use (
                $key,
                $retryKey,
                $clientId,
                $ttl,
                $retryWindow,
                $script
            ): int {
                return (int) $conn->eval(
                    $script,
                    [$key, $retryKey, $clientId, $ttl, $retryWindow],
                    2
                );
            }
        );

        if ($ret === -1) {
            // 静默期内，记一次日志即可，不打 metrics 避免刷屏
            fwrite(STDERR, sprintf(
                '[acquireUserOnline] silent-window clientid=%s username=%s%s',
                $clientId,
                $username,
                PHP_EOL
            ));
        }

        return $ret === 1;
    } catch (Throwable $e) {
        fwrite(STDERR, '[acquireUserOnline] ' . $e->getMessage() . PHP_EOL);
        return false;
    }
}

function releaseUserOnline(string $username, string $clientId): void
{
    $key = 'auth:online:' . $username;
    $retryKey = 'auth:retry:' . $username . ':' . $clientId;

    $script = <<<'LUA'
if redis.call('GET', KEYS[1]) == ARGV[1] then
    redis.call('DEL', KEYS[1])
end
redis.call('DEL', KEYS[2])
return 0
LUA;

    try {
        withRedis(
            static function (Redis $conn) use ($key, $retryKey, $clientId, $script): void {
                $conn->eval($script, [$key, $retryKey, $clientId], 2);
            }
        );
    } catch (Throwable $e) {
        fwrite(STDERR, '[releaseUserOnline] ' . $e->getMessage() . PHP_EOL);
    }
}

function handleAuth(ServerConnection $conn, ServerRequest $req): void
{
    $body = (string) $req->getBody();
    $contentType = strtolower($req->getHeaderLine('Content-Type'));

    if (str_contains($contentType, 'application/json')) {
        $data = json_decode($body, true) ?? [];
    } else {
        $data = [];
        parse_str($body, $data);
    }

    $clientId = trim((string) (
        $data['clientid'] ?? $data['client_id'] ?? $data['clientId'] ?? ''
    ));
    $username = trim((string) (
        $data['username'] ?? $data['user'] ?? ''
    ));

    $password = trim((string) ($data['password'] ?? ''));
    $signature = trim((string) ($data['signature'] ?? $data['token'] ?? ''));

    error_log(sprintf(
        '[auth-debug] clientid=%s username=%s password_len=%d signature_len=%d',
        $clientId, $username, strlen($password), strlen($signature)
    ));

    if ($clientId === '' || $username === '') {
        metricsInc('auth_fail');
        respondJson($conn, ['result' => 'deny', 'reason' => 'missing client_id / username'], 200);
        return;
    }

    if (isClientBlocked($clientId)) {
        metricsInc('auth_fail');
        respondJson($conn, [
            'result' => 'deny',
            'reason' => 'client_blocked',
            'retry_after' => 600,
        ], 200);
        return;
    }

    if ($password === '' && $signature === '') {
        metricsInc('auth_fail');
        respondJson($conn, ['result' => 'deny', 'reason' => 'missing password or signature'], 200);
        return;
    }

    if (! concurrencyAcquire()) {
        metricsInc('auth_fail');
        respondJson($conn, ['result' => 'deny', 'reason' => 'overloaded'], 200);
        return;
    }

    $deviceToken = deviceConcurrencyAcquire($clientId);
    if ($deviceToken === null) {
        concurrencyRelease();
        metricsInc('auth_fail');
        metricsInc('device_concurrency_exceeded');
        respondJson($conn, [
            'result' => 'deny',
            'reason' => 'device_overloaded',
            'detail' => 'max ' . MAX_CONCURRENCY_PER_DEVICE . ' concurrent requests per device',
        ], 200);
        return;
    }

    $rateKey = 'rate:' . $clientId;
    if (rateLimited($rateKey)) {
        deviceConcurrencyRelease($deviceToken);
        concurrencyRelease();
        metricsInc('auth_fail');
        respondJson($conn, ['result' => 'deny', 'reason' => 'rate_limited'], 200);
        return;
    }

    try {
        $useHmac = ($signature !== '');

        if ($useHmac) {
            $authenticated = hmacAuthenticate($clientId, $username, $signature);
            if (! $authenticated) {
                $reason = EdgeAuthContext::getAuthReason() ?: 'hmac_failed';
                recordClientFail($clientId);
                metricsInc('auth_fail');
                respondJson($conn, ['result' => 'deny', 'reason' => $reason], 200);
                return;
            }
            $reason = 'ok';
        } else {
            $result = authenticateDevice($clientId, $username, $password);
            $authenticated = $result['ok'] ?? false;
            $reason = $result['reason'] ?? 'unknown';

            if (! $authenticated) {
                if (in_array($reason, ['password mismatch', 'no secret'], true)) {
                    recordClientFail($clientId);
                }
                metricsInc('auth_fail');
                respondJson($conn, ['result' => 'deny', 'reason' => $reason], 200);
                return;
            }
        }

        // ★ 严格单点 + 静默窗口
        if (! acquireUserOnline($username, $clientId, 300, 30)) {
            metricsInc('auth_fail');
            metricsInc('auth_conflict');
            respondJson($conn, [
                'result' => 'deny',
                'reason' => 'already_logged_in',
                'detail' => '同一账号已在其他设备登录',
                'retry_after' => 30,
            ], 200);
            return;
        }

        resetRate($rateKey);
        try {
            withRedis(static function (Redis $conn) use ($clientId): void {
                $conn->del('fail:' . $clientId, 'block:' . $clientId);
            });
        } catch (Throwable) {
        }

        EdgeAuthContext::setAuth($username, $clientId);

        metricsInc('auth_ok');
        respondJson($conn, ['result' => 'allow', 'reason' => $reason], 200);

    } finally {
        deviceConcurrencyRelease($deviceToken);
        concurrencyRelease();

        try {
            $authedUsername = EdgeAuthContext::getUsername();
            $authedClientId = EdgeAuthContext::getClientId();
            if ($authedUsername !== null && $authedClientId !== null) {
                releaseUserOnline($authedUsername, $authedClientId);
            }
        } catch (Throwable $e) {
            fwrite(STDERR, '[release-online] ' . $e->getMessage() . PHP_EOL);
        } finally {
            EdgeAuthContext::clear();
        }
    }
}

function handleSuper(ServerConnection $conn, ServerRequest $req): void
{
    respondJson($conn, ['result' => 'deny', 'reason' => 'not_implemented'], 501);
}

function handleAcl(ServerConnection $conn, ServerRequest $req): void
{
    respondJson($conn, ['result' => 'deny', 'reason' => 'not_implemented'], 501);
}
