<?php
declare(strict_types=1);

namespace DagaSmart\Edge\Services;

use Illuminate\Support\Facades\Redis;
use RuntimeException;

/**
 * MQTT HMAC 签名服务
 *
 * 签名规则（与 Swow 服务端完全对称）：
 *   payload  = clientId|timestamp|nonce
 *   password = base64(hmac_sha256(payload, secret))|timestamp|nonce
 */
final class EdgeAclEngineService
{
    private const string ALGO = 'sha256';
    private const int NONCE_BYTES = 8;

    /**
     * 生成 MQTT CONNECT 凭据
     */
    public static function sign(string $clientId, string $secret, ?int $timestamp = null): array
    {
        if ($clientId === '' || $secret === '') {
            throw new RuntimeException('clientId 和 secret 不能为空');
        }

        $ts    = $timestamp ?? time();
        $nonce = bin2hex(random_bytes(self::NONCE_BYTES));
        $payload = "{$clientId}|{$ts}|{$nonce}";

        $rawSign = hash_hmac(self::ALGO, $payload, $secret, true);
        if ($rawSign === false) {
            throw new RuntimeException('HMAC 签名失败');
        }

        $sign     = base64_encode($rawSign);
        $password = "{$sign}|{$ts}|{$nonce}";

        return [
            'client_id' => $clientId,
            'username'  => $clientId,
            'password'  => $password,
            'payload'   => $payload,
            'sign'      => $sign,
            'timestamp' => $ts,
            'nonce'     => $nonce,
        ];
    }

    /**
     * 验证签名（服务端用，客户端一般不需要）
     */
    public static function verify(string $clientId, string $password, string $secret, int $timeDrift = 300): bool
    {
        $parts = explode('|', $password, 3);
        if (count($parts) !== 3) {
            return false;
        }

        [$b64Sign, $timestamp, $nonce] = $parts;

        if (!ctype_digit($timestamp)) {
            return false;
        }

        // 时间漂移校验
        if (abs((int)$timestamp - time()) > $timeDrift) {
            return false;
        }

        $payload = "{$clientId}|{$timestamp}|{$nonce}";
        $expect  = hash_hmac(self::ALGO, $payload, $secret, true);
        $actual  = base64_decode($b64Sign, true);

        if ($actual === false) {
            return false;
        }

        return hash_equals($expect, $actual);
    }

    /**
     * 从 Redis 读取密钥并签名（一行调用）
     */
    public static function signFromRedis(string $clientId, string $redisKey = null): array
    {
        $key = $redisKey ?? "device:secret:{$clientId}";

        // 用 Laravel Redis facade
        $secret = Redis::get($key);
        if ($secret === null) {
            throw new RuntimeException("Redis 中未找到密钥: {$key}");
        }

        return self::sign($clientId, $secret);
    }
}
