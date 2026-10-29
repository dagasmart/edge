<?php
declare(strict_types=1);
namespace DagaSmart\Edge\Support;

/**
 * EdgeAuth HMAC 验证器
 * Swow 和 Laravel 共用，零框架依赖
 */
class HmacValidator
{
    public const string NULL_MARKER = '__NULL__';

    /**
     * @return array{valid: bool, reason?: string, nonce?: string, timestamp?: int}
     */
    public static function verify(
        string $clientId,
        string $username,
        string $password,
        callable $secretResolver,
        callable $cacheGet,
        callable $cacheSet,
        int $timeDrift = 300,
        int $cacheTtl = 60
    ): array {
        // ---------- 1. 解析密码字段 ----------
        $parts = explode('|', $password, 3);
        if (count($parts) !== 3) {
            return ['valid' => false, 'reason' => 'parts != 3'];
        }

        [$b64Sign, $timestamp, $nonce] = array_map('trim', $parts);

        // ---------- 2. 签名解码 ----------
        $sign = self::base64urlDecode($b64Sign);
        if ($sign === false || !ctype_digit($timestamp)) {
            return ['valid' => false, 'reason' => 'decode fail or bad timestamp'];
        }

        // ---------- 3. 时间窗 ----------
        if (abs((int) $timestamp - time()) > $timeDrift) {
            return ['valid' => false, 'reason' => 'time drift=' . ((int)$timestamp - time()) . 's'];
        }

        // ---------- 4. Secret + L1 缓存 ----------
        $cacheKey = 'secret:' . $clientId . ':' . $username;
        $secret = $cacheGet($cacheKey);

        if ($secret === null) {
            $secret = $secretResolver('device:secret:' . $clientId . ':' . $username);

            // fallback: old prefix
            if (!is_string($secret) || $secret === '') {
                $secret = $secretResolver('device:secret:old:' . $clientId . ':' . $username);
            }

            if (is_string($secret) && $secret !== '') {
                $cacheSet($cacheKey, $secret, $cacheTtl);
            } else {
                $cacheSet($cacheKey, self::NULL_MARKER, 30);
            }
        } elseif ($secret === self::NULL_MARKER) {
            return ['valid' => false, 'reason' => 'null-cache hit'];
        }

        if (!is_string($secret) || $secret === '') {
            return ['valid' => false, 'reason' => 'empty secret'];
        }

        // ---------- 5. HMAC 比对 ----------
        $payload = $clientId . '|' . $username . '|' . $timestamp . '|' . $nonce;
        $expect  = hash_hmac('sha256', $payload, $secret, true);

        if (!hash_equals($expect, $sign)) {
            return ['valid' => false, 'reason' => 'signature mismatch'];
        }

        // ---------- 6. 全部通过，返回结构化结果 ----------
        return [
            'valid'     => true,
            'nonce'     => $nonce,
            'timestamp' => (int) $timestamp,
        ];
    }

    public static function base64urlDecode(string $data): string|false
    {
        $b64 = strtr($data, '-_', '+/');
        $pad = strlen($b64) % 4;
        if ($pad > 0) {
            $b64 .= str_repeat('=', 4 - $pad);
        }
        return base64_decode($b64, true);
    }

    public static function base64urlEncode(string $data): string
    {
        return rtrim(strtr(base64_encode($data), '+/', '-_'), '=');
    }

    public static function sign(
        string $clientId,
        string $username,
        string $secret,
        int $timestamp,
        string $nonce
    ): string {
        $payload = $clientId . '|' . $username . '|' . $timestamp . '|' . $nonce;
        $raw = hash_hmac('sha256', $payload, $secret, true);
        return self::base64urlEncode($raw);
    }
}
