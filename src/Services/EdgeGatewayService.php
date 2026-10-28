<?php
declare(strict_types=1);
namespace DagaSmart\Edge\Services;

use DagaSmart\Edge\Models\EdgeAuthorize;
use DagaSmart\Edge\Models\EdgeGateway;
use Illuminate\Support\Facades\Hash;
use Illuminate\Support\Facades\Log;

/**
 * 网关-服务类
 * 对接 NanoMQ HTTP 认证 / 超级用户 / ACL 鉴权
 */
class EdgeGatewayService extends AdminService
{
    protected string $modelName = EdgeGateway::class;

    // ==================== 超级用户白名单 ====================
    protected array $superUsers = ['admin', 'gateway01', 'bench_client'];

    // ==================== 认证 ====================

    /**
     * 网关用户认证（NanoMQ auth_req 调用）
     */
    public function auth($username, $password, $clientid = null, $ipaddr = null, $service = 'nano'): ?EdgeAuthorize
    {
        if (empty($clientid)) {
            return null;
        }

        $query = EdgeAuthorize::query()
            ->where('username', $username)
            ->where('active', true);

        if (!empty($service)) {
            $query->where('service', $service);
        }

        $user = $query->first();

        if (!$user) {
            Log::channel('edge')->warning('EdgeGateway auth: user not found', [
                'username' => $username,
                'service'  => $service,
                'clientid' => $clientid,
            ]);
            return null;
        }

        // ---- HMAC 签名路径 ----
        if ($this->isHmacSignature($password)) {
            $valid = $this->verifyHmacSignature($clientid, $username, $password);
            if ($valid) {
                return $user;
            }
            Log::channel('edge')->warning('EdgeGateway auth: HMAC verify failed', [
                'username' => $username,
                'clientid' => $clientid,
            ]);
            return null;
        }

        // ---- 明文密码 Hash 比对 ----
        if (Hash::check($password, $user->password)) {
            return $user;
        }

        Log::channel('edge')->warning('EdgeGateway auth: password mismatch', [
            'username' => $username,
            'clientid' => $clientid,
        ]);
        return null;
    }

    // ==================== 超级用户 ====================

    /**
     * 超级用户判定（NanoMQ super_req 调用）
     */
    public function isSuperUser(string $username, string $clientid, string $service = 'nano'): bool
    {
        if (in_array($username, $this->superUsers, true) || in_array($clientid, $this->superUsers, true)) {
            return true;
        }

        $user = EdgeAuthorize::query()
            ->where('username', $username)
            ->where('active', true)
            ->where('service', $service)
            ->first();

        return $user && $user->is_super;
    }

    // ==================== ACL ====================

    /**
     * ACL 鉴权（NanoMQ acl_req 调用）
     */
    public function checkAcl(string $clientid, string $username, string $topic, int $access, string $ipaddr): bool
    {
        if ($this->isSuperUser($username, $clientid, 'nano')) {
            return true;
        }

        $prefix = $clientid;

        if ($access === 2) {
            $pattern = '#^' . preg_quote($prefix, '#') . '/up(/|$)#';
            $allowed = (bool) preg_match($pattern, $topic);

            if (!$allowed) {
                Log::channel('edge')->info('EdgeGateway acl: publish denied', [
                    'clientid' => $clientid,
                    'topic'    => $topic,
                ]);
            }
            return $allowed;
        }

        if ($access === 1) {
            $pattern1 = '#^' . preg_quote($prefix, '#') . '/down(/|$)#';
            $pattern2 = '#^' . preg_quote($prefix, '#') . '/up/response(/|$)#';

            $allowed = (bool) (preg_match($pattern1, $topic) || preg_match($pattern2, $topic));

            if (!$allowed) {
                Log::channel('edge')->info('EdgeGateway acl: subscribe denied', [
                    'clientid' => $clientid,
                    'topic'    => $topic,
                ]);
            }
            return $allowed;
        }

        return false;
    }

    // ==================== 内部工具 ====================

    /**
     * 判断 password 是否为 HMAC 签名格式
     */
    protected function isHmacSignature(string $password): bool
    {
        $parts = explode('|', $password);
        return count($parts) === 3 && strlen($parts[0]) > 0 && is_numeric($parts[1]);
    }

    /**
     * HMAC 签名校验
     */
    protected function verifyHmacSignature(?string $clientid, string $username, string $password): bool
    {
        if (empty($clientid)) {
            return false;
        }

        $parts     = explode('|', $password, 3);
        $b64Sign   = $parts[0];
        $timestamp = $parts[1];
        $nonce     = $parts[2];

        // 时间窗口 ±300s
        if (abs(time() - (int) $timestamp) > 300) {
            Log::channel('edge')->warning('EdgeGateway acl: timestamp expired', [
                'timestamp' => $timestamp,
                'now'       => time(),
            ]);
            return false;
        }

        $redisKey = "device:secret:{$clientid}:{$username}";
        $secret   = null;

        try {
            $redis = app('redis')->connection('edge');
            $secret = $redis->get($redisKey);
        } catch (\Exception $e) {
            $secret = $this->getSecretFromNativeRedis($clientid, $username);
        }

        if (empty($secret)) {
            Log::channel('edge')->warning('EdgeGateway acl: secret not found in Redis', [
                'key' => $redisKey,
            ]);
            return false;
        }

        $payload  = "{$clientid}|{$username}|{$timestamp}|{$nonce}";
        $rawSign  = hash_hmac('sha256', $payload, $secret, true);
        $expected = rtrim(strtr(base64_encode($rawSign), '+/', '-_'), '=');

        if (!hash_equals($expected, $b64Sign)) {
            Log::channel('edge')->warning('EdgeGateway acl: signature mismatch', [
                'clientid' => $clientid,
                'username' => $username,
            ]);
            return false;
        }

        return true;
    }

    /**
     * Fallback：原生 Redis 取密钥
     */
    protected function getSecretFromNativeRedis(string $clientid, string $username): ?string
    {
        try {
            $redis = new \Redis();
            $redis->connect('127.0.0.1', 16379, 2);
            $redis->auth('uApDVQD7');
            $redis->select(2);
            return $redis->get("device:secret:{$clientid}:{$username}") ?: null;
        } catch (\Exception $e) {
            Log::channel('edge')->error('EdgeGateway: native redis failed', [
                'error' => $e->getMessage(),
            ]);
            return null;
        }
    }

    // ==================== 后台同步 Redis ====================

    /**
     * 保存后同步 Redis（由 EdgeAuthorize::booted() 调用）
     */
    public function syncRedisOnSave(EdgeAuthorize $device): void
    {
        if (empty($device->client_id) || empty($device->username)) {
            return;
        }

        $key   = "device:secret:{$device->client_id}:{$device->username}";
        $redis = new \Redis();

        try {
            $redis->connect('127.0.0.1', 16379, 2);
            $redis->auth('uApDVQD7');
            $redis->select(2);

            if (!$device->active) {
                $redis->del($key);
                Log::channel('edge')->info('EdgeAuth Redis: key removed (inactive)', [
                    'key'     => $key,
                    'service' => $device->service,
                ]);
                return;
            }

            $secret = $this->getSecretForDevice($device);
            if ($secret === null || $secret === '') {
                $redis->del($key);
                Log::channel('edge')->warning('EdgeAuth Redis: secret not found, key removed', [
                    'key' => $key,
                ]);
                return;
            }

            $redis->set($key, $secret);
            Log::channel('edge')->info('EdgeAuth Redis: key synced', [
                'key'     => $key,
                'service' => $device->service,
                'len'     => strlen($secret),
            ]);
        } catch (\Exception $e) {
            Log::channel('edge')->error('EdgeAuth Redis: sync failed', [
                'error' => $e->getMessage(),
                'key'   => $key,
            ]);
        } finally {
            try { $redis->close(); } catch (\Exception $e) {}
        }
    }

    /**
     * 删除后清理 Redis（由 EdgeAuthorize::booted() 调用）
     */
    public function removeRedisOnDelete(EdgeAuthorize $device): void
    {
        if (empty($device->client_id) || empty($device->username)) {
            return;
        }

        $key   = "device:secret:{$device->client_id}:{$device->username}";
        $redis = new \Redis();

        try {
            $redis->connect('127.0.0.1', 16379, 2);
            $redis->auth('uApDVQD7');
            $redis->select(2);
            $redis->del($key);
            Log::channel('edge')->info('EdgeAuth Redis: key removed (deleted)', [
                'key'     => $key,
                'service' => $device->service,
            ]);
        } catch (\Exception $e) {
            Log::channel('edge')->error('EdgeAuth Redis: remove failed', [
                'error' => $e->getMessage(),
                'key'   => $key,
            ]);
        } finally {
            try { $redis->close(); } catch (\Exception $e) {}
        }
    }

    /**
     * 获取设备 HMAC 密钥
     *
     * 唯一来源：DB 的 secret 字段（后台管理的权威来源）
     * secrets.php 已废弃，不再作为兜底。
     */
    protected function getSecretForDevice(EdgeAuthorize $device): ?string
    {
        if (!empty($device->secret)) {
            return $device->secret;
        }

        Log::channel('edge')->warning('EdgeAuth: device has no secret in DB', [
            'client_id' => $device->client_id,
            'username'  => $device->username,
            'id'        => $device->id ?? null,
        ]);

        return null;
    }


}
