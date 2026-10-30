<?php
declare(strict_types=1);

namespace DagaSmart\Edge\Services;

use DagaSmart\Edge\Models\EdgeAuthorize;
use Illuminate\Support\Facades\Hash;
use Illuminate\Support\Facades\Log;

/**
 * 网关-服务类
 * 对接 NanoMQ HTTP 认证 / 超级用户 / ACL 鉴权
 *
 * 认证方式：bcrypt 密码比对
 *   - DB 字段：EdgeAuthorize::$password（Hash::make 生成）
 *   - Redis key：device:secret:{client_id}:{username}（存同一个 bcrypt 哈希）
 *   - Swow 端用 password_verify 比对
 */
class EdgeAuthorizeService extends AdminService
{
    protected string $modelName = EdgeAuthorize::class;

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

        $user = EdgeAuthorize::query()
            ->where('username', $username)
            ->where('active', true)
            ->when($service, fn ($q) => $q->where('service', $service))
            ->first();

        if (! $user) {
            Log::channel('edge')->warning('EdgeGateway auth: user not found', [
                'username' => $username,
                'service'  => $service,
                'clientid' => $clientid,
            ]);

            return null;
        }

        $hash = $user->password;
        if (! is_string($hash) || $hash === '' || ! Hash::check($password, $hash)) {
            Log::channel('edge')->warning('EdgeGateway auth: password mismatch', [
                'username' => $username,
                'clientid' => $clientid,
            ]);

            return null;
        }

        return $user;
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

        $key = "device:secret:{$device->client_id}:{$device->username}";
        $redis = new \Redis;

        try {
            $redis->connect('127.0.0.1', 16379, 2);
            $redis->auth('uApDVQD7');
            $redis->select(2);

            if (! $device->active) {
                $redis->del($key);
                Log::channel('edge')->info('EdgeAuth Redis: key removed (inactive)', [
                    'key' => $key,
                    'service' => $device->service,
                ]);

                return;
            }

            $hash = $device->password;

            // 防御：不是合法 bcrypt 哈希则跳过，避免写脏数据
            if (! is_string($hash) || $hash === '' || ! str_starts_with($hash, '$2y$')) {
                Log::channel('edge')->warning('EdgeAuth Redis: invalid password hash, skip sync', [
                    'key' => $key,
                ]);

                return;
            }

            $redis->set($key, $hash);

            Log::channel('edge')->info('EdgeAuth Redis: key synced (bcrypt)', [
                'key'     => $key,
                'service' => $device->service,
            ]);
        } catch (\Exception $e) {
            Log::channel('edge')->error('EdgeAuth Redis: sync failed', [
                'error' => $e->getMessage(),
                'key'   => $key,
            ]);
        } finally {
            try {
                $redis->close();
            } catch (\Exception $e) {
            }
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

        $key = "device:secret:{$device->client_id}:{$device->username}";
        $redis = new \Redis;

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
            try {
                $redis->close();
            } catch (\Exception $e) {
            }
        }
    }
}
