<?php
declare(strict_types=1);

namespace DagaSmart\Edge\Models;

use Carbon\Carbon;
use DagaSmart\Edge\Services\EdgeAuthorizeService;
use Illuminate\Support\Facades\Log;

/**
 * 边缘网关认证模型
 *
 * 负责：
 *  - 终端认证账号的存储
 *  - password 字段自动 bcrypt 加密
 *  - secret 密钥的自动生成与时间戳维护
 *  - 模型事件驱动 Redis 密钥同步
 *
 * @property int         $id
 * @property string      $username        账号
 * @property string      $password        密码（bcrypt hash 存储）
 * @property string      $client_id       终端 SN
 * @property bool        $active          是否启用
 * @property bool        $is_super        是否超管
 * @property string      $service         网关类型（nano / mqtt / emqx）
 * @property string      $secret          签名密钥
 * @property Carbon|null $secret_change_at 密钥变更时间
 * @property string|null $remark          备注
 */
class EdgeAuthorize extends Model
{
    protected $table = 'mcs_edge_authorize';

    protected $fillable = [
        'username',
        'password',
        'client_id',
        'active',
        'is_super',
        'service',
        'secret',
        'secret_change_at',
        'remark',
    ];

    protected $casts = [
        'active'           => 'boolean',
        'is_super'         => 'boolean',
        'secret_change_at' => 'datetime',
        'created_at'       => 'datetime',
        'updated_at'       => 'datetime',
    ];

    // ==================== Mutator ====================

    /**
     * password 字段自动 bcrypt 加密
     *
     * - 如果已经是 hash（以 $2y$ / $argon2 开头），不再重复加密
     * - 避免明文入库
     * - Laravel 13 推荐用 Hash::make()，底层默认 bcrypt
     */
    public function setPasswordAttribute($value): void
    {
        if ($value === null || $value === '') {
            $this->attributes['password'] = $value;
            return;
        }

        // 已经是 hash，不再加密（防止二次 hash 导致校验失败）
        if (preg_match('/^\$(2y|2a|argon2i|argon2id)\$/', $value)) {
            $this->attributes['password'] = $value;
            return;
        }

        $this->attributes['password'] = \Illuminate\Support\Facades\Hash::make($value);
    }

    /**
     * secret 字段变更时自动维护时间戳
     *
     * - 如果传入的是脱敏占位符（••••••••），直接忽略
     * - 只有值真正发生变化时才更新 secret_change_at
     */
    public function setSecretAttribute($value): void
    {
        if ($value === '••••••••') {
            return;
        }

        $original = $this->getOriginal('secret');

        if ($value !== null && $value !== '' && $value !== $original) {
            $this->attributes['secret'] = $value;
            $this->attributes['secret_change_at'] = now();
        } else {
            $this->attributes['secret'] = $value;
        }
    }

    // ==================== 模型事件 ====================

    /**
     * Laravel 13 推荐在 booted() 中注册模型事件
     */
    protected static function booted(): void
    {
        static::saving(function (self $device) {
            $input = null;
            if (function_exists('request') && app()->has('request')) {
                $input = request()->input('secret');
            }

            // 新增：secret 为空，自动生成
            if (! $device->exists && empty($device->secret)) {
                $device->secret = self::generateSecret();
            }

            // 编辑：传了非占位的新值才覆盖
            if ($device->exists && $input
                && $input !== '••••••••'
                && $input !== $device->getOriginal('secret')
            ) {
                $device->secret = $input;
            }
        });

        static::saved(function (self $device) {
            try {
                app(EdgeAuthorizeService::class)->syncRedisOnSave($device);
            } catch (\Throwable $e) {
                Log::channel('edge')->error('EdgeAuthorize boot: syncRedisOnSave failed', [
                    'error'  => $e->getMessage(),
                    'device' => $device->only(['id', 'client_id', 'username', 'service']),
                ]);
            }
        });

        static::deleted(function (self $device) {
            try {
                app(EdgeAuthorizeService::class)->removeRedisOnDelete($device);
            } catch (\Throwable $e) {
                Log::channel('edge')->error('EdgeAuthorize boot: removeRedisOnDelete failed', [
                    'error'  => $e->getMessage(),
                    'device' => $device->only(['id', 'client_id', 'username', 'service']),
                ]);
            }
        });
    }

    // ==================== 工具方法 ====================

    /**
     * 生成密码学安全的随机密钥
     */
    public static function generateSecret(): string
    {
        return base64_encode(random_bytes(32));
    }
}
