<?php

namespace DagaSmart\Edge\Models;

use Carbon\Carbon;
use DagaSmart\Edge\Services\EdgeAuthorizeService;
use Illuminate\Support\Facades\Log;

/**
 * 边缘网关认证模型
 *
 * @property int    $id
 * @property string $username
 * @property string $password
 * @property string $client_id
 * @property bool   $active
 * @property bool   $is_super
 * @property string $service
 * @property string $secret
 * @property Carbon|null $secret_change_at
 * @property string|null $remark
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

    /**
     * secret 字段变更时自动维护时间戳
     */
    public function setSecretAttribute($value): void
    {
        $original = $this->attributes['secret'] ?? null;

        if ($value !== null && $value !== '' && $value !== $original) {
            $this->attributes['secret'] = $value;
            $this->attributes['secret_change_at'] = now();
        } else {
            $this->attributes['secret'] = $value;
        }
    }

    protected static function booted(): void
    {
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
}
