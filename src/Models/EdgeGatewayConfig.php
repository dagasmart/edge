<?php
declare(strict_types=1);
namespace DagaSmart\Edge\Models;

use DagaSmart\Edge\Traits\EncryptSensitiveConfigTrait;
use DagaSmart\Edge\Traits\SnapshotsConfigTrait;
use Illuminate\Database\Eloquent\Builder;
use Illuminate\Database\Eloquent\Relations\BelongsTo;
use Illuminate\Database\Eloquent\Relations\HasMany;
use Illuminate\Validation\ValidationException;
use Illuminate\Validation\Validator;

/**
 *网关配置模型
 */
class EdgeGatewayConfig extends Model
{
    use EncryptSensitiveConfigTrait;
    use SnapshotsConfigTrait;

    protected $table = 'mcs_edge_gateway_config';

    public $timestamps = true;

    /**
     * ✅ 白名单字段校验（核心）
     * 与迁移 080004 的 Schema 完全对齐
     */
    protected $fillable = [
        'gateway_id',   // 网关实例ID，NULL=全局默认
        'group',        // 配置分组
        'key',          // 配置键
        'value',        // 配置值（敏感字段经 Trait 自动加密）
        'type',         // int/string/bool
        'description',  // 描述
        'is_system',    // 是否系统内置
        'is_active',    // 是否启用
    ];

    protected $casts = [
        'gateway_id' => 'integer',
        'is_system'  => 'boolean',
        'is_active'  => 'boolean',
    ];

    // ---------- 类型白名单（与迁移 seedDefaults() 对齐） ----------
    public const string TYPE_INT    = 'int';
    public const string TYPE_STRING = 'string';
    public const string TYPE_BOOL   = 'bool';

    public static function allowedTypes(): array
    {
        return [self::TYPE_INT, self::TYPE_STRING, self::TYPE_BOOL];
    }

    // ---------- 分组白名单（与迁移 defaults() 对齐） ----------
    public static function allowedGroups(): array
    {
        return [
            'network', 'redis', 'concurrency', 'cache',
            'ratelimit', 'security', 'route', 'debug',
        ];
    }

    // ---------- 关系 ----------

    /**
     * 所属网关实例（gateway_id=NULL 时为全局默认，返回 null）
     */
    public function gateway(): BelongsTo
    {
        return $this->belongsTo(EdgeGateway::class, 'gateway_id');
    }

    /**
     * 变更快照（按时间倒序）
     */
    public function snapshots(): HasMany
    {
        return $this->hasMany(
            EdgeGatewayConfigSnapshot::class,
            'config_id'
        )->orderByDesc('snapshot_at');
    }

    // ---------- 作用域 ----------

    /**
     * 全局默认配置（gateway_id IS NULL）
     */
    public function scopeGlobal(Builder $query): Builder
    {
        return $query->whereNull('gateway_id');
    }

    /**
     * 指定网关的配置
     */
    public function scopeForGateway(Builder $query, ?int $gatewayId): Builder
    {
        if ($gatewayId === null) {
            return $query->whereNull('gateway_id');
        }
        return $query->where(function ($q) use ($gatewayId) {
            $q->where(['gateway_id' => $gatewayId])
                ->orWhereNull('gateway_id');
        });
    }

    /**
     * 仅启用的
     */
    public function scopeActive(Builder $query): Builder
    {
        return $query->where(['is_active' => true]);
    }

    // ---------- 校验逻辑（核心） ----------

    /**
     * ✅ 模型级校验：在 fill() 之后、save() 之前调用
     * 后台 Controller / Service 中 $model->fill($input)->validate()->save()
     */
    public function validate(): static
    {
        $data = $this->getAttributes();

        $validator = Validator::make($data, [
            'gateway_id'  => ['nullable', 'integer', 'exists:mcs_edge_gateway,id'],
            'group'       => ['required', 'string', 'max:32', 'in:' . implode(',', self::allowedGroups())],
            'key'         => ['required', 'string', 'max:64', 'regex:/^[A-Z][A-Z0-9_]*$/'],
            'value'       => ['required', 'string', 'max:500'],
            'type'        => ['required', 'string', 'in:' . implode(',', self::allowedTypes())],
            'description' => ['nullable', 'string', 'max:255'],
            'is_system'   => ['boolean'],
            'is_active'   => ['boolean'],
        ], [
            'key.regex' => '配置键必须以大写字母开头，仅包含大写字母、数字和下划线（如 MAX_CONCURRENCY）。',
            'group.in'  => '分组必须是以下之一：' . implode(', ', self::allowedGroups()),
            'type.in'   => '类型必须是以下之一：' . implode(', ', self::allowedTypes()),
        ]);

        if ($validator->fails()) {
            throw new ValidationException($validator);
        }

        // ---------- 业务规则校验 ----------

        // 1. 唯一性：同一网关下 key 不重复（NULL gateway_id 视为同一组）
        $duplicate = static::where(['key' => $this->key])
            ->where('gateway_id', $this->gateway_id)
            ->when($this->id, fn ($q) => $q->where('id', '!=', $this->id))
            ->exists();

        if ($duplicate) {
            throw ValidationException::withMessages([
                'key' => "配置键 [{$this->key}] 在该网关下已存在（gateway_id=" . ($this->gateway_id ?? 'NULL') . "）。",
            ]);
        }

        // 2. 系统配置保护：is_system 不允许从 true 改为 false
        if ($this->exists && $this->isDirty('is_system') && $this->getOriginal('is_system') === true) {
            throw ValidationException::withMessages([
                'is_system' => '系统内置配置不可取消系统标记。',
            ]);
        }

        // 3. 类型与值的兼容性校验
        $this->validateValueType();

        return $this;
    }

    /**
     * 校验 value 是否符合声明的 type
     */
    protected function validateValueType(): void
    {
        $value = $this->value;

        // 加密字段跳过类型校验（解密后才是真实值）
        if (in_array($this->key, self::$encryptedKeys, true)) {
            return;
        }

        $valid = match ($this->type) {
            self::TYPE_INT    => is_numeric($value) && (string)(int)$value === (string)$value,
            self::TYPE_BOOL   => in_array(strtolower($value), ['true', 'false', '0', '1', 'on', 'off', 'yes', 'no'], true),
            self::TYPE_STRING => is_string($value) && mb_strlen($value) <= 500,
            default           => false,
        };

        if (! $valid) {
            throw ValidationException::withMessages([
                'value' => "配置值 [{$value}] 不符合声明的类型 [{$this->type}]。",
            ]);
        }
    }

    // ---------- 便捷方法 ----------

    /**
     * 获取强转后的运行时值（供 bootstrap / 业务代码使用）
     */
    public function getTypedValue()
    {
        $value = $this->value; // 访问器自动解密

        return match ($this->type) {
            self::TYPE_INT    => (int) $value,
            self::TYPE_BOOL   => in_array(strtolower($value), ['true', '1', 'on', 'yes'], true),
            default           => (string) $value,
        };
    }

    /**
     * 判断是否为全局默认配置
     */
    public function isGlobal(): bool
    {
        return $this->gateway_id === null;
    }

    /**
     * 后台列表脱敏展示
     */
    public function getDisplayValueAttribute(): string
    {
        if (in_array($this->key, self::$encryptedKeys, true)) {
            return '••••••••';
        }
        return $this->value ?? '';
    }
}
