<?php
declare(strict_types=1);
namespace DagaSmart\Edge\Traits;

use Illuminate\Support\Facades\Crypt;

trait EncryptSensitiveConfigTrait
{
    /**
     * 需要加密存储的配置键。
     * 扩展时只需在此数组追加 key 即可。
     */
    protected static array $encryptedKeys = [
        'REDIS_PASSWORD',
    ];

    public static function bootEncryptsSensitiveConfig(): void
    {
        // 写入数据库前自动加密（仅首次，避免重复加密）
        static::saving(function ($model) {
            if (!in_array($model->key, static::$encryptedKeys, true)) {
                return;
            }

            if ($model->isClean('value') || empty($model->value)) {
                return;
            }

            if (!static::isEncrypted($model->value)) {
                $model->value = Crypt::encryptString($model->value);
            }
        });
    }

    /**
     * 读取时自动解密。
     * 业务代码（含 bootstrap loader）拿到的始终是明文。
     */
    public function getValueAttribute($value): ?string
    {
        if (!in_array($this->key, static::$encryptedKeys, true)) {
            return $value;
        }

        if ($value === null || $value === '') {
            return $value;
        }

        if (!static::isEncrypted($value)) {
            return $value;
        }

        try {
            return Crypt::decryptString($value);
        } catch (\Throwable $e) {
            // 解密失败（如 APP_KEY 变更）返回密文，避免业务侧拿到异常
            return $value;
        }
    }

    /**
     * 判断是否为 Laravel Crypt 加密串。
     * Crypt 输出为 base64 编码的 JSON 结构。
     */
    protected static function isEncrypted($value): bool
    {
        if (!is_string($value)) {
            return false;
        }

        return str_starts_with($value, 'eyJ');
    }

    /**
     * 后台列表/表单展示用：脱敏。
     * 永远不要把明文密码送到前端。
     */
    public function getMaskedValueAttribute(): string
    {
        if (!in_array($this->key, static::$encryptedKeys, true)) {
            return $this->value ?? '';
        }

        if (empty($this->value)) {
            return '';
        }

        return '••••••••';
    }

    /**
     * 对外暴露加密键列表（供 ConfigService / 表单校验使用）。
     */
    public static function getEncryptedKeys(): array
    {
        return static::$encryptedKeys;
    }
}
