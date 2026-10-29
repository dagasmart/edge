<?php
/**
 * 边缘网关配置表（含网关差异化支持）
 *
 * 说明：gateway_id 为 NULL 表示全局默认，有值则该配置仅对指定网关实例生效。
 */

use Illuminate\Database\Migrations\Migration;
use Illuminate\Database\Schema\Blueprint;
use Illuminate\Support\Facades\DB;
use Illuminate\Support\Facades\Schema;

return new class extends Migration
{
    protected $connection = 'master';

    private string $name = 'mcs_edge_gateway_config';

    public function up(): void
    {
        if (! Schema::hasTable($this->name)) {
            Schema::create($this->name, function (Blueprint $table) {
                $table->comment('边缘网关配置表');
                $table->id();

                $table->unsignedBigInteger('gateway_id')
                    ->nullable()
                    ->default(null)
                    ->comment('网关实例ID，NULL全局默认');

                $table->string('group', 32)
                    ->comment('配置分组：network/redis/concurrency/cache/ratelimit/security/route/debug');

                $table->string('key', 64)
                    ->comment('配置键，与原常量名一致');

                $table->text('value')
                    ->comment('配置值（敏感字段加密存储）');

                $table->string('type', 16)
                    ->default('string')
                    ->comment('类型：int/string/bool，加载时按此强转');

                $table->string('description', 255)
                    ->nullable()
                    ->comment('描述说明');

                $table->boolean('is_system')
                    ->default(true)
                    ->comment('是否系统内置（内置项不可删除，仅可改值）');

                $table->boolean('is_active')
                    ->default(true)
                    ->comment('是否启用，关闭后自动回退到默认值');

                $table->timestamps();

                $table->index('gateway_id');
                $table->index('group');
                $table->index('key');
                $table->unique(['gateway_id', 'key'], 'uk_gateway_key')->nullsNotDistinct();

                $table->foreign('gateway_id')
                    ->references('id')
                    ->on('mcs_edge_gateway')
                    ->nullOnDelete();
            });
        }

        $driver = config("database.connections.{$this->connection}.driver");

        if ($driver === 'mysql') {
            DB::statement("ALTER TABLE {$this->name} AUTO_INCREMENT=1000000");
        }

        if ($driver === 'pgsql') {
            $seq = "{$this->name}_id_seq";
            DB::statement("SELECT 1 FROM pg_class WHERE relname = '{$seq}'");
            DB::statement("ALTER SEQUENCE {$seq} RESTART WITH 1000000");
        }

        $this->seedDefaults();
    }

    protected function defaults(): array
    {
        return [
            'AUTH_PORT'                 => ['network',     '9503',  'int',    '监听端口'],
            'BACKLOG'                   => ['network',     '4096',  'int',    '连接积压队列长度'],
            'REDIS_HOST'                => ['redis',       '127.0.0.1', 'string', 'Redis 主机'],
            'REDIS_PORT'                => ['redis',       '16379', 'int',    'Redis 端口'],
            'REDIS_PASSWORD'            => ['redis',       'uApDVQD7', 'string', 'Redis 密码（加密存储）'],
            'REDIS_DB'                  => ['redis',       '2',     'int',    'Redis 库号'],
            'REDIS_POOL_SIZE'           => ['redis',       '512',   'int',    'Redis 连接池大小'],
            'MAX_CONCURRENCY'           => ['concurrency', '20000', 'int',    '全局最大并发'],
            'MAX_CONCURRENCY_PER_DEVICE'=> ['concurrency', '5',     'int',    '单设备最大并发'],
            'CACHE_TTL'                 => ['cache',      '300',   'int',    '缓存 TTL（秒）'],
            'LOCAL_CACHE_TTL'           => ['cache',      '60',    'int',    '本地缓存 TTL（秒）'],
            'RATE_MAX'                  => ['ratelimit',  '20',    'int',    '限流窗口内最大请求数'],
            'RATE_TTL'                  => ['ratelimit',  '60',    'int',    '限流窗口大小（秒）'],
            'TIME_DRIFT'                => ['security',   '300',   'int',    '签名时间漂移容差（秒）'],
            'AUTH_HTTP_PREFIX'          => ['route',      '/admin-api/extension/edge', 'string', '认证路由前缀'],
            'DEBUG'                     => ['debug',      'false', 'bool',   '调试模式开关'],
        ];
    }

    protected function seedDefaults(): void
    {
        foreach ($this->defaults() as $key => [$group, $value, $type, $description]) {
            $exists = DB::connection($this->connection)
                ->table($this->name)
                ->where('gateway_id', null)
                ->where('key', $key)
                ->exists();

            if ($exists) {
                continue;
            }

            $storeValue = $value;
            if ($key === 'REDIS_PASSWORD' && $storeValue !== '') {
                try {
                    if (class_exists(\Illuminate\Support\Facades\Crypt::class)
                        && ! str_starts_with($storeValue, 'eyJ')
                    ) {
                        $storeValue = \Illuminate\Support\Facades\Crypt::encryptString($storeValue);
                    }
                } catch (\Throwable $e) {
                    fwrite(STDERR, "[migration] encrypt failed for {$key}: {$e->getMessage()}\n");
                }
            }

            DB::connection($this->connection)->table($this->name)->insert([
                'gateway_id'  => null,
                'group'       => $group,
                'key'         => $key,
                'value'       => $storeValue,
                'type'        => $type,
                'description' => $description,
                'is_system'   => true,
                'is_active'   => true,
                'created_at'  => now(),
                'updated_at'  => now(),
            ]);
        }
    }

    /**
     * 回滚：拆外键 → 无条件 drop，不保留任何数据。
     */
    public function down(): void
    {
        if (! Schema::hasTable($this->name)) {
            return;
        }

        // ✅ 先拆外键（不管 gateway_id 是不是全 NULL，约束对象必须消失）
        Schema::table($this->name, function (Blueprint $table) {
            $table->dropForeign(['gateway_id']);
        });

        // ✅ 无条件 drop，零残留
        Schema::dropIfExists($this->name);
    }
};
