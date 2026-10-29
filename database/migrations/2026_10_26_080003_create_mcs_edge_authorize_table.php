<?php

use Illuminate\Database\Migrations\Migration;
use Illuminate\Database\Schema\Blueprint;
use Illuminate\Support\Facades\DB;
use Illuminate\Support\Facades\Schema;

return new class extends Migration
{
    protected $connection = 'master';

    private string $name = 'mcs_edge_authorize';

    public function up(): void
    {
        if (! Schema::hasTable($this->name)) {
            Schema::create($this->name, function (Blueprint $table) {
                $table->comment('边缘网关认证表');
                $table->id();

                $table->string('username', 64)
                    ->unique()
                    ->comment('用户名');

                $table->string('password', 100)
                    ->comment('密码 password_hash');

                $table->string('client_id', 100)
                    ->comment('设备 cli|sn');

                $table->boolean('active')
                    ->default(false)
                    ->comment('是否激活 true/false');

                $table->boolean('is_super')
                    ->default(false)
                    ->comment('超级用户 true/false');

                $table->string('service', 32)
                    ->default('api')
                    ->comment('网关类型 nano/mqtt/emqx/email/sms/api');

                $table->string('secret', 200)
                    ->comment('HMAC 密钥(明文存储，用于签名)');

                $table->dateTime('secret_change_at')
                    ->comment('密钥变更时间');

                $table->string('remark', 100)
                    ->nullable()
                    ->comment('描述');

                $table->timestamps();

                $table->index('client_id');
                $table->index('username');
                $table->index('active');
                $table->index('service');
                $table->index(['client_id', 'username']);
                $table->index(['username', 'active', 'service']);
                $table->unique(['username', 'service'])->nullsNotDistinct();
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
    }

    public function down(): void
    {
        if (Schema::hasTable($this->name)) {
            if (! DB::table($this->name)->exists()) {
                Schema::dropIfExists($this->name);
            }
        }
    }
};
