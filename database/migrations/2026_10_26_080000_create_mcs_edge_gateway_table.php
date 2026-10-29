<?php

use Illuminate\Database\Migrations\Migration;
use Illuminate\Database\Schema\Blueprint;
use Illuminate\Support\Facades\DB;
use Illuminate\Support\Facades\Schema;

return new class extends Migration
{
    protected $connection = 'master';

    private string $name = 'mcs_edge_gateway';

    public function up(): void
    {
        if (! Schema::hasTable($this->name)) {
            Schema::create($this->name, function (Blueprint $table) {
                $table->comment('边缘网关表');
                $table->id();

                $table->string('gateway', 64)
                    ->unique()
                    ->comment('服务名称');

                // ✅ 修正：tinyText 不支持长度，改为 string
                $table->string('code', 64)
                    ->comment('服务标识');

                $table->ipAddress('ip')
                    ->comment('服务ip');

                $table->tinyInteger('port')
                    ->default(0)
                    ->comment('服务端口');

                $table->string('ca', 255)
                    ->nullable()
                    ->comment('CA证书');

                $table->string('remark', 100)
                    ->nullable()
                    ->comment('描述');

                $table->timestamp('created_at')->useCurrent();
                $table->timestamp('updated_at')->useCurrent();

                $table->index('gateway');
                $table->index('code');
                $table->index('ip');
                $table->index(['code', 'ip']);
                $table->unique(['code', 'ip'])->nullsNotDistinct();
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

    /**
     * 回滚：直接 drop，不判断数据是否存在。
     * 前置条件：config 迁移的 down() 已先拆除外键约束。
     */
    public function down(): void
    {
        if (Schema::hasTable($this->name)) {
            Schema::dropIfExists($this->name);
        }
    }
};
