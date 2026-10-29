<?php

use Illuminate\Database\Migrations\Migration;
use Illuminate\Database\Schema\Blueprint;
use Illuminate\Support\Facades\DB;
use Illuminate\Support\Facades\Schema;

return new class extends Migration
{
    protected $connection = 'master';

    private string $name = 'mcs_edge_data_stream';

    public function up(): void
    {
        if (! Schema::hasTable($this->name)) {
            Schema::create($this->name, function (Blueprint $table) {
                $table->comment('边缘网关设备数据流表');
                $table->id();
                $table->string('client_id', 64)->comment('MQTT client_id');
                $table->string('topic', 100);
                $table->json('payload');
                $table->unsignedBigInteger('received_ms')->comment('毫秒时间戳');
                $table->dateTime('received_at');
                $table->timestamp('created_at')->useCurrent();

                // 幂等核心：同设备 + 同毫秒 + 同 topic 只能有一条
                $table->unique(['client_id', 'received_ms', 'topic'])->nullsNotDistinct();
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
