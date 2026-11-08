<?php

declare(strict_types=1);

use Illuminate\Database\Migrations\Migration;
use Illuminate\Database\Schema\Blueprint;
use Illuminate\Support\Facades\DB;
use Illuminate\Support\Facades\Schema;

return new class extends Migration
{
    private string $name = 'mcs_edge_stream';

    public function up(): void
    {
        if (! Schema::hasTable($this->name)) {
            Schema::create($this->name, function (Blueprint $table) {
                $table->comment('边缘网关设备数据流表');
                $table->id();
                $table->unsignedTinyInteger('shard_no')->default(0)->comment('redis分片编号');
                $table->string('source_stream', 128)->comment('来源stream key');
                $table->string('stream_id', 64)->comment('流id');
                $table->string('consumer_group', 64)->default('')->comment('消费组');
                $table->string('client_id', 64)->comment('MQTT client_id');
                $table->string('topic', 100);
                $table->json('payload');
                $table->unsignedBigInteger('received_ms')->comment('毫秒时间戳');
                $table->dateTime('received_at');
                $table->timestamp('created_at')->useCurrent();

                // 幂等约束：同一设备同一毫秒同一topic不重复
                $table->unique(['client_id', 'received_ms', 'topic'])->nullsNotDistinct();
                // stream_id 全局唯一（跨分片可能重复，但同一stream内唯一）
                $table->unique(['stream_id']);
                // 分片查询索引
                $table->index(['shard_no', 'source_stream']);
                $table->index(['client_id', 'received_ms']);
            });
        }

        $driver = config("database.connections.{$this->connection}.driver");

        if ($driver === 'mysql') {
            DB::statement("ALTER TABLE {$this->name} AUTO_INCREMENT=1000000");
        }

        if ($driver === 'pgsql') {
            $seq = "{$this->name}_id_seq";
            // 检查序列是否存在
            $exists = DB::selectOne("
                SELECT 1 FROM pg_class WHERE relname = '{$seq}'
            ");
            if ($exists) {
                DB::statement("ALTER SEQUENCE {$seq} RESTART WITH 1000000");
            }
        }
    }

    public function down(): void
    {
        Schema::dropIfExists($this->name);
    }
};
