<?php

declare(strict_types=1);

use Illuminate\Database\Migrations\Migration;
use Illuminate\Database\Schema\Blueprint;
use Illuminate\Support\Facades\DB;
use Illuminate\Support\Facades\Schema;

return new class extends Migration
{
    protected $connection = 'master';

    private string $name = 'mcs_edge_stream_fail';

    public function up(): void
    {
        if (! Schema::hasTable($this->name)) {
            Schema::create($this->name, function (Blueprint $table) {
                $table->comment('边缘网关数据流(失败)表');
                $table->id();
                $table->unsignedTinyInteger('shard_no')->default(0)->comment('redis分片编号');
                $table->string('stream_id', 64)->comment('redis流id');
                $table->string('source_stream', 128)->comment('原始流key');
                $table->string('client_id', 64)->comment('设备sn/client（重试回填用）');
                $table->string('topic', 100)->nullable()->comment('topic（重试回填用）');
                $table->json('payload')->comment('消息体快照');
                $table->unsignedBigInteger('received_ms')->comment('毫秒时间戳');
                $table->dateTime('received_at')->nullable()->comment('边缘侧时间（重试回填用）');
                $table->text('last_error')->comment('最后一次错误');

                $table->timestamp('created_at')->useCurrent();

                $table->string('group_name', 64)->comment('消费组');
                $table->string('consumer', 64)->comment('消费者');
                $table->unsignedSmallInteger('delivered')->default(0)->comment('已投递次数');
                $table->unsignedTinyInteger('retry_num')->default(0)->comment('已重试次数');
                $table->timestamp('next_retry_at')->nullable()->comment('下次重试时间');
                $table->boolean('is_resolved')->default(false)->comment('是否已人工处理');

                $table->index(['is_resolved', 'next_retry_at']);
                $table->index(['is_resolved', 'created_at']);
                $table->unique(['stream_id']);
                $table->unique(['client_id', 'received_ms', 'topic'])->nullsNotDistinct();
            });
        }

        $driver = config("database.connections.{$this->connection}.driver");

        if ($driver === 'mysql') {
            DB::statement("ALTER TABLE {$this->name} AUTO_INCREMENT=1000000");
        }

        if ($driver === 'pgsql') {
            $seq = "{$this->name}_id_seq";
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
