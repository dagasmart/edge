<?php

declare(strict_types=1);

use Illuminate\Database\Migrations\Migration;
use Illuminate\Database\Schema\Blueprint;
use Illuminate\Support\Facades\Schema;

return new class extends Migration
{
    private string $name = 'mcs_edge_stream_offset';

    public function up(): void
    {
        if (! Schema::hasTable($this->name)) {
            Schema::create($this->name, function (Blueprint $table) {
                $table->comment('边缘网关数据流消费位点表');
                $table->id();
                $table->string('stream', 128)->comment('stream key');
                $table->string('group_name', 64)->comment('消费组名');
                $table->string('consumer', 64)->comment('消费者名');
                $table->string('last_id', 64)->default('0-0')->comment('最后处理的消息ID');
                $table->unsignedBigInteger('processed_count')->default(0)->comment('已处理条数');
                $table->unsignedBigInteger('error_count')->default(0)->comment('错误条数');
                $table->timestamp('last_processed_at')->nullable()->comment('最后处理时间');
                $table->timestamp('created_at')->useCurrent();
                $table->timestamp('updated_at')->useCurrentOnUpdate();

                $table->unique(['stream', 'group_name', 'consumer']);
                $table->index(['stream', 'group_name']);
            });
        }
    }

    public function down(): void
    {
        Schema::dropIfExists($this->name);
    }
};
