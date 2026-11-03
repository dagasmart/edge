<?php

use Illuminate\Database\Migrations\Migration;
use Illuminate\Database\Schema\Blueprint;
use Illuminate\Support\Facades\DB;
use Illuminate\Support\Facades\Schema;

return new class extends Migration
{
    protected $connection = 'master';

    private string $name = 'mcs_edge_stream_offset';

    public function up(): void
    {
        if (! Schema::hasTable($this->name)) {
            Schema::create($this->name, function (Blueprint $table) {
                $table->comment('边缘网关数据流位点表');
                $table->id();
                $table->string('stream', 128)->comment('stream key');
                $table->string('group_name', 64)->comment('消费组');
                $table->string('last_id', 64)->comment('最后成功处理的 stream_id');
                $table->unsignedInteger('last_lag')->default(0)->comment('消费堆积量');
                $table->timestamp('updated_at')->nullable();

                $table->primary(['stream', 'group_name']);
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
