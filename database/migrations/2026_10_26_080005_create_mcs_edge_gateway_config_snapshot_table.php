<?php
/**
 * 边缘网关配置变更快照表（审计追踪 + 按批次回滚）
 *
 * 关联删除规则：
 * - config_id 关联 mcs_edge_gateway_config.id
 * - 当 config 记录被删除时，snapshot 自动级联删除（cascadeOnDelete）
 * - 这保证了"配置不存在时快照也不存在"的完整性
 */

use Illuminate\Database\Migrations\Migration;
use Illuminate\Database\Schema\Blueprint;
use Illuminate\Support\Facades\DB;
use Illuminate\Support\Facades\Schema;

return new class extends Migration
{
    protected $connection = 'master';
    private string $name = 'mcs_edge_gateway_config_snapshot';

    public function up(): void
    {
        if (!Schema::hasTable($this->name)) {
            Schema::create($this->name, function (Blueprint $table) {
                $table->comment('边缘网关配置变更快照表');
                $table->id();

                $table->unsignedBigInteger('config_id')
                    ->comment('关联的 mcs_edge_gateway_config.id');

                $table->string('config_key', 64)->comment('配置键冗余存储');
                $table->string('group', 32)->nullable()->comment('配置分组冗余存储');
                $table->text('value_before')->nullable()->comment('变更前值（敏感字段存密文）');
                $table->text('value_after')->nullable()->comment('变更后值（敏感字段存密文）');
                $table->string('type', 16)->default('string')->comment('类型，回滚时按此强转');
                $table->string('action', 16)->comment('操作类型：create/update/delete/toggle/rollback');
                $table->unsignedBigInteger('operator_id')->nullable()->comment('操作人ID');
                $table->string('operator_type', 16)->default('system')->comment('操作人类型');
                $table->string('reason', 255)->nullable()->comment('变更原因');
                $table->string('batch', 64)->comment('批次号');
                $table->dateTime('snapshot_at')->comment('快照时间');

                $table->index('config_id');
                $table->index('config_key');
                $table->index('action');
                $table->index('batch');
                $table->index('snapshot_at');

                // ✅ 核心：级联删除，config 没了 snapshot 跟着没
                $table->foreign('config_id')
                    ->references('id')
                    ->on('mcs_edge_gateway_config')
                    ->cascadeOnDelete();
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

        $this->seedBaselineSnapshot();
    }

    public function down(): void
    {
        // ✅ 修正：先确认没有孤儿数据再 drop
        // cascadeOnDelete 意味着正常流程下 snapshot 会随 config 一起消失
        // 这里只需安全 drop 表本身
        if (Schema::hasTable($this->name)) {
            // 防御性检查：如果 config 表已不存在但 snapshot 表还在（手动操作场景）
            if (! Schema::hasTable('mcs_edge_gateway_config')) {
                // config 表都没了，snapshot 肯定是孤儿数据，直接清
                Schema::dropIfExists($this->name);
                return;
            }

            // config 表存在时，检查是否有引用 config 中已不存在的快照
            $orphanExists = DB::connection($this->connection)
                ->table($this->name)
                ->whereNotExists(function ($query) {
                    $query->select(DB::raw(1))
                        ->from('mcs_edge_gateway_config')
                        ->whereRaw('mcs_edge_gateway_config.id = mcs_edge_gateway_config_snapshot.config_id');
                })
                ->exists();

            if ($orphanExists) {
                // 清理孤儿记录后再 drop
                DB::connection($this->connection)
                    ->table($this->name)
                    ->whereNotExists(function ($query) {
                        $query->select(DB::raw(1))
                            ->from('mcs_edge_gateway_config')
                            ->whereRaw('mcs_edge_gateway_config.id = mcs_edge_gateway_config_snapshot.config_id');
                    })
                    ->delete();
            }

            Schema::dropIfExists($this->name);
        }
    }

    /**
     * 首次建表时生成 baseline 快照，作为回滚锚点。
     * 后续增量快照由 SnapshotsConfig Trait + Observer 在业务层写入。
     */
    private function seedBaselineSnapshot(): void
    {
        if (!Schema::hasTable('mcs_edge_gateway_config')) {
            return;
        }

        if (DB::table($this->name)->exists()) {
            return;
        }

        $rows = DB::table('mcs_edge_gateway_config')->get();

        if ($rows->isEmpty()) {
            return;
        }

        $now = now();

        $data = $rows->map(function ($row) use ($now) {
            return [
                'config_id'    => $row->id,
                'config_key'   => $row->key,
                'group'        => $row->group,
                'value_before' => null,
                'value_after'  => $row->value,
                'type'         => $row->type,
                'action'       => 'create',
                'operator_id'  => null,
                'operator_type'=> 'migration',
                'reason'       => 'baseline snapshot on table creation',
                'batch'        => 'baseline',
                'snapshot_at'  => $now,
            ];
        })->toArray();

        DB::table($this->name)->insert($data);
    }
};
