<?php
declare(strict_types=1);
namespace DagaSmart\Edge\Traits;

use DagaSmart\Edge\Models\EdgeGatewayConfig;
use DagaSmart\Edge\Models\EdgeGatewayConfigSnapshot;
use Illuminate\Support\Facades\Auth;
use Illuminate\Support\Str;

trait SnapshotsConfigTrait
{
    /**
     * 单条变更前快照。
     *
     * @param EdgeGatewayConfig $config  变更前的模型实例
     * @param  array  $new        ['value' => ..., 'batch' => ...] 新值信息
     * @param  string $action     create/update/delete/toggle/rollback
     * @param  string|null $reason  变更原因
     */
    protected static function snapshotBeforeChange(
        EdgeGatewayConfig $config,
        array             $new,
        string            $action,
        ?string           $reason = null
    ): void {
        $batch = $new['batch'] ?? (string) Str::uuid();

        // 快照中敏感字段存密文：value_before 取原始值（Model 访问器返回明文，
        // 这里用 getOriginal 拿数据库真实值，避免快照里出现明文密码）
        $valueBefore = $config->getOriginal('value');
        $valueAfter  = $new['value'] ?? $config->getOriginal('value');

        EdgeGatewayConfigSnapshot::query()->insert([
            'config_id'    => $config->id,
            'config_key'   => $config->key,
            'group'        => $config->group,
            'value_before' => $valueBefore,
            'value_after'  => $valueAfter,
            'type'         => $config->type,
            'action'       => $action,
            'operator_id'  => Auth::id(),
            'operator_type'=> Auth::check() ? 'admin' : 'system',
            'reason'       => $reason,
            'batch'        => $batch,
            'snapshot_at'  => now(),
        ]);
    }

    /**
     * 批量变更前快照，返回批次号。
     */
    protected static function snapshotBatchBeforeChange(
        iterable $configs,
        array $newValues,
        string $action,
        ?string $reason = null
    ): string {
        $batch = (string) Str::uuid();

        foreach ($configs as $config) {
            static::snapshotBeforeChange($config, [
                'value' => $newValues[$config->key]
                    ?? $config->getOriginal('value'),
                'batch' => $batch,
            ], $action, $reason);
        }

        return $batch;
    }

    /**
     * 按批次整体回滚。
     * 回滚本身也会产生一条 rollback 类型的快照，形成完整审计链。
     *
     * @return int 回滚的记录数
     */
    protected static function rollbackBatch(string $batch): int
    {
        $snaps = EdgeGatewayConfigSnapshot::query()
            ->where('batch', $batch)
            ->orderByDesc('id')
            ->get();

        $count = 0;

        foreach ($snaps as $s) {
            // value_before 是密文，直接写回数据库，保持加密存储一致
            EdgeGatewayConfig::query()
                ->where('id', $s->config_id)
                ->update([
                    'value'     => $s->value_before,
                    'updated_at'=> now(),
                ]);

            EdgeGatewayConfigSnapshot::query()->insert([
                'config_id'    => $s->config_id,
                'config_key'   => $s->config_key,
                'group'        => $s->group,
                'value_before' => $s->value_after,
                'value_after'  => $s->value_before,
                'type'         => $s->type,
                'action'       => 'rollback',
                'operator_id'  => Auth::id(),
                'operator_type'=> Auth::check() ? 'admin' : 'system',
                'reason'       => "rollback batch {$batch}",
                'batch'        => (string) Str::uuid(),
                'snapshot_at'  => now(),
            ]);

            $count++;
        }

        return $count;
    }
}
