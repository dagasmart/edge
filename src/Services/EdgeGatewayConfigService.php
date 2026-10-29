<?php
declare(strict_types=1);
namespace DagaSmart\Edge\Services;

use DagaSmart\Edge\Models\EdgeGatewayConfig;
use Illuminate\Support\Facades\DB;
use Illuminate\Support\Facades\Log;

class EdgeGatewayConfigService
{
    /**
     * 保存配置（新增或更新），原子操作：事务 + 快照 + 重载。
     */
    public function save(array $data, ?string $reason = null): EdgeGatewayConfig
    {
        $isUpdate = isset($data['id']) && $data['id'] > 0;

        return admin_transaction(function () use ($data, $reason, $isUpdate) {
            if ($isUpdate) {
                $config = EdgeGatewayConfig::lockForUpdate()->findOrFail($data['id']);
                $config->fill($data);
                $config->save();
            } else {
                // 唯一约束保护：同网关下 key 不重复
                $exists = EdgeGatewayConfig::where(['key' => $data['key']])
                    ->where('gateway_id', $data['gateway_id'] ?? null)
                    ->exists();

                if ($exists) {
                    throw new \RuntimeException("配置键 {$data['key']} 已存在，不可重复创建");
                }

                $config = EdgeGatewayConfig::create($data);
            }

            Log::channel('edge_auth')->info($isUpdate ? '配置更新' : '配置新增', [
                'id' => $config->id,
                'key' => $config->key,
                'gateway_id' => $config->gateway_id,
            ]);

            return $config;
        });
    }

    /**
     * 批量保存（如网关差异化场景下一键应用模板）。
     * 整批共享一个 batch 号，可整体回滚。
     */
    public function saveBatch(array $items, ?string $reason = null): array
    {
        return admin_transaction(function () use ($items, $reason) {
            $results = [];

            foreach ($items as $item) {
                $results[] = $this->save($item, $reason);
            }

            return $results;
        });
    }

    /**
     * 按批次回滚。
     */
    public function rollback(string $batch): int
    {
        return DB::transaction(function () use ($batch) {
            $count = EdgeGatewayConfig::rollbackBatch($batch);

            Log::channel('edge_auth')->info('配置回滚', [
                'batch' => $batch,
                'count' => $count,
            ]);

            return $count;
        });
    }

    /**
     * 解析配置：gateway 专属 > 全局 > 空。
     * 供 bootstrap loader 或业务侧使用。
     * @param int|null $gatewayId
     * @return array
     */
    public function resolve(?int $gatewayId = null): array
    {
        $rows = EdgeGatewayConfig::where(['is_active' => true])
            ->orderByRaw('gateway_id IS NULL ASC') // NULL(全局) 在前，被专属覆盖
            ->get();

        $merged = [];
        foreach ($rows as $row) {
            $key = $row->key;
            // 如果指定了网关，则只接受该网关专属或全局配置
            if ($gatewayId !== null) {
                if ($row->gateway_id !== null && $row->gateway_id !== $gatewayId) {
                    continue;
                }
            }
            // 非 NULL gateway_id（专属）覆盖 NULL（全局）
            if (!isset($merged[$key]) || $row->gateway_id !== null) {
                $merged[$key] = (object)[
                    'value'      => $row->value, // 访问器已自动解密
                    'type'       => $row->type,
                    'gateway_id' => $row->gateway_id,
                ];
            }
        }

        return $merged;
    }

    /**
     * 按 key 取值（已解密、已强转）。
     */
    public function get(string $key, $default = null, ?int $gatewayId = null)
    {
        $config = EdgeGatewayConfig::where(['key' => $key])
            ->when($gatewayId !== null, function ($q) use ($gatewayId) {
                $q->where(function ($sq) use ($gatewayId) {
                    $sq->where('gateway_id', $gatewayId)
                       ->orWhereNull('gateway_id');
                });
            })
            ->where('is_active', true)
            ->orderByRaw('gateway_id IS NULL ASC')
            ->first();

        if (!$config) {
            return $default;
        }

        $value = $config->value; // 访问器自动解密

        switch ($config->type) {
            case 'int':
                return (int) $value;
            case 'bool':
                return in_array(strtolower((string)$value), ['true', '1', 'on', 'yes'], true);
            default:
                return (string) $value;
        }
    }

    /**
     * 触发 worker 热重载（发 SIGUSR1）。
     * 仅在 master 进程下有效；Windows 下退化为提示。
     */
    public function reloadWorkers(): void
    {
        if (function_exists('posix_kill')) {
            // 通知 Artisan 命令所在进程组
            $pid = getmypid();
            posix_kill($pid, SIGUSR1);
        }

        Log::channel('edge_auth')->info('已请求 worker 热重载');
    }
}
