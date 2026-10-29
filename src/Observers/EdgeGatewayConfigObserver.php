<?php
declare(strict_types=1);
namespace DagaSmart\Edge\Observers;

use DagaSmart\Edge\Models\EdgeGatewayConfig;
use Illuminate\Support\Facades\Log;
use Illuminate\Support\Facades\Request;

class EdgeGatewayConfigObserver
{
    /**
     * 更新前快照。
     * 只有值或启用状态真正变化时才记录，避免无意义的快照。
     */
    public function updating(EdgeGatewayConfig $config): void
    {
        if (!$config->isDirty('value') && !$config->isDirty('is_active')) {
            return;
        }

        // 此时 saving 事件尚未触发加密，getAttribute('value') 拿到的是待写入明文
        // getOriginal('value') 是数据库中已加密的旧值
        $newValue = $config->isDirty('value')
            ? $config->getAttribute('value')
            : $config->getOriginal('value');

        $action = $config->isDirty('is_active') && !$config->isDirty('value')
            ? 'toggle'
            : 'update';

        EdgeGatewayConfig::snapshotBeforeChange(
            $config,
            ['value' => $newValue],
            $action,
            Request::input('reason') ?? '后台配置修改'
        );
    }

    /**
     * 删除前快照。
     * 系统配置禁止物理删除，转为禁用。
     */
    public function deleting(EdgeGatewayConfig $config): void
    {
        if ($config->is_system) {
            // 阻止物理删除，改为停用，状态变更会触发 updating 快照
            $config->is_active = false;
            $config->saveQuietly();

            Log::channel('edge_auth')->info('系统配置禁止删除，已自动停用', [
                'key' => $config->key,
                'gateway_id' => $config->gateway_id,
            ]);

            return;
        }

        EdgeGatewayConfig::snapshotBeforeChange(
            $config,
            ['value' => ''],
            'delete',
            '物理删除配置项'
        );
    }

    /**
     * 恢复（软恢复）快照。
     */
    public function restored(EdgeGatewayConfig $config): void
    {
        EdgeGatewayConfig::snapshotBeforeChange(
            $config,
            ['value' => $config->value],
            'update',
            '恢复配置项'
        );
    }
}
