<?php
declare(strict_types=1);
namespace DagaSmart\Edge\Models;

use Illuminate\Database\Eloquent\Relations\BelongsTo;

class EdgeGatewayConfigSnapshot extends Model
{

    protected $table = 'mcs_edge_gateway_config_snapshot';

    protected $fillable = [
        'config_id',
        'config_key',
        'group',
        'value_before',
        'value_after',
        'type',
        'action',
        'operator_id',
        'operator_type',
        'reason',
        'batch',
        'snapshot_at',
    ];

    protected $casts = [
        'config_id'  => 'integer',
        'operator_id'=> 'integer',
        'snapshot_at'=> 'datetime',
    ];

    public $timestamps = false;

    public function config(): BelongsTo
    {
        return $this->belongsTo(EdgeGatewayConfig::class, 'config_id');
    }

    public function getActionLabelAttribute(): string
    {
        $map = [
            'create'   => '新增',
            'update'   => '修改',
            'delete'   => '删除',
            'toggle'   => '启停',
            'rollback' => '回滚',
        ];

        return $map[$this->action] ?? $this->action;
    }
}
