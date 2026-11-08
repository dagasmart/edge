<?php

declare(strict_types=1);

namespace DagaSmart\Edge\Models;

/**
 * 消费位点模型（对应 mcs_edge_stream_offset，分片版）
 *
 * 分片版改动：
 *   - 新增 consumer 字段，支持多消费者并行位点追踪
 *   - 主键约束升级为 (stream, group_name, consumer)
 */
class EdgeStreamOffset extends Model
{
    protected $table = 'mcs_edge_stream_offset';

    public $timestamps = false;

    protected $fillable = [
        'stream',
        'group_name',
        'consumer',
        'last_id',
        'last_lag',
        'updated_at',
    ];

    protected $casts = [
        'last_lag' => 'integer',
        'updated_at' => 'datetime',
    ];
}
