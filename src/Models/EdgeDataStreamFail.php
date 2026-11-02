<?php
declare(strict_types=1);
namespace DagaSmart\Edge\Models;

/**
 *失败数据流模型
 */
class EdgeDataStreamFail extends Model
{
    protected $table = 'mcs_edge_data_stream_fail';

    public $timestamps = false;

    protected $fillable = [
        'client_id',
        'topic',
        'payload',
        'received_ms',
        'received_at',
        'created_at',
    ];

    protected $casts = [
        'payload' => 'array',
        'received_ms' => 'integer',
        'received_at' => 'datetime',
        'created_at' => 'datetime',
    ];
}
