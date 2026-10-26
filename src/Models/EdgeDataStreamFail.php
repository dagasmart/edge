<?php

namespace DagaSmart\Edge\Models;

/**
 *数据流模型
 */
class EdgeDataStreamFail extends Model
{
    protected $table = 'mcs_edge_data_stream_fail';

    protected $fillable = [
        'client_id',
        'topic',
        'payload',
        'received_ms',
        'received_at',
    ];

    protected $casts = [
        'payload'     => 'array',          // ← 自动 json_encode / decode，Job 里直接塞数组
        'received_at' => 'datetime',       // ← 认 Carbon 对象
        'received_ms' => 'integer',
    ];

    public $timestamps = false;   // 表里有 received_at，不需要 created_at/updated_at

}
