<?php
declare(strict_types=1);
namespace DagaSmart\Edge\Models;

/**
 *数据流模型
 */
class EdgeDataStream extends Model
{
    protected $table = 'mcs_edge_data_stream';

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
