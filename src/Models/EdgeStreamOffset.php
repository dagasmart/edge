<?php
declare(strict_types=1);
namespace DagaSmart\Edge\Models;


/**
 * 消费位点模型（对应 mcs_edge_stream_offset）
 */
class EdgeStreamOffset extends Model
{
    protected $table = 'mcs_edge_stream_offset';

    public $timestamps = false;

    protected $fillable = [
        'stream',
        'group_name',
        'last_id',
        'last_lag',
        'updated_at',
    ];

    protected $casts = [
        'last_lag'   => 'integer',
        'updated_at' => 'datetime',
    ];
}
