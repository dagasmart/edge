<?php
declare(strict_types=1);
namespace DagaSmart\Edge\Models;

/**
 *网关模型
 */
class EdgeGateway extends Model
{
    protected $table = 'mcs_edge_gateway';

    protected $fillable = [
        'gateway',
        'code',
        'ip',
        'port',
        'ca',
        'remark',
    ];

    protected $casts = [
        'port' => 'integer',
        'created_at' => 'datetime',
        'updated_at' => 'datetime',
    ];

    public $timestamps = true;


}
