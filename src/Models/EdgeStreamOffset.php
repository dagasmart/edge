<?php

declare(strict_types=1);

namespace DagaSmart\Edge\Models;

use Illuminate\Database\Eloquent\Model;

/**
 * 消费位点模型（对应 mcs_edge_stream_offset，分片版）
 *
 * 分片版改动：
 *   - 新增 consumer 字段，支持多消费者并行位点追踪
 *   - 唯一约束：(stream, group_name, consumer)
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

    /* ==================== 便捷方法 ==================== */

    /**
     * 获取指定 stream + group + consumer 的位点
     */
    public static function getOffset(string $stream, string $groupName, string $consumer): ?self
    {
        return self::query()
            ->where('stream', $stream)
            ->where('group_name', $groupName)
            ->where('consumer', $consumer)
            ->first();
    }

    /**
     * 更新或插入位点
     */
    public static function upsertOffset(
        string $stream,
        string $groupName,
        string $consumer,
        string $lastId,
        int $lastLag = 0
    ): void {
        self::query()->updateOrInsert(
            [
                'stream' => $stream,
                'group_name' => $groupName,
                'consumer' => $consumer,
            ],
            [
                'last_id' => $lastId,
                'last_lag' => $lastLag,
                'updated_at' => now(),
            ]
        );
    }

    /**
     * 获取指定 stream + group 下所有消费者的位点（用于监控聚合）
     *
     * @return array<string, array{consumer: string, last_id: string, last_lag: int, updated_at: string}>
     */
    public static function getAllConsumerOffsets(string $stream, string $groupName): array
    {
        return self::query()
            ->where('stream', $stream)
            ->where('group_name', $groupName)
            ->get()
            ->mapWithKeys(fn (self $row) => [
                $row->consumer => [
                    'consumer' => $row->consumer,
                    'last_id' => $row->last_id,
                    'last_lag' => $row->last_lag,
                    'updated_at' => $row->updated_at?->format('Y-m-d H:i:s') ?? '',
                ],
            ])
            ->toArray();
    }
}
