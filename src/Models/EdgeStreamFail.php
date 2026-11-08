<?php

declare(strict_types=1);

namespace DagaSmart\Edge\Models;

use Throwable;

/**
 * 死信模型（对应 mcs_edge_stream_fail，分片版）
 *
 * 重试引擎：
 *   - claimBatch：advisory lock 保护，防多进程并发重试同一批
 *   - markBatchCompleted：批量标记完成/失败
 *   - forceUnlock：进程异常退出兜底
 *
 * 分片版改动：
 *   - 新增 shard_no / source_stream 字段，追溯数据来源
 *   - claimBatch 增加 shard 过滤条件
 */
class EdgeStreamFail extends Model
{
    protected $table = 'mcs_edge_stream_fail';

    public $timestamps = false;

    protected $fillable = [
        'shard_no',
        'stream_id',
        'source_stream',
        'client_id',
        'topic',
        'payload',
        'received_ms',
        'received_at',
        'last_error',
        'group_name',
        'consumer',
        'delivered',
        'retry_num',
        'next_retry_at',
        'is_resolved',
        'created_at',
    ];

    protected $casts = [
        'shard_no' => 'integer',
        'payload' => 'array',
        'received_ms' => 'integer',
        'received_at' => 'datetime',
        'next_retry_at' => 'datetime',
        'is_resolved' => 'boolean',
        'retry_num' => 'integer',
        'delivered' => 'integer',
        'created_at' => 'datetime',
    ];

    /**
     * 认领待重试批次（pg_try_advisory_lock + 超时）
     *
     * 分片版：可按 shard 过滤，让不同分片的重试互不干扰
     *
     * @return self[]
     */
    public static function claimBatch(
        string $stream,
        string $group,
        int $limit = 50,
        int $maxRetries = 3,
        int $lockTimeoutMs = 2000,
        ?int $shardNo = null,
    ): array {
        $lockKey = crc32($stream.':'.$group.':'.($shardNo ?? 'all'));
        $instance = new static;
        $connection = $instance->getConnection();

        $deadline = microtime(true) + ($lockTimeoutMs / 1000);
        $acquired = false;

        while (microtime(true) < $deadline) {
            try {
                $result = $connection->selectOne(
                    'SELECT pg_try_advisory_lock(?) as acquired',
                    [$lockKey]
                );

                if ($result && $result->acquired) {
                    $acquired = true;
                    break;
                }
            } catch (Throwable $e) {
                fwrite(STDERR, sprintf(
                    '[ADVISORY-LOCK] 获取锁失败 stream=%s group=%s: %s'.PHP_EOL,
                    $stream,
                    $group,
                    $e->getMessage(),
                ));
                break;
            }

            usleep(50000); // 50ms
        }

        if (! $acquired) {
            fwrite(STDERR, sprintf(
                '[ADVISORY-LOCK] 获取锁超时 stream=%s group=%s timeout=%dms'.PHP_EOL,
                $stream,
                $group,
                $lockTimeoutMs,
            ));

            return [];
        }

        try {
            $query = static::query()
                ->where('stream', $stream)
                ->where('group_name', $group)
                ->where('is_resolved', false)
                ->where('retry_num', '<', $maxRetries)
                ->where(function ($q) {
                    $q->whereNull('next_retry_at')
                        ->orWhere('next_retry_at', '<=', now());
                });

            // ★ 按分片过滤，可选
            if ($shardNo !== null) {
                $query->where('shard_no', $shardNo);
            }

            return $query->limit($limit)
                ->orderBy('created_at')
                ->get()
                ->all();
        } finally {
            try {
                $connection->statement(
                    'SELECT pg_advisory_unlock(?)',
                    [$lockKey]
                );
            } catch (Throwable $e) {
                fwrite(STDERR, sprintf(
                    '[ADVISORY-UNLOCK] 释放锁失败 stream=%s group=%s: %s'.PHP_EOL,
                    $stream,
                    $group,
                    $e->getMessage(),
                ));
            }
        }
    }

    /**
     * 批量标记完成
     */
    public static function markBatchCompleted(array $ids, array $attributes): void
    {
        if ($ids === []) {
            return;
        }

        static::query()
            ->whereIn('id', $ids)
            ->update($attributes);
    }

    /**
     * 强制释放 advisory lock（进程异常退出兜底）
     */
    public static function forceUnlock(string $stream, string $group, ?int $shardNo = null): void
    {
        $lockKey = crc32($stream.':'.$group.':'.($shardNo ?? 'all'));

        try {
            (new static)->getConnection()->statement(
                'SELECT pg_try_advisory_unlock(?)',
                [$lockKey]
            );
        } catch (Throwable $e) {
            fwrite(STDERR, sprintf(
                '[ADVISORY-UNLOCK] 强制释放锁失败 stream=%s group=%s: %s'.PHP_EOL,
                $stream,
                $group,
                $e->getMessage(),
            ));
        }
    }
}
