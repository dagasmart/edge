<?php
declare(strict_types=1);
namespace DagaSmart\Edge\Models;

use Illuminate\Database\Eloquent\Prunable;
use Throwable;

/**
 * 数据流模型（对应 mcs_edge_stream）
 *
 * 幂等设计：
 *   - stream_id 唯一约束
 *   - upsertBatch 使用 PG 原生 ON CONFLICT DO NOTHING
 *
 * 与 EdgeInsertRedisCommand 对齐的约定：
 *   - 表名从 $table 取，不硬编码、不拼接前缀
 *   - 冲突由 PG 忽略，不抛异常，无 fallback / 无逐条退化
 */
class EdgeStream extends Model
{
    use Prunable;

    protected $table = 'mcs_edge_stream';

    public $timestamps = false;

    protected $fillable = [
        'stream_id',
        'client_id',
        'topic',
        'payload',
        'received_ms',
        'received_at',
        'created_at',
    ];

    protected $casts = [
        'payload'     => 'array',
        'received_ms' => 'integer',
        'received_at' => 'datetime',
        'created_at'  => 'datetime',
    ];

    /**
     * 校验单行数据是否可写入主表
     *
     * @return string[] 错误描述列表，空数组表示校验通过
     */
    public static function validateRow(array $row): array
    {
        $errors = [];

        if (($row['stream_id'] ?? '') === '') {
            $errors[] = 'stream_id 不能为空';
        }

        if (($row['client_id'] ?? '') === '') {
            $errors[] = 'client_id 不能为空';
        }

        if (!isset($row['received_ms']) || (int) $row['received_ms'] <= 0) {
            $errors[] = 'received_ms 必须为正整数';
        }

        if (($row['received_at'] ?? '') === '') {
            $errors[] = 'received_at 不能为空';
        }

        return $errors;
    }

    /**
     * 幂等批量写入 —— PG 原生 ON CONFLICT DO NOTHING
     *
     * 不依赖 Laravel 版本，单条 SQL，批量支持。
     * 连接通过模型实例获取，自动继承基类 master 连接。
     *
     * @return array{0: int, 1: int} [写入数（含冲突）, 重复数]
     */
    public static function upsertBatch(array $rows): array
    {
        if ($rows === []) {
            return [0, 0];
        }

        $instance = new static;
        $table = $instance->getTable();
        $connection = $instance->getConnection();
        $now = now();

        $columns = ['stream_id', 'client_id', 'topic', 'payload', 'received_ms', 'received_at', 'created_at'];
        $placeholders = [];
        $bindings = [];

        foreach ($rows as $row) {
            $placeholders[] = '(?,?,?,?,?,?,?)';
            $bindings[] = $row['stream_id'] ?? '';
            $bindings[] = $row['client_id'] ?? 'unknown';
            $bindings[] = $row['topic'] ?? '';
            $bindings[] = $row['payload'] ?? '{}';
            $bindings[] = (int) ($row['received_ms'] ?? 0);
            $bindings[] = $row['received_at'] ?? $now->format('Y-m-d H:i:s');
            $bindings[] = $now;
        }

        $sql = sprintf(
            'INSERT INTO %s (%s) VALUES %s ON CONFLICT (stream_id) DO NOTHING',
            $table,
            implode(', ', $columns),
            implode(', ', $placeholders)
        );

        $connection->statement($sql, $bindings);

        return [count($rows), 0];
    }

    /**
     * 写入 PG 死信表（对应 mcs_edge_stream_fail）
     */
    public static function writeDead(
        string $streamId,
        string $stream,
        string $group,
        string $consumer,
        int $delivered,
        string $payload,
        string $error,
        string $clientId = 'unknown',
        string $topic = '',
        int $receivedMs = 0,
        ?string $receivedAt = null,
    ): void {
        try {
            EdgeStreamFail::query()->insert([
                'stream_id'     => $streamId,
                'stream'        => $stream,
                'client_id'     => $clientId,
                'topic'         => $topic,
                'payload'       => $payload,
                'received_ms'   => $receivedMs,
                'received_at'   => $receivedAt,
                'last_error'    => $error,
                'group_name'    => $group,
                'consumer'      => $consumer,
                'delivered'     => $delivered,
                'retry_num'     => 0,
                'next_retry_at' => null,
                'is_resolved'   => false,
                'created_at'    => now(),
            ]);
        } catch (Throwable $e) {
            fwrite(STDERR, '[PG-DEAD] 死信写入失败：' . $e->getMessage() . PHP_EOL);
        }
    }

    /**
     * 批量重试死信：校验 -> 批量写主表 -> 标记结果
     *
     * @param EdgeStreamFail[] $claims
     * @return array{retried: int, succeeded: int, failed: int}
     */
    public static function retryBatch(array $claims, int $maxRetries = 3): array
    {
        $stats = ['retried' => count($claims), 'succeeded' => 0, 'failed' => 0];

        if ($claims === []) {
            return $stats;
        }

        $validRows = [];
        $invalidIds = [];

        foreach ($claims as $fail) {
            $row = [
                'stream_id'   => $fail->stream_id,
                'client_id'   => $fail->client_id,
                'topic'       => $fail->topic,
                'payload'     => $fail->payload,
                'received_ms' => $fail->received_ms,
                'received_at' => $fail->received_at?->format('Y-m-d H:i:s'),
            ];

            $errors = self::validateRow($row);

            if ($errors !== []) {
                $invalidIds[] = $fail->id;
                continue;
            }

            $validRows[] = [
                'fail'   => $fail,
                'db_row' => [
                    'stream_id'   => $fail->stream_id,
                    'client_id'   => $fail->client_id,
                    'topic'       => $fail->topic,
                    'payload'     => json_encode($fail->payload, JSON_UNESCAPED_UNICODE),
                    'received_ms' => $fail->received_ms,
                    'received_at' => $fail->received_at?->format('Y-m-d H:i:s'),
                    'created_at'  => now(),
                ],
            ];
        }

        if ($invalidIds !== []) {
            EdgeStreamFail::markBatchCompleted($invalidIds, [
                'is_resolved' => true,
                'last_error'  => 'validation failed, marked as resolved',
            ]);
        }

        if ($validRows === []) {
            return $stats;
        }

        $dbRows = array_column($validRows, 'db_row');
        $failIds = array_map(static fn (array $item) => $item['fail']->id, $validRows);

        try {
            [$written] = self::upsertBatch($dbRows);

            EdgeStreamFail::markBatchCompleted($failIds, [
                'is_resolved' => true,
                'last_error'  => 'retried and succeeded',
            ]);

            $stats['succeeded'] = $written;

            return $stats;
        } catch (Throwable $e) {
            $retryNum = $claims[0]->retry_num + 1;

            $attributes = [
                'retry_num'     => $retryNum,
                'last_error'    => 'retry failed: ' . $e->getMessage(),
                'next_retry_at' => match ($retryNum) {
                    1       => now()->addMinute(),
                    2       => now()->addMinutes(5),
                    default => now()->addMinutes(15),
                },
            ];

            if ($retryNum >= $maxRetries) {
                $attributes['is_resolved'] = true;
                $attributes['last_error']  = 'max retries exceeded: ' . $e->getMessage();
                $stats['failed'] = count($failIds);
            }

            EdgeStreamFail::markBatchCompleted($failIds, $attributes);

            return $stats;
        }
    }

    /**
     * 更新消费位点（对应 mcs_edge_stream_offset）
     */
    public static function updateOffset(
        string $stream,
        string $group,
        string $lastId,
        int $lag,
    ): void {
        EdgeStreamOffset::query()->updateOrInsert(
            ['stream' => $stream, 'group_name' => $group],
            ['last_id' => $lastId, 'last_lag' => $lag, 'updated_at' => now()],
        );
    }

    public function prunable()
    {
        return static::where(
            'created_at',
            '<',
            now()->subMonths(6)->startOfMonth(),
        );
    }
}
