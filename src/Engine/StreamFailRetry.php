<?php

declare(strict_types=1);

namespace DagaSmart\Edge\Engine;

use DagaSmart\Edge\Models\EdgeStream;
use DagaSmart\Edge\Models\EdgeStreamFail;
use Illuminate\Support\Facades\DB;
use RuntimeException;
use Throwable;

/**
 * 边缘数据流失败重试引擎
 *
 * 死信统一存储在 mcs_edge_stream_fail：
 *   - retry_num      : 已重试次数
 *   - next_retry_at  : 下次重试时间（指数退避 + 全抖动）
 *   - is_resolved    : 是否已处理
 */
class StreamFailRetry
{
    protected const int DEFAULT_MAX_ATTEMPTS = 5;
    protected const int DEFAULT_BASE_DELAY_MS = 1000;
    protected const int DEFAULT_MAX_DELAY_MS = 60000;

    public function __construct(
        protected string $connection = 'master',
        protected int $maxAttempts = self::DEFAULT_MAX_ATTEMPTS,
        protected int $baseDelayMs = self::DEFAULT_BASE_DELAY_MS,
        protected int $maxDelayMs = self::DEFAULT_MAX_DELAY_MS,
    ) {}

    public function retryPending(int $batch = 50): int
    {
        $rows = $this->fetchPending($batch);

        $retried = 0;

        foreach ($rows as $row) {
            try {
                $success = $this->retryOne($row);

                if ($success) {
                    $this->markResolved($row->id);
                    $retried++;
                } else {
                    $this->scheduleNextRetry($row);
                }
            } catch (Throwable $e) {
                fwrite(
                    STDERR,
                    sprintf(
                        '[StreamFailRetry] retry failed id=%d error=%s%s',
                        $row->id,
                        $e->getMessage(),
                        PHP_EOL
                    )
                );

                $this->scheduleNextRetry($row);
            }
        }

        return $retried;
    }

    /**
     * 重试单条：回填 mcs_edge_stream
     */
    protected function retryOne(object $row): bool
    {
        if (!isset($row->client_id, $row->topic, $row->payload, $row->received_ms, $row->received_at)) {
            throw new RuntimeException('死信记录字段不完整，无法重试');
        }

        $rows = [
            [
                'stream_id'   => $row->stream_id ?? '',
                'client_id'   => $row->client_id,
                'topic'       => $row->topic ?? '',
                'payload'     => is_string($row->payload)
                    ? $row->payload
                    : json_encode($row->payload, JSON_UNESCAPED_UNICODE | JSON_THROW_ON_ERROR),
                'received_ms' => (int) $row->received_ms,
                'received_at' => $row->received_at,
            ],
        ];

        [$written, $duplicate] = EdgeStream::upsertBatch($rows);

        return ($written + $duplicate) > 0;
    }

    protected function calculateBackoffMs(int $attempts): int
    {
        $power = min($attempts, 31);
        $cap   = (int) min(
            $this->maxDelayMs,
            $this->baseDelayMs * (1 << $power)
        );

        if ($cap <= 0) {
            return 0;
        }

        return random_int(0, $cap);
    }

    protected function fetchPending(int $batch): array
    {
        return DB::connection($this->connection)
            ->table('mcs_edge_stream_fail')
            ->where('is_resolved', false)
            ->whereNotNull('next_retry_at')
            ->where('next_retry_at', '<=', now())
            ->orderBy('next_retry_at')
            ->limit($batch)
            ->get()
            ->all();
    }

    protected function scheduleNextRetry(object $row): void
    {
        $nextAttempts = ($row->retry_num ?? 0) + 1;

        if ($nextAttempts >= $this->maxAttempts) {
            DB::connection($this->connection)
                ->table('mcs_edge_stream_fail')
                ->where('id', $row->id)
                ->update([
                    'retry_num'     => $nextAttempts,
                    'is_resolved'   => true,
                    'next_retry_at' => null,
                    'updated_at'    => now(),
                ]);

            fwrite(
                STDERR,
                sprintf('[StreamFailRetry] max attempts reached id=%d%s', $row->id, PHP_EOL)
            );

            return;
        }

        $delayMs = $this->calculateBackoffMs($nextAttempts);

        DB::connection($this->connection)
            ->table('mcs_edge_stream_fail')
            ->where('id', $row->id)
            ->update([
                'retry_num'     => $nextAttempts,
                'next_retry_at' => now()->addMilliseconds($delayMs),
                'updated_at'    => now(),
            ]);
    }

    protected function markResolved(int $id): void
    {
        DB::connection($this->connection)
            ->table('mcs_edge_stream_fail')
            ->where('id', $id)
            ->update([
                'is_resolved'   => true,
                'next_retry_at' => null,
                'updated_at'    => now(),
            ]);
    }
}
