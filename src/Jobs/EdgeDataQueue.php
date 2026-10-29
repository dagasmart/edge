<?php
declare(strict_types=1);
namespace DagaSmart\Edge\Jobs;

use Carbon\CarbonInterface;
use DagaSmart\Edge\Models\EdgeDataStream;
use Illuminate\Bus\Queueable;
use Illuminate\Contracts\Queue\ShouldBeUnique;
use Illuminate\Contracts\Queue\ShouldQueue;
use Illuminate\Foundation\Bus\Dispatchable;
use Illuminate\Queue\InteractsWithQueue;
use Illuminate\Queue\SerializesModels;
use Illuminate\Support\Carbon;
use Illuminate\Support\Facades\Log;
use Throwable;

/**
 * 边缘设备数据异步入库 Job
 *
 * 设计要点：
 * 1. ShouldBeUnique —— 同一设备 + 同一毫秒时间戳只入队一次，防 broker 重试/网络抖动造成的重复投递
 * 2. updateOrCreate  —— 唯一键冲突时静默覆盖，与 migration 的 UNIQUE 约束配合实现幂等
 * 3. 不抛异常给 broker —— 入库失败只记日志，由 failed_jobs 表兜底 + queue:retry 重跑
 */
class EdgeDataQueue implements ShouldQueue, ShouldBeUnique
{
    use Dispatchable;
    use InteractsWithQueue;
    use Queueable;
    use SerializesModels;

    /* ============ 队列行为配置 ============ */

    public int $tries = 3;
    public int $timeout = 30;
    public array $backoff = [5, 15, 30];
    public $queue = 'edge-data';
    public int $uniqueFor = 300;

    /* ============ 任务载荷 ============ */

    /**
     * 载荷结构（来自控制器的 $validated）：
     * [
     *   'payload'   => array,        // 设备上报的原始数据
     *   'topic'     => string,       // 如 edge/abc123/data
     *   'clientid'  => string,       // MQTT client_id
     *   'timestamp' => ?int,         // 毫秒时间戳，null 则用入库时刻
     * ]
     */
    private array $data;

    /* ============ 构造 ============ */

    public function __construct(array $data)
    {
        $this->data = $data;
        $this->onQueue('edge-data');
    }

    /* ============ 唯一锁 ============ */

    public function uniqueId(): string
    {
        return 'edge_data:'
            . $this->data['clientid']
            . ':'
            . ($this->data['timestamp'] ?? '0');
    }

    /* ============ 核心逻辑 ============ */

    public function handle(): void
    {
        $receivedMs = $this->resolveReceivedMs();

        EdgeDataStream::query()->updateOrCreate(
            [
                'client_id'   => $this->data['clientid'],
                'received_ms' => $receivedMs,
                'topic'       => $this->data['topic'],
            ],
            [
                'payload'     => $this->data['payload'],
                'received_at' => $this->msToCarbon($receivedMs),
            ]
        );

        Log::debug('[EdgeDataQueue] persisted', [
            'client_id' => $this->data['clientid'],
            'topic'     => $this->data['topic'],
            'ms'        => $receivedMs,
            'job_id'    => $this->job?->getJobId(),
        ]);
    }

    /* ============ 异常处理 ============ */

    public function retryUntil(): CarbonInterface
    {
        return now()->addMinutes(5);
    }

    public function failed(?Throwable $exception): void
    {
        Log::error('[EdgeDataQueue] job failed permanently', [
            'client_id' => $this->data['clientid'] ?? null,
            'topic'     => $this->data['topic'] ?? null,
            'payload'   => $this->data['payload'] ?? null,
            'error'     => $exception?->getMessage(),
            'trace'     => $exception?->getTraceAsString(),
        ]);
    }

    /* ============ 私有辅助 ============ */

    private function resolveReceivedMs(): int
    {
        $ts = $this->data['timestamp'] ?? (int) (microtime(true) * 1000);
        $ts = (int) $ts;

        if ($ts > 0 && $ts < 1_000_000_000_000) {
            $ts *= 1000;
        }

        return $ts;
    }

    private function msToCarbon(int $ms): Carbon
    {
        return Carbon::createFromTimestampMs($ms);
    }
}
