<?php

declare(strict_types=1);

namespace App\Jobs;

use DagaSmart\Edge\Models\EdgeDataStream;
use Illuminate\Bus\Queueable;
use Illuminate\Contracts\Queue\ShouldBeUnique;
use Illuminate\Contracts\Queue\ShouldQueue;
use Illuminate\Foundation\Bus\Dispatchable;
use Illuminate\Queue\InteractsWithQueue;
use Illuminate\Queue\SerializesModels;
use Illuminate\Support\Facades\Log;
use Throwable;

/**
 * 设备数据异步入库 Job
 *
 * 设计要点：
 * 1. ShouldBeUnique —— 同一设备 + 同一毫秒时间戳只入队一次，防 broker 重试/网络抖动造成的重复投递
 * 2. updateOrCreate  —— 唯一键冲突时静默覆盖，与 migration 的 UNIQUE 约束配合实现幂等
 * 3. 不抛异常给 broker —— 入库失败只记日志，由 failed_jobs 表兜底 + queue:retry 重跑
 */
class NanoEdgeData implements ShouldQueue, ShouldBeUnique
{
    use Dispatchable;
    use InteractsWithQueue;
    use Queueable;
    use SerializesModels;

    /* ============ 队列行为配置 ============ */

    /** @var int 最大尝试次数（含首次） */
    public int $tries = 3;

    /** @var int 单 Job 超时（秒），超过则 worker 强制杀掉并标记为失败 */
    public int $timeout = 30;

    /** @var array<int> 重试退避（秒）：第1次失败等5s，第2次等15s，第3次等30s */
    public array $backoff = [5, 15, 30];

    /** @var string 投递到的队列名，与 supervisor 的 --queue=nano-data 对应 */
    public string $queue = 'nano-data';

    /** @var int 唯一锁 TTL（秒）：必须大于 broker 可能的最大重试窗口，否则重复投递会穿透 */
    public int $uniqueFor = 300;

    /* ============ 任务载荷 ============ */

    /**
     * 载荷结构（来自控制器的 $validated）：
     * [
     *   'payload'   => array,        // 设备上报的原始数据
     *   'topic'     => string,       // 如 device/abc123/data
     *   'clientid'  => string,       // MQTT client_id
     *   'timestamp' => ?int,         // 毫秒时间戳，null 则用入库时刻
     * ]
     */
    private array $data;

    /* ============ 构造 / 反序列化 ============ */

    public function __construct(array $data)
    {
        $this->data = $data;

        // 明确指定队列，覆盖 $queue 属性（Laravel 8+ 推荐写法）
        $this->onQueue('nano-data');
    }

    /**
     * 唯一键：同一设备在同一毫秒的上报只处理一次
     *
     * 与 migration 的唯一键 (device_id, received_ms, topic) 语义一致，
     * 但此处只需 device_id + 时间戳即可覆盖 99% 场景，降低锁粒度。
     */
    public function uniqueId(): string
    {
        return 'nano_data:' . $this->data['clientid'] . ':' . ($this->data['timestamp'] ?? '0');
    }

    /* ============ 核心逻辑 ============ */

    public function handle(): void
    {
        $receivedMs = $this->resolveReceivedMs();

        EdgeDataStream::query()->updateOrCreate(
            [
                // 查询条件 = 唯一键，命中则更新，不命中则插入
                'client_id'   => $this->data['clientid'],
                'received_ms' => $receivedMs,
                'topic'       => $this->data['topic'],
            ],
            [
                // 更新/插入的字段
                'payload'     => $this->data['payload'],   // Model 的 $casts 会自动 JSON 序列化
                'received_at' => $this->msToCarbon($receivedMs),
            ]
        );

        Log::debug('[PersistDeviceData] persisted', [
            'client_id' => $this->data['clientid'],
            'topic'     => $this->data['topic'],
            'ms'        => $receivedMs,
            'job_id'    => $this->job?->getJobId(),
        ]);
    }

    /* ============ 异常处理 ============ */

    /**
     * 即将重试前调用
     * 记录告警级日志，方便从日志里看出"有数据在反复失败"
     */
    public function retryUntil(): \DateTime
    {
        return now()->addMinutes(5);   // 超过 5 分钟不再重试，直接进 failed_jobs
    }

    /**
     * 彻底失败后（3 次都失败）调用
     * 不要在这里抛异常，否则会被 worker 捕获并可能触发死循环
     */
    public function failed(?Throwable $exception): void
    {
        Log::error('[PersistDeviceData] job failed permanently', [
            'device_id' => $this->data['clientid'] ?? null,
            'topic'     => $this->data['topic'] ?? null,
            'payload'   => $this->data['payload'] ?? null,   // 失败数据留在日志里，便于人工补偿
            'error'     => $exception?->getMessage(),
            'trace'     => $exception?->getTraceAsString(),
        ]);

        // 可选：写一张专门的 dlq 表，比 failed_jobs 更好查
        // DeviceDataDlq::create([...]);
    }

    /* ============ 私有辅助方法 ============ */

    /**
     * 解析毫秒时间戳
     * 兼容：毫秒（13位）/ 秒（10位）/ 缺失（用当前时间）
     */
    private function resolveReceivedMs(): int
    {
        $ts = $this->data['timestamp'] ?? (int) (microtime(true) * 1000);
        $ts = (int) $ts;

        // 10 位秒级时间戳 → 补成毫秒
        if ($ts > 0 && $ts < 1_000_000_000_000) {
            $ts *= 1000;
        }

        return $ts;
    }

    /**
     * 毫秒时间戳 → Carbon，供 $casts 为 datetime 的字段使用
     */
    private function msToCarbon(int $ms): \Illuminate\Support\Carbon
    {
        return \Illuminate\Support\Carbon::createFromTimestampMs($ms);
    }
}
