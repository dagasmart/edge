<?php

declare(strict_types=1);

namespace DagaSmart\Edge\Support;

/**
 * 自适应 Batch（单维度：排队等待时间）
 *
 * 根据消息在 Channel 中的排队等待时间，
 * 动态放大或缩小 flush batch size。
 *
 * 低峰 → 小 batch（低延迟）
 * 高峰 → 大 batch（高吞吐）
 */
class AdaptiveBatch
{
    /** 当前建议的 batch size */
    private int $current;

    /** batch 下限，保证低峰延迟 */
    private int $minBatch;

    /** batch 上限，防止撑爆 Redis Pipeline */
    private int $maxBatch;

    /** 基准值，用于按比例放大 */
    private int $baseBatch;

    /** EMA 平滑系数，越小越稳 */
    private float $alpha;

    /** 刷盘间隔（ms），用于计算等待时间比例 */
    private int $flushIntervalMs;

    /** 最近一次 adapt() 的原始目标值（调试用） */
    private int $lastTarget = 0;

    public function __construct(
        int $minBatch = 10,
        int $maxBatch = 2000,
        int $baseBatch = 200,
        float $alpha = 0.3,
        int $flushIntervalMs = 100,
    ) {
        $this->minBatch = max(1, $minBatch);
        $this->maxBatch = max($this->minBatch, $maxBatch);
        $this->baseBatch = max(1, $baseBatch);
        $this->alpha = max(0.0, min(1.0, $alpha));
        $this->flushIntervalMs = max(1, $flushIntervalMs);
        // 初始值取中间值，避免冷启动过激
        $this->current = (int) (($this->minBatch + $this->baseBatch) / 2);
    }

    /**
     * 根据排队等待时间调整 batch（EMA 平滑）
     *
     * @param  float  $waitMs  从入队到刷盘经历的毫秒数
     */
    public function adapt(float $waitMs): int
    {
        // 等待时间越长 → 积压越严重 → 需要更大的 batch
        $ratio = $waitMs / (float) $this->flushIntervalMs;
        $target = (int) round($ratio * $this->baseBatch);
        $target = max($this->minBatch, min($this->maxBatch, $target));

        // EMA 平滑，避免抖动
        $this->current = (int) (
            $this->alpha * $target
            + (1 - $this->alpha) * $this->current
        );
        $this->current = max($this->minBatch, min($this->maxBatch, $this->current));
        $this->lastTarget = $target;

        return $this->current;
    }

    /**
     * 带背压感知：Channel 水位 ≥ 阈值直接顶满
     */
    public function adaptWithPressure(
        float $waitMs,
        int $channelCapacity,
        int $channelLength,
        float $pressureRatio = 0.8,
    ): int {
        if ($channelCapacity > 0) {
            $pressure = $channelLength / $channelCapacity;
            if ($pressure >= $pressureRatio) {
                // 背压严重，直接顶满，尽快消化积压
                $this->current = $this->maxBatch;

                return $this->maxBatch;
            }
        }

        return $this->adapt($waitMs);
    }

    /**
     * 获取当前建议的 batch size（不更新状态）
     */
    public function current(): int
    {
        return $this->current;
    }

    /**
     * 强制设置（用于配置热更新或测试）
     */
    public function setCurrent(int $size): void
    {
        $this->current = max($this->minBatch, min($this->maxBatch, $size));
    }

    /**
     * 获取统计信息（调试 / 指标暴露）
     */
    public function stats(): array
    {
        return [
            'current' => $this->current,
            'min' => $this->minBatch,
            'max' => $this->maxBatch,
            'base' => $this->baseBatch,
            'last_target' => $this->lastTarget,
            'alpha' => $this->alpha,
            'interval_ms' => $this->flushIntervalMs,
        ];
    }
}
