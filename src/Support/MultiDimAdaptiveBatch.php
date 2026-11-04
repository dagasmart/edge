<?php

declare(strict_types=1);

namespace DagaSmart\Edge\Support;

/**
 * 多维自适应批量大小控制器
 *
 * 根据队列积压、延迟反馈动态调整批量大小，
 * 在吞吐量和延迟之间做平衡。
 */
class MultiDimAdaptiveBatch
{
    private int $current;

    public function __construct(
        private readonly int $min = 10,
        private readonly int $max = 2000,
        private readonly float $alpha = 0.3,
    ) {
        if ($this->min < 1) {
            throw new \InvalidArgumentException('min must be >= 1');
        }

        if ($this->max < $this->min) {
            throw new \InvalidArgumentException('max must be >= min');
        }

        if ($this->alpha <= 0.0 || $this->alpha > 1.0) {
            throw new \InvalidArgumentException('alpha must be between 0 and 1');
        }

        $this->current = $this->min;
    }

    /**
     * 获取当前批量大小
     */
    public function getBatchSize(): int
    {
        return $this->current;
    }

    /**
     * 根据队列积压调整批量大小
     *
     * 积压越高 → 批量越大（提升吞吐）
     * 积压越低 → 批量越小（降低延迟）
     */
    public function adjust(int $backlog): void
    {
        if ($backlog <= 0) {
            // 无积压，缓慢回落到最小值
            $this->current = max($this->min, (int) ($this->current * (1.0 - $this->alpha * 0.5)));
            return;
        }

        // 目标批量随积压线性增长，上限为 max
        $target = min($this->max, $this->min + $backlog);

        // 指数移动平均平滑，避免抖动
        $this->current = (int) (
            $this->alpha * $target + (1.0 - $this->alpha) * $this->current
        );

        $this->current = max($this->min, min($this->max, $this->current));
    }

    /**
     * 重置为最小批量
     */
    public function reset(): void
    {
        $this->current = $this->min;
    }
}
