<?php

declare(strict_types=1);

namespace DagaSmart\Edge\Support;

/**
 * 消息优先级路由器
 *
 * 根据 MQTT 主题匹配规则，将消息分为高优先级和普通优先级。
 * 高优主题支持 MQTT 通配符（+ 和 #）匹配。
 */
class PriorityRouter
{
    /** @var string[] 编译后的正则表达式数组 */
    private readonly array $highPriorityTopics;

    public function __construct(
        array $highPriorityTopics = [],
        private readonly ?MultiDimAdaptiveBatch $batch = null,
    ) {
        $this->highPriorityTopics = $this->compilePatterns($highPriorityTopics);
    }

    /**
     * 判断主题是否为高优先级
     */
    public function isHighPriority(string $topic): bool
    {
        foreach ($this->highPriorityTopics as $pattern) {
            if (@preg_match($pattern, $topic) === 1) {
                return true;
            }
        }

        return false;
    }

    /**
     * 获取关联的批量控制器
     */
    public function getBatchController(): ?MultiDimAdaptiveBatch
    {
        return $this->batch;
    }

    /**
     * 获取编译后的正则模式列表（调试用）
     */
    public function getCompiledPatterns(): array
    {
        return $this->highPriorityTopics;
    }

    /**
     * 将 MQTT 通配符编译为正则表达式
     *
     * MQTT 通配符规则：
     * - + 匹配单层（不含 /）
     * - # 匹配多层（必须是最后一段）
     */
    private function compilePatterns(array $topics): array
    {
        $patterns = [];

        foreach ($topics as $topic) {
            if (! is_string($topic) || $topic === '') {
                continue;
            }

            $segments = explode('/', $topic);
            $regex = '/^';

            foreach ($segments as $i => $segment) {
                if ($i > 0) {
                    $regex .= '\/';
                }

                // # 只能出现在最后一段
                if ($segment === '#') {
                    if ($i !== count($segments) - 1) {
                        // # 不在末尾，跳过此非法模式
                        continue 2;
                    }
                    $regex .= '(?:$|\/.*)';
                    continue;
                }

                // 逐字符构建，精确转义
                $escaped = '';
                $chars = mb_str_split($segment);
                foreach ($chars as $char) {
                    if ($char === '+') {
                        $escaped .= '[^/]+';
                    } else {
                        // 只转义正则特殊字符，不用 preg_quote 避免干扰
                        $escaped .= preg_quote($char, '/');
                    }
                }
                $regex .= $escaped;
            }

            $regex .= '$/';
            $patterns[] = $regex;
        }

        return $patterns;
    }
}
