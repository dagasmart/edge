<?php
declare(strict_types=1);
// 自定义辅助函数

if (!function_exists('test')) {
    /**
     * @return bool
     */
    function test(): bool
    {
        return true;
    }
}


if (! function_exists('swow_co_sleep')) {
    function swow_co_sleep(float $seconds): void
    {
        if ($seconds <= 0) {
            try {
                (new \Swow\Channel(1))->pop(0);
            } catch (\Swow\ChannelException $e) {
                // 超时/被唤醒，忽略
            }
            return;
        }

        $ms = (int) ceil($seconds * 1000);
        try {
            $ch = new \Swow\Channel(1);
            $ch->pop($ms);
        } catch (\Swow\ChannelException $e) {
            // 超时正常，忽略
        }
    }
}
