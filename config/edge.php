<?php

return [
    'mqtt' => [
        'batch_size' => 50,           // 批量写入大小
        'flush_interval_ms' => 100,   // 最大刷盘间隔
        'channel_size' => 2048,      // Channel 队列容量
        'max_payload_len' => 65536,  // 单条 payload 上限（64KB）
    ],
    'redis' => [
        'pool_size' => 64,
        'db' => 2,
    ],
];
