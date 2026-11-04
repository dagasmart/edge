<?php

return [
    'mqtt' => [
        'batch_size' => 200,           // 批量写入大小（普通 Channel 初始 batch）
        'flush_interval_ms' => 100,   // 最大刷盘间隔（毫秒）
        'channel_size' => 4096,       // Channel 队列容量（普通 Channel）
        'max_payload_len' => 65536,   // 单条 payload 上限（64KB）

        // 多主题订阅：数组 / 逗号分隔字符串 / 单字符串，支持 +/# 通配符
        'topics' => [
            'heartbeat/response',
        ],

        // 高优先级主题（前缀匹配，命中则走高优 Channel）
        'high_priority_topics' => [
            'device/+/alarm',
            'device/+/alert',
            'device/+/control',
            'device/+/command',
        ],

        // 高优 Channel 配置
        'high_channel_size' => 512,
        'high_batch_size' => 10,
        'high_flush_interval_ms' => 20, // 毫秒

        // 自适应 batch 配置（普通 Channel）
        'adapt_min_batch' => 10,
        'adapt_max_batch' => 2000,
        'adapt_alpha' => 0.3,         // EMA 平滑系数，越大对瞬时流量越敏感
    ],

    // redis落盘
    'redis' => [
        'pool_size' => 64,
        'db' => 2,
        // Stream key 前缀（按需调整）
        'stream_high' => 'stream:device:high',
        'stream_data' => 'stream:device:data',
        'stream_dead' => 'stream:device:dead',
    ],

    // 指标追踪
    'metrics' => [
        'enabled' => true,
        'host' => '0.0.0.0',
        'port' => 9090,
    ],
];
