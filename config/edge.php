<?php

return [
    'mqtt' => [
        'host' => '1.15.51.149',
        'port' => 21883,
        'timeout' => 3,
        'tls' => [
            'enable' => false,
        ],
        'batch_size' => 200,           // 批量写入大小（普通 Channel 初始 batch）
        'flush_interval_ms' => 100,   // 最大刷盘间隔（毫秒）
        'channel_size' => 4096,       // Channel 队列容量（普通 Channel）
        'max_payload_len' => 65536,   // 单条 payload 上限（64KB）

        /* Worker 池 */
        'normal_workers' => 4,
        'high_workers' => 2,
        'dead_workers' => 1,
        'worker_idle_timeout' => 5,

        // 死信 Channel（可选，有就启用）
        'dead_channel_size' => 1024,
        'dead_batch_size' => 50,
        'dead_flush_interval_ms' => 1000,

        // 心跳续期 ttl（入库侧 device:active:{sn/client} 的过期时间）
        'device_active_ttl' => 60,

        // 多主题订阅：数组 / 逗号分隔字符串 / 单字符串，支持 +/# 通配符
        'topics' => [
            'heartbeat/response', // MQTT 心跳 topic
            'face/+/request',     // ★ 设备请求 topic（如 face/{client_id}/request）
            'face/+/response',    // ★ 设备响应 topic（如 face/{client_id}/response）
        ],

        // 高优先级主题（MQTT + 通配符匹配，命中则走高优 Channel）
        'high_priority_topics' => [
            'face/+/response',    // ★ * 改 + ，匹配 face/{client_id}/response
            'face/+/request',     // ★ 新增：设备请求走高优
            'face/response',      // 推送人脸识别结果 topic（无 client_id 变体）
            'qr/response',        // 二维码数据 topic
            'online/response',    // mqtt 上线 topic
            'will/+/response',    // 推送遗嘱 topic
        ],

        // 高优 Channel 配置
        'high_channel_size' => 512,
        'high_batch_size' => 10,
        'high_flush_interval_ms' => 20, // 毫秒

        // 自适应 batch 配置（普通 Channel）
        'adapt_min_batch' => 10,
        'adapt_max_batch' => 2000,
        'adapt_alpha' => 0.3,         // EMA 平滑系数，越大对瞬时流量越敏感

        /* ========== Prometheus 观测 ========== */
        'stats_interval_sec' => 5,    // 内存指标刷新间隔
        'histogram_buckets' => [      // 耗时/大小分布桶（毫秒 / 条）
            1, 5, 10, 25, 50, 100, 200, 500, 1000, 2000, 5000,
        ],
    ],

    // redis落盘
    'redis' => [
        'pool_size' => 64,
        'db' => 2,
        // Stream key 前缀（按需调整）
        'stream_high' => 'stream:device:high',
        'stream_data' => 'stream:device:data',
        'stream_dead' => 'stream:device:dead',

        'stream_max_len' => 10000,   // 每个 stream key 近似裁剪保留条数

        /* ========== 死信消费端（可选） ========== */
        'dead_letter' => [
            'enabled' => true,
            'group' => 'edge-consumer',
            'consumer' => 'worker-1',    // 多实例部署需唯一
            'batch_size' => 100,         // 单次 XREADGROUP 条数
            'block_ms' => 2000,          // 阻塞等待毫秒
            'claim_idle_ms' => 30000,    // 超过此时间未 ACK 视为 pending
            'retry_limit' => 3,          // 单条最大重试次数
        ],
    ],

    // 指标追踪
    'metrics' => [
        'enabled' => true,
        'scheme' => 'http',
        'host' => '0.0.0.0',
        'port' => 9090,
        'path' => '/metrics',           // Prometheus scrape 路径
        'instance' => '',               // 留空自动取 hostname:pid
    ],
];
