<?php
/**
 * EdgeAuth 设备密钥配置
 *
 * 格式：clientid:username => password(secret)
 * deploy.sh / seed_redis.php 会读取此文件并预热到 Redis: device:secret:{clientid}:{username}
 *
 * ⚠️ 生产环境此文件权限建议 600，属主 root，且不要提交到 Git
 * ⚠️ 注意：Redis key 中的分隔符是【冒号】，deploy.sh 与 seed_redis.php 均保持原样，
 *    切勿替换为短横线（历史 bug 已修复）。
 */

return [
    // ========== 生产设备 ==========
    'device001:user001' => 'a8f3c9e2m7x1p5q8w2e4r6t9y',
    'device002:user001' => 'b2d4f6h8j0k1l3m5n7p8q9r0s',
    'device003:user002' => 'c3e5g7i9k2m4o6p8q0s1t2u3v',
    'gateway01:admin'   => 'd4f6h8j0l2n4p6q8r0t1u3w5y',

    // ========== 测试设备（压测用） ==========
    'bench_client:bench_user' => 'bench_secret',

    // ========== 示例格式说明 ==========
    // 'clientid:username' => 'hmac_secret_key',
    //   clientid  → MQTT clientId
    //   username  → MQTT username
    //   secret    → HMAC 签名密钥（建议 32 字节随机串）
];
