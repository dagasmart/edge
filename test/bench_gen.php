<?php
declare(strict_types=1);

/**
 * 离线 HMAC 请求体生成器
 *
 * 用法: php bench_gen.php [数量] [输出文件]
 * 默认: php bench_gen.php 200000 /tmp/bench_bodies.txt
 */

$count   = (int)($argv[1] ?? 200000);
$output  = $argv[2] ?? '/tmp/bench_bodies.txt';

// ---------- Redis 连接 ----------
$redis = new Redis();
$redis->connect('127.0.0.1', 16379, 2);
$redis->auth('uApDVQD7');
$redis->select(2);

// ---------- 配置 ----------
$clientIdPrefix = 'bench';
$usernamePrefix = 'user';
$secret         = 'bench-secret-2024';
$timeDrift      = 120; // 跟 server.php 的 TIME_DRIFT 一致
$noncePrefix    = 'nonce';

// ---------- 先往 Redis 写 secret ----------
// 为了让压测时 L1 缓存命中，写一批 clientId:username 的 secret
$uniquePairs = min($count, 1000); // 最多 1000 个不同用户
for ($i = 0; $i < $uniquePairs; $i++) {
    $cid = $clientIdPrefix . '-' . ($i % 200);
    $uid = $usernamePrefix . '-' . ($i % 1000);
    $redis->setex("device:secret:{$cid}:{$uid}", 3600, $secret);
}
echo "[gen] Redis secrets written: {$uniquePairs} pairs\n";

// ---------- 生成请求体 ----------
$fp = fopen($output, 'w');
if (!$fp) {
    die("[gen] Cannot open output file: {$output}\n");
}

$now = time();
$written = 0;

for ($i = 0; $i < $count; $i++) {
    $cid  = $clientIdPrefix . '-' . ($i % 200);
    $uid  = $usernamePrefix . '-' . ($i % 1000);
    $ts   = $now + ($i % $timeDrift) - ($timeDrift / 2); // 在漂移窗口内波动
    $nonce = $noncePrefix . '-' . $i;

    // payload 跟 hmac.php 里完全一致
    $payload = $cid . '|' . $uid . '|' . $ts . '|' . $nonce;

    $sign = hash_hmac('sha256', $payload, $secret, true);
    $b64  = base64_encode($sign);

    // password 字段格式：base64|timestamp|nonce
    $password = $b64 . '|' . $ts . '|' . $nonce;

    // POST body（application/x-www-form-urlencoded）
    $body = http_build_query([
        'clientid' => $cid,
        'username' => $uid,
        'password' => $password,
        'ipaddr'   => '127.0.0.1',
    ]);

    fwrite($fp, $body . "\n");
    $written++;

    if ($written % 50000 === 0) {
        echo "[gen] {$written} bodies written...\n";
    }
}

fclose($fp);

echo "[gen] Done! {$written} request bodies → {$output}\n";
echo "[gen] File size: " . round(filesize($output) / 1024 / 1024, 1) . " MB\n";
echo "\n";
echo "Next step:\n";
echo "  wrk -t12 -c1000 -d30s --script=bench_wrk.lua --latency http://127.0.0.1:9503/mqtt/auth\n";
