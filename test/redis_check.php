<?php
declare(strict_types=1);

$redis = new Redis();
$redis->connect('127.0.0.1', 16379);
$redis->auth('uApDVQD7');
$redis->select(2);

$keys = $redis->keys('device:secret:*');
echo 'Found ' . count($keys) . " keys:\n";
foreach ($keys as $k) {
    $v = $redis->get($k);
    echo "  {$k} => {$v}\n";
}

// 额外：直接验证 hmac.php 会查的那个 key
echo "\n--- Direct GET ---\n";
$testKey = 'device:secret:device001:user001';
$val = $redis->get($testKey);
echo "GET {$testKey} => " . ($val ?? 'NULL') . "\n";

$testKey2 = 'device:secret:device001-user001';
$val2 = $redis->get($testKey2);
echo "GET {$testKey2} => " . ($val2 ?? 'NULL') . "\n";
