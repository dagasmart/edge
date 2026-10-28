<?php
$redis = new Redis();
$redis->connect('127.0.0.1', 16379, 2);
$redis->auth('uApDVQD7');
$redis->select(2);

$redis->set("device:secret:bench_client:bench_user","bench_secret");
echo "OK\n";

$val = $redis->get('device:secret:bench_client:bench_user');
echo "Value: [" . ($val ?? 'NULL') . "]\n";
echo "Length: " . strlen($val ?? '') . " bytes\n";
echo "Hex: " . bin2hex($val ?? '') . "\n";
