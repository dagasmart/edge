<?php
declare(strict_types=1);
// gen_request.php — 跑一次拿到请求体
$clientId = 'test_client';
$username = 'test_user';
$secret   = 'my_shared_secret'; // 对应 Redis 里 device:secret:test_client:test_user 的值

$timestamp = time();
$nonce     = bin2hex(random_bytes(8));
$payload   = $clientId . '|' . $username . '|' . $timestamp . '|' . $nonce;
$sign      = base64_encode(hash_hmac('sha256', $payload, $secret, true));

$password = $sign . '|' . $timestamp . '|' . $nonce;

echo "clientid: {$clientId}\n";
echo "username: {$username}\n";
echo "password: {$password}\n\n";

// 直接输出 curl 命令
echo "curl -X POST http://127.0.0.1:9503/mqtt/auth \\\n";
echo "  -d 'clientid={$clientId}' \\\n";
echo "  -d 'username={$username}' \\\n";
echo "  -d 'password={$password}' \\\n";
echo "  -d 'ipaddr=127.0.0.1'\n";
