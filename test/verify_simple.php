<?php

/**
 * EdgeAuth Simple Verify (Windows 兼容 - 最终版)
 * 所有请求统一用 fsockopen，Windows 下稳定
 */

$host     = '127.0.0.1';
$port     = 9503;
$clientId = 'bench_client';
$username = 'bench_user';
$secret   = 'bench_secret';

echo "=== EdgeAuth Simple Verify (Windows) ===\n\n";

// ---------- 辅助：生成签名 ----------
function genPayload($clientId, $username, $secret) {
    $ts    = time();
    $nonce = bin2hex(random_bytes(8));
    $payload = "{$clientId}|{$username}|{$ts}|{$nonce}";
    $sign  = hash_hmac('sha256', $payload, $secret, true);
    $b64   = strtr(base64_encode($sign), '+/=', '-_~');
    return [$b64 . '|' . $ts . '|' . $nonce, $nonce];
}

// ---------- 辅助：发 POST 请求 ----------
function sendPost($host, $port, $body) {
    $fp = @fsockopen($host, $port, $errno, $errstr, 3);
    if (!$fp) {
        return [0, "connect failed: {$errstr}"];
    }

    $req  = "POST /mqtt/auth HTTP/1.1\r\n";
    $req .= "Host: {$host}:{$port}\r\n";
    $req .= "Content-Type: application/x-www-form-urlencoded\r\n";
    $req .= "Content-Length: " . strlen($body) . "\r\n";
    $req .= "Connection: close\r\n\r\n";
    $req .= $body;

    fwrite($fp, $req);
    $response = '';
    while (!feof($fp)) {
        $chunk = fread($fp, 8192);
        if ($chunk === false) break;
        $response .= $chunk;
    }
    fclose($fp);

    if (preg_match('/HTTP\/1\.\d\s+(\d+)/', $response, $m)) {
        return [(int) $m[1], $response];
    }
    return [0, $response];
}

// ---------- 辅助：发 GET 请求（独立 fsockopen） ----------
function sendGet($host, $port, $path) {
    $fp = @fsockopen($host, $port, $errno, $errstr, 3);
    if (!$fp) {
        return [0, "connect failed: {$errstr}"];
    }

    $req  = "GET {$path} HTTP/1.1\r\n";
    $req .= "Host: {$host}:{$port}\r\n";
    $req .= "Connection: close\r\n\r\n";

    fwrite($fp, $req);
    $response = '';
    while (!feof($fp)) {
        $chunk = fread($fp, 8192);
        if ($chunk === false) break;
        $response .= $chunk;
    }
    fclose($fp);

    if (preg_match('/HTTP\/1\.\d\s+(\d+)/', $response, $m)) {
        return [(int) $m[1], $response];
    }
    return [0, $response];
}

// ---------- Warmup ----------
[$code, $resp] = sendPost($host, $port, 'clientid=&username=&password=');
if ($code === 0) {
    echo "ERROR: Cannot reach http://{$host}:{$port}\n";
    echo "Make sure server.php is running.\n";
    exit(1);
}
echo "Warmup OK (got HTTP {$code})\n\n";

$allPass = true;

// ---------- Test 1 ----------
[$code, $resp] = sendPost($host, $port, 'clientid=&username=&password=');
echo "Test 1  - Empty fields:          HTTP {$code} ";
echo $code === 401 ? "✅ PASS\n" : "❌ FAIL\n";
if ($code !== 401) $allPass = false;

// ---------- Test 2 ----------
[$password, $nonce] = genPayload($clientId, $username, $secret);
$body = 'clientid=' . rawurlencode($clientId)
    . '&username=' . rawurlencode($username)
    . '&password=' . rawurlencode($password);

[$code, $resp] = sendPost($host, $port, $body);
echo "Test 2  - Valid signature:       HTTP {$code} ";
echo $code === 200 ? "✅ PASS\n" : "❌ FAIL\n";
if ($code !== 200) $allPass = false;

// ---------- Test 3 ----------
[$code, $resp] = sendPost($host, $port, $body);
echo "Test 3  - Replay (cache hit):     HTTP {$code} ";
echo $code === 200 ? "✅ PASS (cache hit, expected 200)\n" : "❌ FAIL\n";
if ($code !== 200) $allPass = false;

// ---------- Test 3b ----------
$redis = new Redis();
$redis->connect('127.0.0.1', 16379, 2);
$redis->auth('uApDVQD7');
$redis->select(2);

$fakeNonce = bin2hex(random_bytes(8));
$redis->setex("device:nonce:{$clientId}:{$username}", 600, $fakeNonce);

$ts2 = time();
$payload2 = "{$clientId}|{$username}|{$ts2}|{$fakeNonce}";
$sign2 = hash_hmac('sha256', $payload2, $secret, true);
$b642 = strtr(base64_encode($sign2), '+/=', '-_~');
$body2 = 'clientid=' . rawurlencode($clientId)
    . '&username=' . rawurlencode($username)
    . '&password=' . rawurlencode($b642 . '|' . $ts2 . '|' . $fakeNonce);

[$code, $resp] = sendPost($host, $port, $body2);
echo "Test 3b - Nonce pre-used (401):   HTTP {$code} ";
echo $code === 401 ? "✅ PASS\n" : "❌ FAIL\n";
if ($code !== 401) $allPass = false;

$redis->del("device:nonce:{$clientId}:{$username}");

// ---------- Test 4 ----------
$badBody = 'clientid=' . rawurlencode($clientId)
    . '&username=' . rawurlencode($username)
    . '&password=' . rawurlencode(substr($password, 0, -1) . 'X');

[$code, $resp] = sendPost($host, $port, $badBody);
echo "Test 4  - Bad signature:          HTTP {$code} ";
echo $code === 401 ? "✅ PASS\n" : "❌ FAIL\n";
if ($code !== 401) $allPass = false;

// ---------- Test 5 ----------
$oldTs = time() - 9999;
$oldNonce = bin2hex(random_bytes(8));
$oldPayload = "{$clientId}|{$username}|{$oldTs}|{$oldNonce}";
$oldSign = hash_hmac('sha256', $oldPayload, $secret, true);
$oldB64 = strtr(base64_encode($oldSign), '+/=', '-_~');
$oldBody = 'clientid=' . rawurlencode($clientId)
    . '&username=' . rawurlencode($username)
    . '&password=' . rawurlencode($oldB64 . '|' . $oldTs . '|' . $oldNonce);

[$code, $resp] = sendPost($host, $port, $oldBody);
echo "Test 5  - Expired timestamp:      HTTP {$code} ";
echo $code === 401 ? "✅ PASS\n" : "❌ FAIL\n";
if ($code !== 401) $allPass = false;

// ---------- Test 6 & 7: GET 请求 ----------
// 关键：usleep 让 Windows 释放端口
usleep(300000); // 300ms

[$code, $resp] = sendGet($host, $port, '/health');
echo "Test 6  - /health endpoint:       HTTP {$code} ";
echo $code === 200 ? "✅ PASS\n" : "❌ FAIL\n";
if ($code !== 200) $allPass = false;

usleep(300000); // 再等 300ms

[$code, $resp] = sendGet($host, $port, '/metrics');
echo "Test 7  - /metrics endpoint:      HTTP {$code} ";
echo $code === 200 ? "✅ PASS\n" : "❌ FAIL (got {$code})\n";
if ($code !== 200) $allPass = false;

// ---------- 总结 ----------
echo "\n=== " . ($allPass ? "🎯 ALL PASSED — EdgeAuth working correctly" : "❌ SOME FAILED — review output above") . " ===\n";

exit($allPass ? 0 : 1);
