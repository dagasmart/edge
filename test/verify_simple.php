#!/usr/bin/env php
<?php
/**
 * EdgeAuth 功能验证脚本（与 server.php / hmac.php 字节级对齐版）
 *
 * 协议：
 *   password = b64Sign|timestamp|nonce
 *   b64Sign  = URL-safe base64(HMAC-SHA256($clientId|$username|$timestamp|$nonce, $secret))
 *   nonce    = 随机字符串，防重放
 *
 * 用法：php extensions/edge/test/verify_simple.php [base_url]
 *
 * 退出码：
 *   0 = 全部通过
 *   1 = 存在失败项
 */

error_reporting(E_ALL);
ini_set('display_errors', '1');

$baseUrl = $argv[1] ?? 'http://127.0.0.1:9503';

// ===================== 读取 secrets.php（统一文件名，复数） =====================

$secretFile = dirname(__DIR__) . '/config/secrets.php';
if (!is_file($secretFile)) {
    fwrite(STDERR, "❌ secrets.php 不存在: {$secretFile}\n");
    fwrite(STDERR, "   将仅使用内置 bench_secret 做基础验证\n");
    $secrets = ['bench_client:bench_user' => 'bench_secret'];
} else {
    $secrets = include $secretFile;
    echo "📦 从 secrets.php 加载了 " . count($secrets) . " 个设备密钥\n\n";
}

// ===================== 签名函数（与 hmac.php 字节级对齐） =====================

function sign(string $clientId, string $username, string $secret): string
{
    $timestamp = (string)time();
    $nonce = bin2hex(random_bytes(8)); // 16 hex chars, 防重放

    // ★ 跟 hmac.php 完全一致的 payload（竖线分隔，含 nonce）
    $payload = $clientId . '|' . $username . '|' . $timestamp . '|' . $nonce;

    // ★ HMAC-SHA256 原始二进制（第四个参数 true）
    $rawSign = hash_hmac('sha256', $payload, $secret, true);

    // ★ URL-safe base64（跟 hmac.php 的 strtr 反向对应：+/ → -_）
    $b64Sign = strtr(base64_encode($rawSign), '+/', '-_');

    // ★ password = b64Sign|timestamp|nonce
    return $b64Sign . '|' . $timestamp . '|' . $nonce;
}

// ===================== HTTP 工具 =====================

function httpPost(string $url, array $data, int $timeout = 5): array
{
    $ch = curl_init($url);
    curl_setopt_array($ch, [
        CURLOPT_POST           => true,
        CURLOPT_POSTFIELDS     => http_build_query($data),
        CURLOPT_RETURNTRANSFER => true,
        CURLOPT_TIMEOUT        => $timeout,
        CURLOPT_HTTPHEADER     => ['Content-Type: application/x-www-form-urlencoded'],
        CURLOPT_HEADER         => false,
    ]);
    $body = curl_exec($ch);
    $code = curl_getinfo($ch, CURLINFO_HTTP_CODE);
    $err  = curl_error($ch);
    curl_close($ch);

    if ($err) {
        return ['code' => 0, 'body' => "CURL ERROR: {$err}"];
    }
    return ['code' => $code, 'body' => trim($body)];
}

// ===================== 测试框架 =====================

$passed = 0;
$failed = 0;
$total  = 0;

function test(string $name, bool $condition, string $detail = ''): void
{
    global $passed, $failed, $total;
    $total++;
    if ($condition) {
        $passed++;
        echo "  ✅ {$name}\n";
    } else {
        $failed++;
        echo "  ❌ {$name}" . ($detail ? " — {$detail}" : "") . "\n";
    }
}

// ===================== 开始测试 =====================

echo "🎯 EdgeAuth Verify | Base: {$baseUrl}\n";
echo "   协议: b64Sign|timestamp|nonce | HMAC-SHA256 raw binary\n\n";

// Test 1: /health
echo "━━━ Test 1: /health ━━━\n";
$ch = curl_init($baseUrl . '/health');
curl_setopt_array($ch, [CURLOPT_RETURNTRANSFER => true, CURLOPT_TIMEOUT => 3]);
$body = curl_exec($ch);
$code = curl_getinfo($ch, CURLINFO_HTTP_CODE);
curl_close($ch);
test('/health 返回 200', $code === 200, "got {$code}");
test('/health 响应非空', !empty($body), 'empty body');
echo "\n";

// Test 2: /metrics
echo "━━━ Test 2: /metrics ━━━\n";
$ch = curl_init($baseUrl . '/metrics');
curl_setopt_array($ch, [CURLOPT_RETURNTRANSFER => true, CURLOPT_TIMEOUT => 3]);
$body = curl_exec($ch);
$code = curl_getinfo($ch, CURLINFO_HTTP_CODE);
curl_close($ch);
test('/metrics 返回 200', $code === 200, "got {$code}");
echo "\n";

// Test 3: 空字段
echo "━━━ Test 3: 空字段认证拒绝 ━━━\n";
$result = httpPost($baseUrl . '/mqtt/auth', ['clientid'=>'','username'=>'','password'=>'']);
test('空字段 → 401', $result['code'] === 401, "got {$result['code']}");
echo "\n";

// Test 4: 不存在设备
echo "━━━ Test 4: 不存在设备拒绝 ━━━\n";
$result = httpPost($baseUrl . '/mqtt/auth', [
    'clientid' => 'nonexistent_device_' . uniqid(),
    'username' => 'nobody',
    'password' => 'fakesig|' . time() . '|' . bin2hex(random_bytes(4)),
]);
test('不存在设备 → 401', $result['code'] === 401, "got {$result['code']}");
echo "\n";

// Test 5: 错误密钥
echo "━━━ Test 5: 错误密钥拒绝 ━━━\n";
$fakeTs = time();
$fakeNonce = bin2hex(random_bytes(8));
$fakePayload = 'device001|user001|' . $fakeTs . '|' . $fakeNonce;
$fakeB64 = strtr(base64_encode(hash_hmac('sha256', $fakePayload, 'wrong_secret', true)), '+/', '-_');
$result = httpPost($baseUrl . '/mqtt/auth', [
    'clientid' => 'device001', 'username' => 'user001',
    'password' => $fakeB64 . '|' . $fakeTs . '|' . $fakeNonce,
]);
test('错误密钥 → 401', $result['code'] === 401, "got {$result['code']}");
echo "\n";

// Test 6: 真实密钥
echo "━━━ Test 6: 真实设备密钥认证 ━━━\n";
echo "  （验证 " . count($secrets) . " 个设备）\n\n";

foreach ($secrets as $key => $secret) {
    [$clientId, $username] = explode(':', $key, 2);
    $password = sign($clientId, $username, $secret);

    $result = httpPost($baseUrl . '/mqtt/auth', [
        'clientid' => $clientId,
        'username' => $username,
        'password' => $password,
    ]);

    test(
        "{$key} → 200",
        $result['code'] === 200,
        "got {$result['code']}, body: " . substr($result['body'], 0, 80)
    );
}
echo "\n";

// Test 7: 过期时间戳
echo "━━━ Test 7: 过期时间戳拒绝 ━━━\n";
$firstKey = array_key_first($secrets);
$firstSecret = $secrets[$firstKey];
[$clientId, $username] = explode(':', $firstKey, 2);

$oldTimestamp = (string)(time() - 600); // 10 分钟前，超过 TIME_DRIFT(300)
$oldNonce = bin2hex(random_bytes(8));
$oldPayload = $clientId . '|' . $username . '|' . $oldTimestamp . '|' . $oldNonce;
$oldRawSign = hash_hmac('sha256', $oldPayload, $firstSecret, true);
$oldB64 = strtr(base64_encode($oldRawSign), '+/', '-_');
$oldPassword = $oldB64 . '|' . $oldTimestamp . '|' . $oldNonce;

$result = httpPost($baseUrl . '/mqtt/auth', [
    'clientid' => $clientId, 'username' => $username, 'password' => $oldPassword,
]);
test(
    '过期时间戳 → 401（时间窗口 ±300s）',
    $result['code'] === 401,
    "got {$result['code']}"
);

// ===================== 汇总 =====================

echo "\n━━━━━━━━━━━━━━━━━━━━━━━━━━\n";
echo "  总计: {$total} | ✅ {$passed} | ❌ {$failed}\n";

if ($failed === 0) {
    echo "  🎯 ALL PASSED\n";
    exit(0);
} else {
    echo "  ⚠️  {$failed} 个测试失败\n";
    exit(1);
}
