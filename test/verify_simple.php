#!/usr/bin/env php
<?php
declare(strict_types=1);

/**
 * EdgeAuth 功能验证脚本（Redis 密钥源，与 seed_redis.php 对齐）
 *
 * Key 格式：device:secret:{clientId}:{username}
 * 协议：password = b64Sign|timestamp|nonce
 *
 * 用法：php extensions/edge/test/verify_simple.php [base_url] [--timeout <sec>]
 *
 *   base_url   网关基地址（默认 http://127.0.0.1:9503/admin-api/extension/edge/gateway）
 *   --timeout  统一超时秒数，同时作用于 Redis 连接与 cURL（默认 3.0）
 *
 * 示例：
 *   php verify_simple.php
 *   php verify_simple.php http://127.0.0.1:9503/admin-api/extension/edge/gateway --timeout 5.0
 */
error_reporting(E_ALL);
ini_set('display_errors', '1');

// ---------- 参数解析（位置参数 + 命名参数） ----------

$options = [
    'base-url' => 'http://127.0.0.1:9503/admin-api/extension/edge/gateway',
    'timeout'  => 3.0,
];

// 第一个非命名参数视为 base_url
for ($i = 1; $i < $argc; $i++) {
    if ($argv[$i] === '--timeout') {
        $options['timeout'] = (float)($argv[++$i] ?? 3.0);
    } elseif (strpos($argv[$i], '--') !== 0) {
        // 第一个裸参数 = base_url
        $options['base-url'] = $argv[$i];
    }
}

$baseUrl = $options['base-url'];
$timeout = $options['timeout'];

// ---------- Redis 配置（与 seed_redis.php / server.php 一致） ----------
const REDIS_HOST = '127.0.0.1';
const REDIS_PORT = 16379;
const REDIS_PASSWORD = 'uApDVQD7';
const REDIS_DB = 2;
const SECRET_PREFIX = 'device:secret:'; // ← 跟 seed_redis.php 对齐

// ---------- 工具函数 ----------
function httpPost(string $url, array $fields, float $timeout): array
{
    $ch = curl_init($url);
    $json = json_encode($fields);
    $t = (int) ceil($timeout);
    curl_setopt_array($ch, [
        CURLOPT_POST => true,
        CURLOPT_POSTFIELDS => $json,
        CURLOPT_RETURNTRANSFER => true,
        CURLOPT_TIMEOUT => $t,
        CURLOPT_CONNECTTIMEOUT => $t,
        CURLOPT_HTTPHEADER => ['Accept: application/json', 'Content-Type: application/json'],
    ]);
    $body = curl_exec($ch);
    $code = curl_getinfo($ch, CURLINFO_HTTP_CODE);
    $err = curl_error($ch);
    curl_close($ch);

    if ($err || $body === false) {
        return ['code' => 0, 'body' => "CURL ERROR: {$err}"];
    }

    return ['code' => $code, 'body' => trim($body)];
}

function httpGet(string $url, float $timeout): array
{
    $ch = curl_init($url);
    $t = (int) ceil($timeout);
    curl_setopt_array($ch, [
        CURLOPT_RETURNTRANSFER => true,
        CURLOPT_TIMEOUT => $t,
        CURLOPT_CONNECTTIMEOUT => $t,
        CURLOPT_HTTPHEADER => ['Accept: application/json'],
    ]);
    $body = curl_exec($ch);
    $code = curl_getinfo($ch, CURLINFO_HTTP_CODE);
    $err = curl_error($ch);
    curl_close($ch);

    if ($err || $body === false) {
        return ['code' => 0, 'body' => "CURL ERROR: {$err}"];
    }

    return ['code' => $code, 'body' => trim($body)];
}

function loadSecretsFromRedis(float $timeout): array
{
    $redis = new Redis;
    try {
        $redis->connect(REDIS_HOST, REDIS_PORT, $timeout);
        $redis->auth(REDIS_PASSWORD);
        $redis->select(REDIS_DB);
        $redis->ping();
    } catch (Exception $e) {
        fwrite(STDERR, '❌ Redis 连接失败: '.$e->getMessage().PHP_EOL);
        exit(1);
    }

    // 用 seed_redis.php 的 key 格式扫描
    $keys = $redis->keys(SECRET_PREFIX.'*');
    $devices = [];

    foreach ($keys as $fullKey) {
        // device:secret:clientId:username → 提取 clientId 和 username
        $suffix = substr($fullKey, strlen(SECRET_PREFIX));
        $parts = explode(':', $suffix, 2);
        if (count($parts) === 2) {
            [$clientId, $username] = $parts;
            $secret = $redis->get($fullKey);
            if ($secret !== false) {
                // 存成 clientId:username => secret
                $devices[$clientId.':'.$username] = $secret;
            }
        }
    }

    $redis->close();

    return $devices;
}

function buildPassword(string $clientId, string $username, string $secret): string
{
    $timestamp = (string) time();
    $nonce = bin2hex(random_bytes(8));
    $payload = $clientId.'|'.$username.'|'.$timestamp.'|'.$nonce;
    $rawSign = hash_hmac('sha256', $payload, $secret, true);
    $b64Sign = rtrim(strtr(base64_encode($rawSign), '+/', '-_'), '=');

    return $b64Sign.'|'.$timestamp.'|'.$nonce;
}

// ---------- 主流程 ----------
echo '📦 从 Redis db='.REDIS_DB.' 加载设备密钥（key 格式: '.SECRET_PREFIX."*）\n\n";

$secrets = loadSecretsFromRedis($timeout);

if (empty($secrets)) {
    echo '❌ Redis 里没有找到任何 '.SECRET_PREFIX."* 密钥\n";
    echo "   请先运行: php extensions/edge/test/seed_redis.php\n";
    exit(1);
}

echo "🎯 EdgeAuth Verify | Base: {$baseUrl}\n";
echo "   协议: b64Sign|timestamp|nonce | HMAC-SHA256\n";
echo '   加载了 '.count($secrets)." 个设备密钥 | 超时: {$timeout}s\n\n";

$passed = 0;
$failed = 0;
$total = 0;

function test(string $name, bool $condition, string $detail = ''): void
{
    global $passed, $failed, $total;
    $total++;
    if ($condition) {
        $passed++;
        echo "  ✅ {$name}\n";
    } else {
        $failed++;
        echo "  ❌ {$name}".($detail ? " — {$detail}" : '')."\n";
    }
}

// ---------- Test 1: /health ----------
echo "━━━ Test 1: /health ━━━\n";
$res = httpGet($baseUrl.'/health', $timeout);
test('/health 返回 200', $res['code'] === 200, "got {$res['code']}");
test('/health 响应非空', ! empty($res['body']), 'empty body');
echo "\n";

// ---------- Test 2: /metrics ----------
echo "━━━ Test 2: /metrics ━━━\n";
$res = httpGet($baseUrl.'/metrics', $timeout);
test('/metrics 返回 200', $res['code'] === 200, "got {$res['code']}");
echo "\n";

// ---------- Test 3: 空字段 ----------
echo "━━━ Test 3: 空字段认证拒绝 ━━━\n";
$res = httpPost($baseUrl.'/auth', ['clientid' => '', 'username' => '', 'password' => ''], $timeout);
test('空字段 → 401', $res['code'] === 401, "got {$res['code']}");
echo "\n";

// ---------- Test 4: 不存在设备 ----------
echo "━━━ Test 4: 不存在设备拒绝 ━━━\n";
$fakeId = 'nonexistent_'.uniqid();
$res = httpPost($baseUrl.'/auth', [
    'clientid' => $fakeId, 'username' => 'nobody',
    'password' => 'fakesig|'.time().'|'.bin2hex(random_bytes(4)),
], $timeout);
test('不存在设备 → 401', $res['code'] === 401, "got {$res['code']}");
echo "\n";

// ---------- Test 5: 错误密钥 ----------
echo "━━━ Test 5: 错误密钥拒绝 ━━━\n";
$firstKey = array_key_first($secrets);
[$clientId, $username] = explode(':', $firstKey, 2);
$fakeTs = time();
$fakeNonce = bin2hex(random_bytes(8));
$fakePayload = $clientId.'|'.$username.'|'.$fakeTs.'|'.$fakeNonce;
$fakeB64 = rtrim(strtr(base64_encode(hash_hmac('sha256', $fakePayload, 'wrong_secret', true)), '+/', '-_'), '=');
$res = httpPost($baseUrl.'/auth', [
    'clientid' => $clientId, 'username' => $username,
    'password' => $fakeB64.'|'.$fakeTs.'|'.$fakeNonce,
], $timeout);
test('错误密钥 → 401', $res['code'] === 401, "got {$res['code']}");
echo "\n";

// ---------- Test 6: 真实设备密钥认证 ----------
echo "━━━ Test 6: 真实设备密钥认证 ━━━\n";
echo '  （验证 '.count($secrets)." 个设备）\n\n";

foreach ($secrets as $key => $secret) {
    [$clientId, $username] = explode(':', $key, 2);
    $password = buildPassword($clientId, $username, $secret);
    $res = httpPost($baseUrl.'/auth', [
        'clientid' => $clientId, 'username' => $username, 'password' => $password,
    ], $timeout);
    test("{$key} → 200", $res['code'] === 200,
        "got {$res['code']}, body: ".substr($res['body'], 0, 80));
}
echo "\n";

// ---------- Test 7: 过期时间戳 ----------
echo "━━━ Test 7: 过期时间戳拒绝 ━━━\n";
$firstKey = array_key_first($secrets);
$firstSecret = $secrets[$firstKey];
[$clientId, $username] = explode(':', $firstKey, 2);

$oldTs = (string) (time() - 600);
$oldNonce = bin2hex(random_bytes(8));
$oldPayload = $clientId.'|'.$username.'|'.$oldTs.'|'.$oldNonce;
$oldRaw = hash_hmac('sha256', $oldPayload, $firstSecret, true);
$oldB64 = rtrim(strtr(base64_encode($oldRaw), '+/', '-_'), '=');
$oldPwd = $oldB64.'|'.$oldTs.'|'.$oldNonce;

$res = httpPost($baseUrl.'/auth', [
    'clientid' => $clientId, 'username' => $username, 'password' => $oldPwd,
], $timeout);
test('过期时间戳 → 401（时间窗口 ±300s）', $res['code'] === 401, "got {$res['code']}");
echo "\n";

// ---------- 汇总 ----------
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━\n";
echo "  总计: {$total} | ✅ {$passed} | ❌ {$failed}\n";
if ($failed === 0) {
    echo "  🎯 ALL PASSED\n";
    exit(0);
} else {
    echo "  ⚠️  {$failed} 个测试失败\n";
    echo "  💡 确认: seed_redis.php 已写入 + Swow 已重启 + hmac.php 的 redisGet 用 device:secret:{clientId}:{username} 格式\n";
    exit(1);
}
