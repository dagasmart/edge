#!/usr/bin/env php
<?php
/**
 * EdgeAuth Redis 密钥种子工具
 *
 * 用法：
 *   php seed_redis.php                        # 正常写入（已存在则跳过）
 *   php seed_redis.php --dry-run              # 只打印，不实际写入
 *   php seed_redis.php --force                # 强制覆盖已存在的密钥
 *   php seed_redis.php --host 127.0.0.1 --port 16379 --db 2 --password xxx
 *
 * 退出码：
 *   0 = 全部成功
 *   1 = 参数错误 / 连接失败
 *   2 = 部分写入失败
 */

error_reporting(E_ALL);
ini_set('display_errors', '1');

// ===================== 参数解析 =====================

$options = [
    'dry-run'   => false,
    'force'     => false,
    'host'      => '127.0.0.1',
    'port'      => 16379,
    'password'  => 'uApDVQD7',
    'db'        => 2,
    'help'      => false,
];

for ($i = 1; $i < $argc; $i++) {
    $arg = $argv[$i];
    switch ($arg) {
        case '--dry-run':
            $options['dry-run'] = true;
            break;
        case '--force':
            $options['force'] = true;
            break;
        case '--host':
            $options['host'] = $argv[++$i] ?? $options['host'];
            break;
        case '--port':
            $options['port'] = (int)($argv[++$i] ?? $options['port']);
            break;
        case '--password':
            $options['password'] = $argv[++$i] ?? $options['password'];
            break;
        case '--db':
            $options['db'] = (int)($argv[++$i] ?? $options['db']);
            break;
        case '-h':
        case '--help':
            $options['help'] = true;
            break;
    }
}

if ($options['help']) {
    echo <<<HELP
EdgeAuth Redis 密钥种子工具

用法: php seed_redis.php [选项]

选项:
  --dry-run         只打印将要执行的操作，不实际写入
  --force           强制覆盖已存在的密钥（默认跳过已存在的）
  --host <ip>       Redis 主机（默认 127.0.0.1）
  --port <port>     Redis 端口（默认 16379）
  --password <pwd>  Redis 密码（默认 uApDVQD7）
  --db <num>        Redis 数据库（默认 2）
  -h, --help        显示此帮助

示例:
  php seed_redis.php --dry-run
  php seed_redis.php --force --host 10.0.0.5 --port 6379

HELP;
    exit(0);
}

// ===================== 读取 secret.php =====================

$secretFile = dirname(__DIR__) . '/config/secret.php';
if (!is_file($secretFile)) {
    fwrite(STDERR, "❌ secret.php 不存在: {$secretFile}\n");
    exit(1);
}

$secrets = include $secretFile;
if (!is_array($secrets) || empty($secrets)) {
    fwrite(STDERR, "❌ secret.php 返回空或格式错误\n");
    exit(1);
}

// ===================== 连接 Redis =====================

echo "🔌 连接 Redis {$options['host']}:{$options['port']} db={$options['db']} ...\n";

$redis = new Redis();
try {
    $redis->connect($options['host'], $options['port'], 2);
    if ($options['password'] !== '') {
        $redis->auth($options['password']);
    }
    $redis->select($options['db']);
    // 探活
    $redis->ping();
} catch (RedisException $e) {
    fwrite(STDERR, "❌ Redis 连接失败: " . $e->getMessage() . "\n");
    exit(1);
}

echo "✅ Redis 连接成功\n\n";

// ===================== 执行 seed =====================

$mode = $options['dry-run'] ? 'DRY-RUN' : ($options['force'] ? 'FORCE' : 'NORMAL');
echo "📋 模式: {$mode} | 设备数: " . count($secrets) . "\n\n";

$ok = 0;
$skipped = 0;
$failed = 0;

foreach ($secrets as $key => $secret) {
    [$clientId, $username] = explode(':', $key, 2);
    $redisKey = "device:secret:{$clientId}:{$username}";

    // 检查是否已存在
    $exists = $redis->exists($redisKey);
    $existingValue = $exists ? $redis->get($redisKey) : null;

    // 格式化输出
    $status = '';
    $action = '';

    if ($exists && !$options['force']) {
        // 已存在且非 force → 跳过
        if ($existingValue === $secret) {
            $status = '⏭️  跳过（已存在，值一致）';
            $skipped++;
        } else {
            $status = '⚠️  跳过（已存在，值不同！用 --force 覆盖）';
            $skipped++;
        }
        $action = 'SKIP';
    } elseif ($options['dry-run']) {
        $status = "🔍 将写入: {$redisKey} = {$secret}";
        $action = 'DRY';
    } else {
        // 实际写入
        try {
            $redis->set($redisKey, $secret);
            $status = "✅ SET {$redisKey} = {$secret}";
            if ($exists) {
                $status .= ' (覆盖)';
            }
            $ok++;
            $action = 'SET';
        } catch (RedisException $e) {
            $status = "❌ 写入失败: " . $e->getMessage();
            $failed++;
            $action = 'FAIL';
        }
    }

    printf("  %-50s %s\n", $key, $status);

    // dry-run 或正常模式都打印详情
    if (($options['dry-run'] || ($exists && !$options['force'])) && $existingValue !== null) {
        echo "     当前值: {$existingValue}\n";
        echo "     新值:   {$secret}\n";
    }
}

// ===================== 验证（仅非 dry-run） =====================

if (!$options['dry-run']) {
    echo "\n━━━ 验证写入结果 ━━━\n";
    $allGood = true;
    foreach ($secrets as $key => $secret) {
        [$clientId, $username] = explode(':', $key, 2);
        $redisKey = "device:secret:{$clientId}:{$username}";
        $actual = $redis->get($redisKey);
        if ($actual === $secret) {
            echo "  ✅ {$key} = 正确\n";
        } else {
            echo "  ❌ {$key} = 不匹配 (got: " . ($actual ?? 'nil') . ")\n";
            $allGood = false;
            $failed++;
            $ok = max(0, $ok - 1);
        }
    }
}

// ===================== 汇总 =====================

echo "\n━━━━━━━━━━━━━━━━━━━━━━━━━━\n";
echo "  总计: " . count($secrets) . " | ✅ {$ok} | ⏭️ {$skipped} | ❌ {$failed}\n";

if ($options['dry-run']) {
    echo "  🔍 DRY-RUN 完成，未实际写入任何数据\n";
    exit(0);
}

if ($failed === 0) {
    echo "  🎯 SEED 完成\n";

    // 打印密钥长度校验
    echo "\n  📏 密钥长度校验:\n";
    foreach ($secrets as $key => $secret) {
        $len = strlen($secret);
        $flag = $len >= 16 ? '✅' : '⚠️';
        echo "     {$flag} {$key}: {$len} bytes\n";
    }

    exit(0);
} else {
    echo "  ⚠️  部分操作失败\n";
    exit(2);
}
