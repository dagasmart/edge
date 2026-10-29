#!/usr/bin/env php
<?php
declare(strict_types=1);

/**
 * EdgeAuth Redis 密钥种子工具
 *
 * 密钥来源：脚本内置数组（与 DB edge_authorize.secret 保持一致）
 * secrets.php 已废弃，不再作为密钥来源。
 *
 * 用法：
 *   php seed_redis.php                        # 正常写入（已存在则跳过）
 *   php seed_redis.php --dry-run              # 只打印，不实际写入
 *   php seed_redis.php --force                # 强制覆盖已存在的密钥
 *   php seed_redis.php --host 127.0.0.1 --port 16379 --db 2 --password xxx
 *   php seed_redis.php --timeout 5.0          # 自定义 Redis 连接超时（秒，支持小数）
 *
 * Redis key 格式：device:secret:{clientid}:{username}   ← 冒号，不替换
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
    'timeout'   => 2.0,   // Redis 连接超时（秒，支持小数）
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
        case '--timeout':
            $options['timeout'] = (float)($argv[++$i] ?? 2.0);
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
  --timeout <sec>   Redis 连接超时秒数，支持小数（默认 2.0）
  -h, --help        显示此帮助

密钥来源: 脚本内置（与 DB edge_authorize.secret 保持一致）
示例:
  php seed_redis.php --dry-run
  php seed_redis.php --force --host 10.0.0.5 --port 6379
  php seed_redis.php --timeout 5.0

HELP;
    exit(0);
}

// ===================== 密钥定义（内置，不依赖任何文件） =====================
// ★ 格式: 'device_id:username' => 'secret'
// ★ 必须与 DB edge_authorize 表的 secret 字段保持一致
// ★ 如需外部化，改为: $secrets = json_decode(shell_exec('php artisan edge:dump-secrets --json'), true);

$secrets = [
    'device001:user001'        => 'a8f3c9e2m7x1p5q8w2e4r6t9y',
    'device002:user001'        => 'b2d4f6h8j0k1l3m5n7p8q9r0s',
    'device003:user002'        => 'c3e5g7i9k2m4o6p8q0s1t2u3v',
    'gateway01:admin'          => 'd4f6h8j0l2n4p6q8r0t1u3w5y',
    'bench_client:bench_user'  => 'z9x8c7v6b5n4m3l2k1j0h9g8f7', // ← 已补到 25 bytes
];

if (empty($secrets)) {
    fwrite(STDERR, "❌ 内置密钥数组为空，脚本异常\n");
    exit(1);
}

// ===================== 连接 Redis =====================

echo "🔌 连接 Redis {$options['host']}:{$options['port']} db={$options['db']} (timeout={$options['timeout']}s) ...\n";

$redis = new Redis();
try {
    $redis->connect($options['host'], $options['port'], $options['timeout']);
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
    // ★ 关键：Redis key 保留冒号，与 hmac.php / EdgeAuthorizeService 查询格式一致
    $redisKey = "device:secret:{$clientId}:{$username}";

    // 检查是否已存在
    $exists = $redis->exists($redisKey);
    $existingValue = $exists ? $redis->get($redisKey) : null;

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

    // dry-run 或已存在时打印详情
    if (($options['dry-run'] || ($exists && !$options['force'])) && $existingValue !== null) {
        echo "     当前值: {$existingValue}\n";
        echo "     新值:   {$secret}\n";
    }
}

// ===================== 验证（仅非 dry-run） =====================

if (!$options['dry-run']) {
    echo "\n━━━ 验证写入结果 ━━━\n";
    foreach ($secrets as $key => $secret) {
        [$clientId, $username] = explode(':', $key, 2);
        $redisKey = "device:secret:{$clientId}:{$username}";
        $actual = $redis->get($redisKey);
        if ($actual === $secret) {
            echo "  ✅ {$key} = 正确\n";
        } else {
            echo "  ❌ {$key} = 不匹配 (got: " . ($actual ?? 'nil') . ")\n";
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
