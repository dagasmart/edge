<?php
// bootstrap.php

// ---------- 1. 加载 .env ----------
$envFile = __DIR__ . '/../.env';
if (file_exists($envFile)) {
    $lines = file($envFile, FILE_IGNORE_NEW_LINES | FILE_SKIP_EMPTY_LINES);
    foreach ($lines as $line) {
        $line = trim($line);
        if ($line === '' || str_starts_with($line, '#') || !str_contains($line, '=')) {
            continue;
        }
        [$k, $v] = explode('=', $line, 2);
        $k = trim($k);
        $v = trim(trim($v), '"\''); // 去掉首尾空格和引号
        putenv("$k=$v");
        $_ENV[$k] = $v;
        $_SERVER[$k] = $v;
    }
}

// ---------- 2. 所有配置集中声明（与 .env 完全对齐） ----------
$configs = [
    // 网络
    'AUTH_PORT'                 => 9503,
    'BACKLOG'                   => 4096,

    // Redis
    'REDIS_HOST'                => '127.0.0.1',
    'REDIS_PORT'                => 16379,
    'REDIS_PASSWORD'            => 'uApDVQD7',
    'REDIS_DB'                  => 2,
    'REDIS_POOL_SIZE'           => 512,

    // 并发
    'MAX_CONCURRENCY'           => 20000,
    'MAX_CONCURRENCY_PER_DEVICE'=> 5,

    // 缓存
    'CACHE_TTL'                 => 300,
    'LOCAL_CACHE_TTL'           => 60,

    // 限流
    'RATE_MAX'                  => 20,
    'RATE_TTL'                 => 60,

    // 安全
    'TIME_DRIFT'                => 300,

    // 路由（← 之前漏掉的）
    'AUTH_HTTP_PREFIX'          => '/admin-api/extension/edge/gateway',

    // 调试
    'DEBUG'                     => 'false',
];

foreach ($configs as $key => $default) {
    $raw = getenv($key);
    $value = $raw !== false ? $raw : $default;

    // 类型转换
    if (is_int($default)) {
        $value = (int) $value;
    } elseif (is_bool($default)) {
        $value = in_array(strtolower((string)$value), ['true', '1', 'on', 'yes'], true);
    } else {
        $value = (string) $value;
    }

    if (!defined($key)) {
        define($key, $value);
    }
}

// ---------- 3. 强制校验（与 $configs 完全一致） ----------
$required = array_keys($configs);
foreach ($required as $c) {
    if (!defined($c)) {
        fwrite(STDERR, "[bootstrap] FATAL: constant {$c} not defined\n");
        exit(1);
    }
}
