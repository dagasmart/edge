<?php
// bootstrap.php —— EdgeAuth 配置加载器
//
// 加载优先级（从高到低）：
//   1. 数据库 mcs_edge_gateway_config（gateway 专属 > 全局）
//   2. 环境变量 / .env
//   3. 代码默认值
//
// 敏感字段（如 REDIS_PASSWORD）在数据库中加密存储，Model 访问器自动解密；
// 但本文件在 Laravel 完全启动前执行，无法使用 Model，因此手动解密。

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
        $v = trim(trim($v), '"\'');
        putenv("{$k}={$v}");
        $_ENV[$k] = $v;
        $_SERVER[$k] = $v;
    }
}

// ---------- 2. 配置声明（key => [默认值, 类型]） ----------
$configs = [
    // 网络
    'AUTH_PORT'                 => [9503,  'int'],
    'BACKLOG'                   => [4096,  'int'],

    // Redis
    'REDIS_HOST'                => ['127.0.0.1', 'string'],
    'REDIS_PORT'                => [16379, 'int'],
    'REDIS_PASSWORD'            => ['uApDVQD7', 'string'],
    'REDIS_DB'                  => [2,     'int'],
    'REDIS_POOL_SIZE'           => [512,   'int'],

    // 并发
    'MAX_CONCURRENCY'           => [20000, 'int'],
    'MAX_CONCURRENCY_PER_DEVICE'=> [5,     'int'],

    // 缓存
    'CACHE_TTL'                 => [300, 'int'],
    'LOCAL_CACHE_TTL'           => [60,  'int'],

    // 限流
    'RATE_MAX'                  => [20, 'int'],
    'RATE_TTL'                  => [60, 'int'],

    // 安全
    'TIME_DRIFT'                => [300, 'int'],

    // 路由
    'AUTH_HTTP_PREFIX'          => ['/admin-api/extension/edge', 'string'],

    // 调试
    'DEBUG'                     => ['false', 'bool'],
];

// 需要解密的键（与 Model::$encryptedKeys 保持一致）
$encryptedKeys = ['REDIS_PASSWORD'];

// 当前网关实例 ID（容器/进程级指定，用于差异化配置）
$currentGatewayId = getenv('GATEWAY_ID') ?: null;
if ($currentGatewayId !== null) {
    $currentGatewayId = (int) $currentGatewayId;
}

// ---------- 3. 从数据库加载（Laravel 可用时） ----------
$dbValues = [];
$dbAvailable = false;
$dbSchema = 'master';


try {
    if (class_exists(\Illuminate\Support\Facades\DB::class)) {
        $rows = \Illuminate\Support\Facades\DB::connection($dbSchema)
            ->table('mcs_edge_gateway_config')
            ->where('is_active', true)
            ->orderByRaw('gateway_id IS NULL ASC') // 全局在前，专属覆盖
            ->get();

        foreach ($rows as $row) {
            // 若指定了网关，跳过其他网关的专属配置
            if ($currentGatewayId !== null
                && $row->gateway_id !== null
                && (int) $row->gateway_id !== $currentGatewayId) {
                continue;
            }

            $key = $row->key;
            // 专属配置（非 NULL）覆盖全局配置（NULL）
            if (!isset($dbValues[$key]) || $row->gateway_id !== null) {
                $dbValues[$key] = $row;
            }
        }

        $dbAvailable = true;
    }
} catch (\Throwable $e) {
    // DB 不可用时静默回退到 .env / 默认值，保证服务可启动
    fwrite(STDERR, "[bootstrap] DB config unavailable, fallback to env: {$e->getMessage()}\n");
}

// ---------- 4. 定义常量 ----------
foreach ($configs as $key => [$default, $type]) {
    if (defined($key)) {
        continue; // 已被其他方式定义，跳过
    }

    if (isset($dbValues[$key])) {
        $row   = $dbValues[$key];
        $value = $row->value;
        $type  = $row->type ?? $type;

        // 手动解密（bootstrap 阶段无 Model 实例）
        if (in_array($key, $encryptedKeys, true) && is_string($value) && str_starts_with($value, 'eyJ')) {
            try {
                if (class_exists(\Illuminate\Support\Facades\Crypt::class)) {
                    $value = \Illuminate\Support\Facades\Crypt::decryptString($value);
                }
            } catch (\Throwable $e) {
                // 解密失败，保留原始值（兼容明文/旧数据）
                fwrite(STDERR, "[bootstrap] decrypt {$key} failed: {$e->getMessage()}\n");
            }
        }
    } elseif (($env = getenv($key)) !== false) {
        $value = $env;
    } else {
        $value = $default;
    }

    // 类型转换
    if ($type === 'int') {
        $value = (int) $value;
    } elseif ($type === 'bool') {
        $value = in_array(strtolower((string)$value), ['true', '1', 'on', 'yes'], true);
    } else {
        $value = (string) $value;
    }

    define($key, $value);
}

// ---------- 5. 强制校验 ----------
$required = array_keys($configs);
foreach ($required as $c) {
    if (!defined($c)) {
        fwrite(STDERR, "[bootstrap] FATAL: constant {$c} not defined\n");
        exit(1);
    }
}

unset($configs, $encryptedKeys, $currentGatewayId, $dbValues, $dbAvailable, $envFile, $lines, $line, $k, $v, $rows, $row, $key, $default, $type, $value, $env, $required);
