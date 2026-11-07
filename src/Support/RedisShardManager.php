<?php

declare(strict_types=1);

namespace DagaSmart\Edge\Support;

use Swow\Channel;
use Swow\Coroutine;

/**
 * EdgeAuth Redis 分片连接池管理器
 *
 * 路由规则：
 * - edge:metrics:* / edge:config:* / edge:global:* → shard-0
 * - edge:device:active:* → shard-0（在线统计 key，集中存储便于 SCAN）
 * - device:secret:{clientId}:{username} → 按 clientId
 * - auth:online:{username} → 按 username
 * - fail:{clientId} / block:{clientId} / rate:{clientId} → 按 clientId
 * - device:secret:{username}（旧格式兜底）→ 按 username
 * - 无法提取路由因子 → shard-0
 *
 * 未配置 EDGE_REDIS_SHARDS → 单实例模式，从常量读取连接参数
 *
 * 使用方式：
 *   RedisShardManager::init($poolSize, $db);
 *   $conn = RedisShardManager::pop($key);
 *   RedisShardManager::push($key, $conn);
 *   $stats = RedisShardManager::stats();
 *   $ready = RedisShardManager::isReady();
 */
class RedisShardManager
{
    /** @var Channel[]|null */
    private static ?array $pools = null;

    /** @var array<int, array{host:string,port:int,password:string,db:int}>|null */
    private static ?array $configs = null;

    private static int $shardCount = 0;
    private static int $poolSize = 256;
    private static int $defaultDb = 2;
    private static bool $ready = false;

    /** @var int|null 记录 init 时的 pid，fork 后子进程 pid 变了自动重 init */
    private static ?int $initPid = null;

    /** @var array<int,int> 后台填充连续失败计数（熔断用） */
    private static array $fillFailures = [];

    public static function init(int $poolSize = 256, int $db = 2): void
    {
        $currentPid = getmypid();

        // 同进程已初始化 → 直接返回
        if (self::$ready && self::$initPid === $currentPid) {
            return;
        }

        // fork 后子进程 pid 变了 → 重置状态，重新初始化
        if (self::$ready && self::$initPid !== null && self::$initPid !== $currentPid) {
            self::$pools = null;
            self::$configs = null;
            self::$shardCount = 0;
            self::$ready = false;
            self::$fillFailures = [];
        }

        self::$initPid = $currentPid;
        self::$poolSize = $poolSize;
        self::$defaultDb = $db;
        self::$fillFailures = [];

        $shards = self::parseShardList();

        // ---- 单实例退化模式 ----
        if ($shards === []) {
            $host = defined('REDIS_HOST') ? REDIS_HOST : '127.0.0.1';
            $port = defined('REDIS_PORT') ? (int) REDIS_PORT : 6379;
            $password = defined('REDIS_PASSWORD') ? REDIS_PASSWORD : '';
            $actualDb = defined('REDIS_DB') ? (int) REDIS_DB : $db;

            self::$shardCount = 0;
            self::$configs = [0 => [
                'host'     => $host,
                'port'     => $port,
                'password' => $password,
                'db'       => $actualDb,
            ]];

            $pool = new Channel($poolSize);
            $probeSize = min(3, $poolSize);
            $ok = 0;

            for ($i = 0; $i < $probeSize; $i++) {
                try {
                    $redis = new \Redis;
                    $redis->connect($host, $port, 2.0);
                    if ($password !== '') {
                        $authOk = $redis->auth($password);
                        if (! $authOk) {
                            fwrite(STDERR, sprintf(
                                '[EdgeAuth] Redis AUTH FAILED (%s:%d, password length=%d)' . PHP_EOL,
                                $host, $port, strlen($password)
                            ));
                            continue;
                        }
                    }
                    if ($actualDb > 0) {
                        $selectOk = $redis->select($actualDb);
                        if (! $selectOk) {
                            fwrite(STDERR, sprintf(
                                '[EdgeAuth] Redis SELECT DB %d FAILED' . PHP_EOL,
                                $actualDb
                            ));
                            continue;
                        }
                    }
                    $pool->push($redis);
                    $ok++;
                } catch (\Throwable $e) {
                    fwrite(STDERR, sprintf(
                        '[EdgeAuth] Redis probe %d failed: %s' . PHP_EOL,
                        $i, $e->getMessage()
                    ));
                }
            }

            fwrite(STDOUT, sprintf(
                '[EdgeAuth] Redis single-mode probed: %d/%d (%s:%d/%d)' . PHP_EOL,
                $ok, $probeSize, $host, $port, $actualDb
            ));

            // 后台渐进填充
            if ($ok > 0 && $poolSize > $probeSize) {
                $remaining = $poolSize - $probeSize;
                $fillHost = $host;
                $fillPort = $port;
                $fillPassword = $password;
                $fillDb = $actualDb;

                Coroutine::run(static function () use ($remaining, $fillHost, $fillPort, $fillPassword, $fillDb): void {
                    $filled = 0;
                    $maxFailures = 50;

                    for ($i = 0; $i < $remaining; $i++) {
                        $failures = self::$fillFailures[0] ?? 0;
                        if ($failures >= $maxFailures) {
                            fwrite(STDERR, sprintf(
                                '[EdgeAuth] single-mode background fill STOPPED (consecutive failures=%d)' . PHP_EOL,
                                $failures
                            ));
                            break;
                        }

                        try {
                            $redis = new \Redis;
                            $redis->connect($fillHost, $fillPort, 2.0);
                            if ($fillPassword !== '') {
                                $redis->auth($fillPassword);
                            }
                            if ($fillDb > 0) {
                                $redis->select($fillDb);
                            }
                            // 单实例模式下 pools[0] 一定存在
                            if (isset(self::$pools[0])) {
                                self::$pools[0]->push($redis);
                            }
                            self::$fillFailures[0] = 0;
                            $filled++;
                        } catch (\Throwable $e) {
                            self::$fillFailures[0] = $failures + 1;
                            fwrite(STDERR, sprintf(
                                '[EdgeAuth] single-mode background fill failed: %s (consecutive=%d)' . PHP_EOL,
                                $e->getMessage(), $failures + 1
                            ));
                        }
                        usleep(10000);
                    }

                    fwrite(STDOUT, sprintf(
                        '[EdgeAuth] Redis single-mode background filled: %d/%d' . PHP_EOL,
                        $filled, $remaining
                    ));
                });
            }

            self::$pools = [0 => $pool];
            self::$ready = true;

            if ($ok === 0) {
                fwrite(STDERR, '[EdgeAuth] Redis single-mode: ALL PROBES FAILED, pool is empty!' . PHP_EOL);
            }

            return;
        }

        // ---- 分片模式 ----
        self::$configs = $shards;
        self::$shardCount = count($shards);
        self::$pools = [];

        foreach ($shards as $index => $cfg) {
            $pool = new Channel($poolSize);
            $probeSize = min(3, $poolSize);
            $ok = 0;

            for ($i = 0; $i < $probeSize; $i++) {
                try {
                    $redis = new \Redis;
                    $redis->connect($cfg['host'], $cfg['port'], 2.0);
                    if ($cfg['password'] !== '') {
                        $redis->auth($cfg['password']);
                    }
                    if ($cfg['db'] > 0) {
                        $redis->select($cfg['db']);
                    }
                    $pool->push($redis);
                    $ok++;
                } catch (\Throwable $e) {
                    fwrite(STDERR, sprintf(
                        '[EdgeAuth] shard=%d probe %d failed: %s' . PHP_EOL,
                        $index, $i, $e->getMessage()
                    ));
                }
            }

            fwrite(STDOUT, sprintf(
                '[EdgeAuth] Redis shard=%d probed=%d/%d (%s:%d/%d)' . PHP_EOL,
                $index, $ok, $probeSize, $cfg['host'], $cfg['port'], $cfg['db']
            ));

            // 后台渐进填充
            if ($ok > 0 && $poolSize > $probeSize) {
                $remaining = $poolSize - $probeSize;
                $shardIndex = $index;
                $shardCfg = $cfg;

                Coroutine::run(static function () use ($remaining, $pool, $shardCfg, $shardIndex): void {
                    $filled = 0;
                    $maxFailures = 50;

                    for ($i = 0; $i < $remaining; $i++) {
                        $failures = self::$fillFailures[$shardIndex] ?? 0;
                        if ($failures >= $maxFailures) {
                            fwrite(STDERR, sprintf(
                                '[EdgeAuth] shard=%d background fill STOPPED (consecutive failures=%d)' . PHP_EOL,
                                $shardIndex, $failures
                            ));
                            break;
                        }

                        try {
                            $redis = new \Redis;
                            $redis->connect($shardCfg['host'], $shardCfg['port'], 2.0);
                            if ($shardCfg['password'] !== '') {
                                $redis->auth($shardCfg['password']);
                            }
                            if ($shardCfg['db'] > 0) {
                                $redis->select($shardCfg['db']);
                            }
                            $pool->push($redis);
                            self::$fillFailures[$shardIndex] = 0;
                            $filled++;
                        } catch (\Throwable $e) {
                            self::$fillFailures[$shardIndex] = $failures + 1;
                            fwrite(STDERR, sprintf(
                                '[EdgeAuth] shard=%d background fill failed: %s (consecutive=%d)' . PHP_EOL,
                                $shardIndex, $e->getMessage(), $failures + 1
                            ));
                        }
                        usleep(10000);
                    }

                    fwrite(STDOUT, sprintf(
                        '[EdgeAuth] Redis shard=%d background filled: %d/%d' . PHP_EOL,
                        $shardIndex, $filled, $remaining
                    ));
                });
            }

            self::$pools[$index] = $pool;
        }

        self::$ready = true;
        fwrite(STDOUT, sprintf(
            '[EdgeAuth] Redis sharded mode: %d shards' . PHP_EOL,
            count($shards)
        ));
    }

    /**
     * 解析 EDGE_REDIS_SHARDS 环境变量
     *
     * 格式：host:port[:[password]][@db]
     * 密码允许为空（:: 或 :@db 或无密码段）
     *
     * @return array<int,array{host:string,port:int,password:string,db:int}>
     */
    private static function parseShardList(): array
    {
        $raw = (string) getenv('EDGE_REDIS_SHARDS');
        if ($raw === '') {
            return [];
        }

        $result = [];
        foreach (explode(',', $raw) as $part) {
            $part = trim($part);
            if ($part === '') {
                continue;
            }

            // host:port[:[password]][@db]
            if (! preg_match('/^([^:]+):(\d+)(?:::?(.+?))?(?:@(\d+))?$/', $part, $m)) {
                fwrite(STDERR, sprintf('[EdgeAuth] invalid shard config: %s' . PHP_EOL, $part));
                continue;
            }

            $result[] = [
                'host'     => $m[1],
                'port'     => (int) $m[2],
                'password' => $m[3] ?? '',
                'db'       => isset($m[4]) && $m[4] !== '' ? (int) $m[4] : self::$defaultDb,
            ];
        }

        return $result;
    }

    /**
     * 根据 key 计算 shard 索引
     */
    public static function shardIndex(string $key): int
    {
        if (self::$shardCount <= 1) {
            return 0;
        }

        // 全局 key 固定 shard-0
        if (preg_match('#^(edge:(?:metrics|config|global):)#', $key)) {
            return 0;
        }

        $routeBy = self::extractRouteValue($key);
        if ($routeBy === '') {
            return 0;
        }

        return (crc32($routeBy) & 0x7FFFFFFF) % self::$shardCount;
    }

    /**
     * 从 key 中提取路由因子（clientId 优先，其次 username）
     */
    private static function extractRouteValue(string $key): string
    {
        // ★ edge:device:active:* 走 shard 0（在线统计 key，集中存储便于 SCAN）
        if (preg_match('#^edge:device:active:#', $key)) {
            return '';  // 返回空 → shard 0
        }

        // device:secret:{clientId}:{username}
        if (preg_match('#^device:secret:([^:]+):#', $key, $m)) {
            return $m[1];
        }

        // auth:online:{username}
        if (preg_match('#^auth:online:(.+)$#', $key, $m)) {
            return $m[1];
        }

        // fail:{clientId} / block:{clientId} / rate:{clientId}
        if (preg_match('#^(?:fail|block|rate):(.+)$#', $key, $m)) {
            return $m[1];
        }

        // device:secret:{username}（旧格式兜底）
        if (preg_match('#^device:secret:(.+)$#', $key, $m)) {
            return $m[1];
        }

        return '';
    }

    public static function pop(string $key, float $timeout = 0.3): ?\Redis
    {
        if (!self::$ready || self::$pools === null) {
            return null;
        }

        $index = self::shardIndex($key);
        $pool = self::$pools[$index] ?? null;
        if ($pool === null) {
            return null;
        }

        try {
            $ms = max(1, (int) ($timeout * 1000));
            return $pool->pop($ms);
        } catch (\Throwable) {
            return null;
        }
    }

    public static function push(string $key, \Redis $conn): void
    {
        if (!self::$ready || self::$pools === null) {
            return;
        }

        $index = self::shardIndex($key);
        $pool = self::$pools[$index] ?? null;
        if ($pool === null) {
            return;
        }

        $cfg = self::$configs[$index] ?? null;

        try {
            $conn->ping();
            $pool->push($conn);
            return;
        } catch (\Throwable) {
            // dead, recreate
        }

        if ($cfg !== null) {
            try {
                $new = new \Redis;
                $new->connect($cfg['host'], $cfg['port'], 2.0);
                if ($cfg['password'] !== '') {
                    $new->auth($cfg['password']);
                }
                if ($cfg['db'] > 0) {
                    $new->select($cfg['db']);
                }
                $pool->push($new);
                return;
            } catch (\Throwable $e) {
                fwrite(STDERR, sprintf(
                    '[EdgeAuth] shard=%d replace failed: %s' . PHP_EOL,
                    $index, $e->getMessage()
                ));
            }
        }
    }

    public static function getShardCount(): int
    {
        return self::$shardCount;
    }

    public static function stats(): array
    {
        if (self::$pools === null) {
            return ['mode' => 'uninitialized', 'shards' => []];
        }

        $shards = [];
        $mode = (self::$shardCount > 1) ? 'sharded' : 'single';

        foreach (self::$pools as $i => $pool) {
            try {
                $free = $pool->getLength();
            } catch (\Throwable) {
                $free = 0;
            }
            $shards[$i] = [
                'free'     => $free,
                'capacity' => self::$poolSize,
                'config'   => self::$configs[$i] ?? null,
            ];
        }

        return [
            'mode'        => $mode,
            'shard_count' => count(self::$pools),
            'shards'      => $shards,
        ];
    }

    public static function isSharded(): bool
    {
        return self::$shardCount > 1;
    }

    /**
     * 冷启动门禁：检查连接池是否达到指定填充比例
     */
    public static function isReady(float $threshold = 0.8): bool
    {
        if (!self::$ready || self::$pools === null) {
            return false;
        }

        if (self::$shardCount <= 1) {
            return true; // 单实例模式认为始终就绪
        }

        foreach (self::$pools as $pool) {
            try {
                $free = $pool->getLength();
                $capacity = self::$poolSize;
                if ($capacity > 0 && ($free / $capacity) < $threshold) {
                    return false;
                }
            } catch (\Throwable) {
                return false;
            }
        }

        return true;
    }

    /**
     * 连接池整体填充比例
     */
    public static function fillRatio(): float
    {
        if (self::$pools === null) {
            return 0.0;
        }

        $totalFree = 0;
        $totalCap = self::$poolSize * count(self::$pools);

        foreach (self::$pools as $pool) {
            try {
                $totalFree += $pool->getLength();
            } catch (\Throwable) {
            }
        }

        return $totalCap > 0 ? ($totalFree / $totalCap) : 0.0;
    }
}
