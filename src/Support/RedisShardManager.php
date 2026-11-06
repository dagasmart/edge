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
 * - device:secret:{clientId}:{username} → 按 clientId
 * - auth:online:{username} → 按 username
 * - fail:{clientId} / block:{clientId} / rate:{clientId} → 按 clientId
 * - device:secret:{username}（旧格式兜底）→ 按 username
 * - 无法提取路由因子 → shard-0
 *
 * 未配置 EDGE_REDIS_SHARDS → 退化为单实例模式，行为与原来完全一致。
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

    public static function init(int $poolSize = 256, int $db = 2): void
    {
        if (self::$ready) {
            return;
        }

        self::$poolSize = $poolSize;
        self::$defaultDb = $db;

        $shards = self::parseShardList();
        if ($shards === []) {
            // 未配置，退化为单实例
            self::$shardCount = 0;
            self::$ready = true;
            return;
        }

        self::$configs = $shards;
        self::$shardCount = count($shards);
        self::$pools = [];

        foreach ($shards as $index => $cfg) {
            $pool = new Channel($poolSize);
            $ok = self::fillProbe($pool, $cfg, min(3, $poolSize));

            fwrite(STDOUT, sprintf(
                '[EdgeAuth] Redis shard=%d probed=%d/%d (%s:%d/%d)%s',
                $index, $ok, min(3, $poolSize),
                $cfg['host'], $cfg['port'], $cfg['db'], PHP_EOL
            ));

            // 后台渐进填充
            $remaining = $poolSize - $ok;
            if ($ok > 0 && $remaining > 0) {
                $shardIndex = $index;
                Coroutine::run(static function () use ($remaining, $pool, $cfg, $shardIndex): void {
                    $filled = 0;
                    for ($i = 0; $i < $remaining; $i++) {
                        if (self::pushNew($pool, $cfg)) {
                            $filled++;
                        }
                        usleep(10000);
                    }
                    fwrite(STDOUT, sprintf(
                        '[EdgeAuth] Redis shard=%d filled=%d%s',
                        $shardIndex, $filled, PHP_EOL
                    ));
                });
            }

            self::$pools[$index] = $pool;
        }

        self::$ready = true;
    }

    /**
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

            $db = 0;
            if (preg_match('/^(.+?)@(\d+)$/', $part, $m)) {
                $part = $m[1];
                $db = (int) $m[2];
            }

            if (! preg_match('/^(.+?):(\d+)(?::(.+))?$/', $part, $m)) {
                fwrite(STDERR, sprintf('[EdgeAuth] invalid shard: %s%s', $part, PHP_EOL));
                continue;
            }

            $result[] = [
                'host' => $m[1],
                'port' => (int) $m[2],
                'password' => $m[3] ?? '',
                'db' => $db ?: self::$defaultDb,
            ];
        }

        return $result;
    }

    private static function fillProbe(Channel $pool, array $cfg, int $count): int
    {
        $ok = 0;
        for ($i = 0; $i < $count; $i++) {
            if (self::pushNew($pool, $cfg)) {
                $ok++;
            }
        }
        return $ok;
    }

    private static function pushNew(Channel $pool, array $cfg): bool
    {
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
            return true;
        } catch (\Throwable $e) {
            fwrite(STDERR, sprintf(
                '[EdgeAuth] shard connect failed: %s%s', $e->getMessage(), PHP_EOL
            ));
            return false;
        }
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
            self::pushNew($pool, $cfg);
        }
    }

    public static function getShardCount(): int
    {
        return self::$shardCount;
    }

    public static function stats(): array
    {
        if (self::$pools === null) {
            return ['mode' => 'single', 'shards' => []];
        }

        $shards = [];
        foreach (self::$pools as $i => $pool) {
            try {
                $free = $pool->getLength();
            } catch (\Throwable) {
                $free = 0;
            }
            $shards[$i] = [
                'free' => $free,
                'capacity' => self::$poolSize,
                'config' => self::$configs[$i] ?? null,
            ];
        }

        return [
            'mode' => self::$shardCount > 1 ? 'sharded' : 'single',
            'shard_count' => self::$shardCount,
            'shards' => $shards,
        ];
    }

    public static function isSharded(): bool
    {
        return self::$shardCount > 1;
    }
}
