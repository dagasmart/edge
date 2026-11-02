<?php
declare(strict_types=1);

namespace DagaSmart\Edge\Console\Commands;




use Illuminate\Console\Command;
use PhpMqtt\Client\Facades\MQTT;
use PhpMqtt\Client\Exceptions\MqttClientException;
use Throwable;
use Swow\Coroutine;
use Swow\Signal;

$edgeRoot = dirname(__DIR__, 3);
require_once $edgeRoot . '/bin/bootstrap.php';
require_once $edgeRoot . '/bin/helper.php';

class EdgeInsertRedisCommand extends Command
{
    protected $signature = 'edge:insert-redis';
    protected $description = '订阅 NanoMQ heartbeat/response，批量写入 Redis Stream（php-mqtt/laravel-client）';

    private const string STREAM = 'stream:device:data';
    private const string DEAD_STREAM = 'stream:device:dead';

    private const int MAX_LEN = 50000;
    private const int DEAD_MAX_LEN = 10000;

    private const int BATCH_SIZE = 100;

    private const int RECONNECT_BASE_DELAY = 1;
    private const int RECONNECT_MAX_DELAY = 30;

    /** @var array<int, array<string, mixed>> */
    private array $batch = [];
    private bool $stopping = false;

    public function handle(): int
    {
        $this->info('[edge:insert-redis] 启动，PID=' . getmypid());

        // Windows/Linux 通用：Swow 协程监听信号
        foreach ([Signal::INT, Signal::TERM] as $sig) {
            Coroutine::run(function () use ($sig): void {
                Signal::wait($sig);
                $this->warn("[signal] 收到 {$sig}，准备优雅停机");
                $this->stopping = true;
            });
        }

        $this->initRedisPool();

        $attempt = 0;
        $delay = self::RECONNECT_BASE_DELAY;

        while (!$this->stopping) {
            $mqtt = null;

            try {
                $mqtt = MQTT::connection();
                $this->subscribe($mqtt);

                if ($attempt > 0) {
                    $this->warn('[mqtt] 重连成功');
                } else {
                    $this->info(sprintf(
                        '[mqtt] 已连接 %s:%d 并订阅 heartbeat/response',
                        config('mqtt-client.connections.nanomq.host'),
                        config('mqtt-client.connections.nanomq.port'),
                    ));
                }

                $attempt = 0;
                $delay = self::RECONNECT_BASE_DELAY;

                // 用 loop(false) 自己驱动，这样每次迭代都能检查 $stopping
                while (!$this->stopping) {
                    $mqtt->loop(false);
                }

                // stopping=true 退出内层循环后，跳出外层 while
                break;

            } catch (MqttClientException $e) {
                $attempt++;
                $this->error(sprintf(
                    '[mqtt] 连接异常（第 %d 次）：%s，%d 秒后重试',
                    $attempt,
                    $e->getMessage(),
                    $delay,
                ));

                try {
                    $mqtt?->disconnect();
                } catch (Throwable) {
                    // ignore
                }

                // 可中断的等待
                $waited = 0;
                $target = $delay * 1000000;
                while ($waited < $target && !$this->stopping) {
                    usleep(100000);
                    $waited += 100000;
                }

                $delay = min((int) ($delay * 1.5), self::RECONNECT_MAX_DELAY);
            }
        }

        // 停机前把剩余批次刷完
        if ($this->batch !== []) {
            $this->flushBatch($this->batch);
            $this->batch = [];
        }

        $this->info('[edge:insert-redis] 已停止');
        return 0;
    }

    private function subscribe($mqtt): void
    {
        $mqtt->subscribe('heartbeat/response', function (
            string $topic,
            string $payload,
            bool $retained,
            int $qos
        ) {
            if ($this->stopping) {
                return;
            }

            $parts = explode('/', $topic, 3);
            $device = $parts[1] ?? 'unknown';

            $this->line(sprintf(
                '[MQTT] topic=%s qos=%d payload_len=%d',
                $topic,
                $qos,
                strlen($payload),
            ));

            $this->batch[] = [
                'topic' => $topic,
                'payload' => $payload,
                'device' => $device,
                'ts' => (string) microtime(true),
            ];

            if (count($this->batch) >= self::BATCH_SIZE) {
                $this->flushBatch($this->batch);
                $this->batch = [];
            }
        }, 1);
    }

    /* ==================== Redis ==================== */

    private function initRedisPool(): void
    {
        if (!function_exists('redisPoolInit')) {
            throw new \RuntimeException('helper.php 未加载');
        }
        if (!defined('REDIS_HOST')) {
            throw new \RuntimeException('bootstrap.php 未加载');
        }

        redisPoolInit(
            (int) (defined('REDIS_POOL_SIZE') ? REDIS_POOL_SIZE : 64),
            (int) (defined('REDIS_DB') ? REDIS_DB : 2),
        );

        $this->info(sprintf(
            '[redis] 连接池已初始化 %s:%s db=%s',
            REDIS_HOST,
            REDIS_PORT,
            REDIS_DB,
        ));
    }

    private function flushBatch(array $batch): void
    {
        if ($batch === []) {
            return;
        }

        try {
            withRedis(function (\Redis $conn) use ($batch): void {
                $conn->multi(\Redis::PIPELINE);
                foreach ($batch as $item) {
                    $conn->xAdd(
                        self::STREAM,
                        '*',
                        [
                            'topic' => $item['topic'],
                            'payload' => $item['payload'],
                            'device' => $item['device'],
                            'ts' => $item['ts'],
                        ],
                        self::MAX_LEN,
                        true,
                    );
                }
                $conn->exec();
            });

            $this->line(sprintf('[FLUSH] batch=%d → %s', count($batch), self::STREAM));
        } catch (Throwable $e) {
            $this->error(sprintf(
                '[FLUSH] Redis 写入失败（%d 条）→ 进入死信：%s',
                count($batch),
                $e->getMessage(),
            ));
            $this->writeDeadLetter($batch, $e);
        }
    }

    private function writeDeadLetter(array $batch, Throwable $cause): void
    {
        try {
            withRedis(function (\Redis $conn) use ($batch, $cause): void {
                $conn->xAdd(
                    self::DEAD_STREAM,
                    '*',
                    [
                        'batch' => json_encode($batch, JSON_UNESCAPED_SLASHES | JSON_UNESCAPED_UNICODE),
                        'error' => $cause->getMessage(),
                        'ts' => (string) microtime(true),
                    ],
                    self::DEAD_MAX_LEN,
                    true,
                );
            });

            $this->warn('[DEAD] 已写入死信 ' . self::DEAD_STREAM);
        } catch (Throwable $e) {
            fwrite(STDERR, '[FATAL] 死信写入也失败：' . $e->getMessage() . PHP_EOL);
        }
    }
}
