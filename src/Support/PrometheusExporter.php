<?php

namespace DagaSmart\Edge\Support;

use Swow\Buffer;
use Swow\Coroutine;
use Swow\Socket;
use Swow\SocketException;
use Throwable;

/**
 * Prometheus 指标收集与 HTTP 暴露
 */
class PrometheusExporter
{
    /** @var array<string, mixed> */
    protected array $metrics = [];

    protected bool $running = false;

    protected Socket $server;

    public function __construct()
    {
        $this->reset();
    }

    /**
     * 重置所有指标（进程启动时调用）
     */
    public function reset(): void
    {
        $this->metrics = [
            // 接收总量
            'edge_received_total' => [
                'type' => 'counter',
                'help' => 'Total number of messages received from MQTT',
                'value' => 0,
            ],
            // 高优写入成功
            'edge_high_write_success_total' => [
                'type' => 'counter',
                'help' => 'Total number of high priority messages written to Redis Stream',
                'value' => 0,
            ],
            // 普通写入成功
            'edge_data_write_success_total' => [
                'type' => 'counter',
                'help' => 'Total number of normal messages written to Redis Stream',
                'value' => 0,
            ],
            // 死信总量
            'edge_dead_letter_total' => [
                'type' => 'counter',
                'help' => 'Total number of messages sent to dead letter queue',
                'value' => 0,
            ],
            // 当前队列积压
            'edge_queue_backlog' => [
                'type' => 'gauge',
                'help' => 'Current message backlog in channels',
                'value' => 0,
            ],
            // 普通 Channel 等待时间
            'edge_message_wait_ms_normal' => [
                'type' => 'gauge',
                'help' => 'Average wait time in normal channel (ms)',
                'value' => 0,
            ],
            // 高优 Channel 等待时间
            'edge_message_wait_ms_high' => [
                'type' => 'gauge',
                'help' => 'Average wait time in high priority channel (ms)',
                'value' => 0,
            ],
            // QPS
            'edge_qps' => [
                'type' => 'gauge',
                'help' => 'Current messages processed per second',
                'value' => 0,
            ],
            // Redis 写入失败
            'edge_redis_write_failed_total' => [
                'type' => 'counter',
                'help' => 'Total number of Redis write failures',
                'value' => 0,
            ],
            // MQTT 连接状态
            'edge_mqtt_connected' => [
                'type' => 'gauge',
                'help' => 'MQTT connection status (1=connected, 0=disconnected)',
                'value' => 0,
            ],
        ];
    }

    /**
     * 递增计数器
     */
    public function inc(string $name, int $value = 1): void
    {
        if (isset($this->metrics[$name]) && $this->metrics[$name]['type'] === 'counter') {
            $this->metrics[$name]['value'] += $value;
        }
    }

    /**
     * 设置 Gauge 值
     */
    public function set(string $name, float|int $value): void
    {
        if (isset($this->metrics[$name]) && $this->metrics[$name]['type'] === 'gauge') {
            $this->metrics[$name]['value'] = $value;
        }
    }

    /**
     * 获取当前指标快照（供外部使用）
     */
    public function getMetrics(): array
    {
        return $this->metrics;
    }

    /**
     * 构建 Prometheus 文本格式
     */
    public static function buildMetrics(array $metrics): string
    {
        $lines = [];

        foreach ($metrics as $name => $info) {
            if (isset($info['help'])) {
                $lines[] = "# HELP {$name} {$info['help']}";
            }
            if (isset($info['type'])) {
                $lines[] = "# TYPE {$name} {$info['type']}";
            }
            $lines[] = sprintf('%s %s', $name, $info['value']);
            $lines[] = ''; // 每条指标后空行，符合 Prometheus 格式
        }

        return implode("\n", $lines);
    }

    /**
     * 启动 Prometheus HTTP 指标服务（协程方式）
     * 返回 Server Socket 供调用方管理生命周期
     */
    public function startMetricsServer(
        int $port = 9090,
        string $host = '0.0.0.0',
        ?callable $metricsProvider = null
    ): Socket {
        $this->running = true;

        $server = new Socket(Socket::TYPE_TCP);
        $server->bind($host, $port)->listen();
        $this->server = $server;

        $provider = $metricsProvider ?? fn(): array => $this->getMetrics();

        Coroutine::run(function () use ($server, $provider): void {
            while ($this->running) {
                try {
                    $conn = $server->accept();

                    if ($conn === false) {
                        // 非阻塞模式下无连接，让出 CPU
                        Coroutine::yield();
                        continue;
                    }

                    // 每个连接独立协程处理
                    Coroutine::run(function () use ($conn, $provider): void {
                        $readBuffer = null;

                        try {
                            // Swow Socket::read(Buffer) —— 你的版本无 SocketTimeoutException
                            $readBuffer = new Buffer(Buffer::COMMON_SIZE);
                            $conn->read($readBuffer);

                            // 忽略请求内容，只回指标
                            $metrics = $provider();
                            $body = self::buildMetrics($metrics);

                            $header = "HTTP/1.1 200 OK\r\n"
                                . "Content-Type: text/plain; version=0.0.4\r\n"
                                . "Content-Length: " . strlen($body) . "\r\n"
                                . "Connection: close\r\n"
                                . "\r\n";

                            // Swow Socket::write(array) —— I/O 向量写
                            $conn->write([
                                new Buffer($header),
                                new Buffer($body),
                            ]);

                        } catch (SocketException) {
                            // socket 读/写错误（客户端断开等），静默关闭
                        } catch (Throwable) {
                            // 业务逻辑异常，尝试回 500
                            try {
                                $conn->write([
                                    new Buffer(
                                        "HTTP/1.1 500 Internal Server Error\r\n"
                                        . "Content-Length: 0\r\n"
                                        . "Connection: close\r\n"
                                        . "\r\n"
                                    ),
                                ]);
                            } catch (Throwable) {
                                // ignore
                            }
                        } finally {
                            // 确保连接一定关闭，防止泄漏
                            try {
                                $conn->close();
                            } catch (Throwable) {
                                // ignore
                            }
                            unset($readBuffer);
                        }
                    });

                } catch (SocketException) {
                    // accept 失败，短暂让出 CPU，避免忙等
                    if ($this->running) {
                        Coroutine::yield();
                    }
                } catch (Throwable) {
                    if ($this->running) {
                        Coroutine::yield();
                    }
                }
            }
        });

        return $server;
    }

    /**
     * 停止指标服务
     */
    public function stop(): void
    {
        $this->running = false;

        if (isset($this->server)) {
            try {
                $this->server->close();
            } catch (Throwable) {
                // ignore
            }
        }
    }

    public function isRunning(): bool
    {
        return $this->running;
    }
}
