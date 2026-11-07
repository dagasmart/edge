<?php

declare(strict_types=1);

namespace DagaSmart\Edge\Support;

use Swow\Coroutine;
use Throwable;

/**
 * Prometheus 指标收集与 HTTP 暴露（纯内存版）
 *
 * 指标仅存于进程内存，通过 HTTP 端点暴露：
 * - /metrics       Prometheus 文本格式（Prometheus Server 抓取）
 * - /api/metrics   JSON 格式（运维脚本 / 健康检查）
 *
 * 不依赖 Redis、不依赖 Channel、不依赖额外协程。
 */
class PrometheusExporter
{
    /** @var array<string, mixed> */
    protected array $metrics = [];

    /**
     * @var array<string, array{sum: float, count: int, buckets: array<float, int>}>
     */
    protected array $histograms = [];

    protected bool $running = false;

    /** @var resource|null */
    protected $server;

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
            // ============ 接收 ============
            'edge_received_total' => [
                'type' => 'counter',
                'help' => '从 MQTT 接收到的消息总数',
                'value' => 0,
            ],

            // ============ 写入成功 ============
            'edge_high_write_success_total' => [
                'type' => 'counter',
                'help' => '成功写入高优 Redis Stream 的消息总数',
                'value' => 0,
            ],
            'edge_data_write_success_total' => [
                'type' => 'counter',
                'help' => '成功写入普通 Redis Stream 的消息总数',
                'value' => 0,
            ],
            'edge_dead_letter_total' => [
                'type' => 'counter',
                'help' => '进入死信队列的消息总数',
                'value' => 0,
            ],

            // ============ 丢弃 ============
            'edge_dropped_by_sampling_total' => [
                'type' => 'counter',
                'help' => '因采样被丢弃的消息总数',
                'value' => 0,
            ],
            'edge_redis_write_failed_total' => [
                'type' => 'counter',
                'help' => 'Redis 写入失败总数',
                'value' => 0,
            ],

            // ============ 心跳续期 ============
            'edge_heartbeat_renew_total' => [
                'type' => 'counter',
                'help' => '心跳续期 device:active:* 总次数',
                'value' => 0,
            ],

            // ============ MQTT 探针 ============
            'edge_mqtt_probe_failed_total' => [
                'type' => 'counter',
                'help' => 'MQTT 连通性探测失败总次数',
                'value' => 0,
            ],
            'edge_mqtt_probe_consecutive_failures' => [
                'type' => 'gauge',
                'help' => 'MQTT 连续探测失败次数',
                'value' => 0,
            ],
            'edge_mqtt_connected' => [
                'type' => 'gauge',
                'help' => 'MQTT 连接状态（1=已连接，0=已断开）',
                'value' => 0,
            ],

            // ============ 积压 ============
            'edge_queue_backlog' => [
                'type' => 'gauge',
                'help' => '当前 Channel 中的积压消息数',
                'value' => 0,
            ],

            // ============ 等待时间 ============
            'edge_message_wait_ms_normal' => [
                'type' => 'gauge',
                'help' => '普通通道消息平均等待时间（毫秒）',
                'value' => 0.0,
            ],
            'edge_message_wait_ms_high' => [
                'type' => 'gauge',
                'help' => '高优通道消息平均等待时间（毫秒）',
                'value' => 0.0,
            ],
            'edge_message_wait_ms_dead' => [
                'type' => 'gauge',
                'help' => '死信通道消息平均等待时间（毫秒）',
                'value' => 0.0,
            ],

            // ============ QPS ============
            'edge_qps' => [
                'type' => 'gauge',
                'help' => '当前每秒处理消息数',
                'value' => 0.0,
            ],

            // ============ Worker 活跃数 ============
            'edge_active_workers_normal' => [
                'type' => 'gauge',
                'help' => '普通 Worker 活跃数',
                'value' => 0,
            ],
            'edge_active_workers_high' => [
                'type' => 'gauge',
                'help' => '高优 Worker 活跃数',
                'value' => 0,
            ],
            'edge_active_workers_dead' => [
                'type' => 'gauge',
                'help' => '死信 Worker 活跃数',
                'value' => 0,
            ],

            // ============ Channel 积压 ============
            'edge_channel_backlog_normal' => [
                'type' => 'gauge',
                'help' => '普通 Channel 积压消息数',
                'value' => 0,
            ],
            'edge_channel_backlog_high' => [
                'type' => 'gauge',
                'help' => '高优 Channel 积压消息数',
                'value' => 0,
            ],
            'edge_channel_backlog_dead' => [
                'type' => 'gauge',
                'help' => '死信 Channel 积压消息数',
                'value' => 0,
            ],

            // ============ 平均 flush 耗时 ============
            'edge_flush_latency_ms_normal' => [
                'type' => 'gauge',
                'help' => '普通 Worker 平均 flush 耗时（毫秒）',
                'value' => 0.0,
            ],
            'edge_flush_latency_ms_high' => [
                'type' => 'gauge',
                'help' => '高优 Worker 平均 flush 耗时（毫秒）',
                'value' => 0.0,
            ],
            'edge_flush_latency_ms_dead' => [
                'type' => 'gauge',
                'help' => '死信 Worker 平均 flush 耗时（毫秒）',
                'value' => 0.0,
            ],

            // ============ 已刷盘总数 ============
            'edge_flushed_total_normal' => [
                'type' => 'counter',
                'help' => '普通 Worker 已刷盘消息总数',
                'value' => 0,
            ],
            'edge_flushed_total_high' => [
                'type' => 'counter',
                'help' => '高优 Worker 已刷盘消息总数',
                'value' => 0,
            ],
            'edge_flushed_total_dead' => [
                'type' => 'counter',
                'help' => '死信 Worker 已刷盘消息总数',
                'value' => 0,
            ],

            // ============ 活跃客户端 ============
            'edge_active_clients' => [
                'type' => 'gauge',
                'help' => '当前活跃客户端（设备）数',
                'value' => 0,
            ],

            // ============ Auto-Scaler ============
            'edge_target_workers_normal' => [
                'type' => 'gauge',
                'help' => 'Auto-Scaler 目标普通 Worker 数',
                'value' => 0,
            ],
            'edge_target_workers_high' => [
                'type' => 'gauge',
                'help' => 'Auto-Scaler 目标高优 Worker 数',
                'value' => 0,
            ],
            'edge_sample_rate' => [
                'type' => 'gauge',
                'help' => '当前采样率',
                'value' => 1.0,
            ],
            'edge_current_batch_size' => [
                'type' => 'gauge',
                'help' => '当前自适应 batch 大小',
                'value' => 0,
            ],
            'edge_current_flush_interval_ms' => [
                'type' => 'gauge',
                'help' => '当前刷盘间隔（毫秒）',
                'value' => 0,
            ],
        ];

        $this->histograms = [];
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
     * 记录直方图观测值（flush 耗时分布）
     *
     * @param string $name  指标名，如 edge_flush_duration_ms_normal
     * @param float  $value 观测值（毫秒）
     */
    public function observe(string $name, float $value): void
    {
        $this->histograms[$name] ??= [
            'sum' => 0.0,
            'count' => 0,
            'buckets' => [],
        ];

        $this->histograms[$name]['sum'] += $value;
        $this->histograms[$name]['count']++;

        $buckets = config('edge.mqtt.histogram_buckets', [1, 5, 10, 50, 100, 500, 1000, 5000]);

        foreach ($buckets as $b) {
            $b = (float) $b;
            if ($value <= $b) {
                $this->histograms[$name]['buckets'][$b]
                    = ($this->histograms[$name]['buckets'][$b] ?? 0) + 1;
            }
        }

        // +Inf bucket
        $inf = INF;
        $this->histograms[$name]['buckets'][$inf]
            = ($this->histograms[$name]['buckets'][$inf] ?? 0) + 1;
    }

    /**
     * 获取当前指标快照
     */
    public function getMetrics(): array
    {
        return $this->metrics;
    }

    /**
     * 获取直方图快照
     *
     * @return array<string, mixed>
     */
    public function getHistograms(): array
    {
        return $this->histograms;
    }

    /**
     * 构建 Prometheus 文本格式（gauge/counter + histogram）
     */
    public static function buildMetrics(array $metrics, array $histograms = []): string
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
            $lines[] = '';
        }

        // 直方图格式：_bucket{le} / _sum / _count
        foreach ($histograms as $name => $h) {
            $lines[] = "# HELP {$name} {$name}";
            $lines[] = "# TYPE {$name} histogram";

            $buckets = $h['buckets'] ?? [];
            // 按 le 排序，+Inf 放最后
            uksort($buckets, function ($a, $b): int {
                if ($a === INF) return 1;
                if ($b === INF) return -1;
                if (! is_numeric($a) || ! is_numeric($b)) return 0;
                return $a <=> $b;
            });

            $cum = 0;
            foreach ($buckets as $le => $count) {
                $cum += $count;
                $leStr = is_infinite((float) $le)
                    ? '+Inf'
                    : rtrim(rtrim(sprintf('%.6F', (float) $le), '0'), '.');
                $lines[] = sprintf('%s_bucket{le="%s"} %d', $name, $leStr, $cum);
            }

            $lines[] = sprintf(
                '%s_sum %s',
                $name,
                rtrim(rtrim(sprintf('%.6F', $h['sum'] ?? 0.0), '0'), '.'),
            );
            $lines[] = sprintf('%s_count %d', $name, $h['count'] ?? 0);
            $lines[] = '';
        }

        return implode("\n", $lines);
    }

    /**
     * 将指标转换为结构化数组（用于 JSON 接口）
     */
    public static function metricsToArray(array $metrics, array $histograms = []): array
    {
        $result = [];
        foreach ($metrics as $name => $info) {
            $result[] = [
                'name' => $name,
                'type' => $info['type'],
                'help' => $info['help'] ?? '',
                'value' => $info['value'],
            ];
        }

        foreach ($histograms as $name => $h) {
            $result[] = [
                'name' => $name,
                'type' => 'histogram',
                'help' => $name,
                'value' => [
                    'sum' => $h['sum'] ?? 0.0,
                    'count' => $h['count'] ?? 0,
                    'buckets' => $h['buckets'] ?? [],
                ],
            ];
        }

        return $result;
    }

    private static function parsePath(string $request): string
    {
        $line = strtok($request, "\r\n");
        if ($line === false) {
            return '/';
        }
        if (preg_match('#^\w+\s+(/[^?\s]*)#', $line, $matches)) {
            return $matches[1];
        }
        return '/';
    }

    /**
     * 启动 Prometheus HTTP 指标服务
     */
    public function startMetricsServer(
        int $port = 9090,
        string $host = '0.0.0.0',
        ?callable $metricsProvider = null
    ): mixed {
        $this->running = true;
        $provider = $metricsProvider ?? fn (): array => $this->getMetrics();

        $addr = "tcp://{$host}:{$port}";
        $socket = @stream_socket_server(
            $addr,
            $errno,
            $errstr,
            STREAM_SERVER_BIND | STREAM_SERVER_LISTEN
        );

        if ($socket === false) {
            throw new \RuntimeException(
                "无法启动 Prometheus metrics server on {$addr}: {$errstr} ({$errno})"
            );
        }

        stream_set_blocking($socket, false);
        $this->server = $socket;

        Coroutine::run(function () use ($socket, $provider): void {
            /** @var array<int, resource> $conns */
            $conns = [];
            /** @var array<int, string> $buffers */
            $buffers = [];

            while ($this->running) {
                $read = [$socket];
                foreach ($conns as $c) {
                    if (is_resource($c) && !feof($c)) {
                        $read[] = $c;
                    }
                }

                $write = null;
                $except = null;
                $ready = stream_select($read, $write, $except, 1);

                if ($ready === false) {
                    usleep(10000);
                    continue;
                }

                if ($ready <= 0) {
                    continue;
                }

                foreach ($read as $sock) {
                    if ($sock === $socket) {
                        $conn = @stream_socket_accept($socket, 0);
                        if ($conn !== false && $conn !== null) {
                            stream_set_blocking($conn, false);
                            $conns[] = $conn;
                            $buffers[(int) $conn] = '';
                        }
                        continue;
                    }

                    $data = @fread($sock, 8192);
                    $key = (int) $sock;

                    if ($data === '' || $data === false) {
                        fclose($sock);
                        $idx = array_search($sock, $conns, true);
                        if ($idx !== false) {
                            unset($conns[$idx]);
                        }
                        unset($buffers[$key]);
                        continue;
                    }

                    $buffers[$key] .= $data;

                    if (!str_contains($buffers[$key], "\r\n\r\n")) {
                        continue;
                    }

                    try {
                        $request = $buffers[$key];
                        $metrics = $provider();
                        $path = self::parsePath($request);

                        if ($path === '/api/metrics') {
                            $body = json_encode([
                                'code' => 0,
                                'msg' => 'success',
                                'data' => self::metricsToArray(
                                    $metrics,
                                    $this->histograms,
                                ),
                            ], JSON_UNESCAPED_UNICODE | JSON_UNESCAPED_SLASHES);

                            if ($body === false) {
                                throw new \RuntimeException('Failed to encode metrics to JSON');
                            }

                            $resp = "HTTP/1.1 200 OK\r\n"
                                ."Content-Type: application/json; charset=utf-8\r\n"
                                .'Content-Length: '.strlen($body)."\r\n"
                                ."Connection: close\r\n"
                                ."\r\n"
                                .$body;
                        } else {
                            $body = self::buildMetrics(
                                $metrics,
                                $this->histograms,
                            );

                            $resp = "HTTP/1.1 200 OK\r\n"
                                ."Content-Type: text/plain; version=0.0.4; charset=utf-8\r\n"
                                .'Content-Length: '.strlen($body)."\r\n"
                                ."Connection: close\r\n"
                                ."\r\n"
                                .$body;
                        }

                        @fwrite($sock, $resp);
                    } catch (Throwable $e) {
                        @fwrite(
                            $sock,
                            "HTTP/1.1 500 Internal Server Error\r\n"
                            ."Content-Length: 0\r\n"
                            ."Connection: close\r\n"
                            ."\r\n"
                        );
                    } finally {
                        fclose($sock);
                        $idx = array_search($sock, $conns, true);
                        if ($idx !== false) {
                            unset($conns[$idx]);
                        }
                        unset($buffers[$key]);
                    }
                }
            }
        });

        return $socket;
    }

    /**
     * 停止指标服务
     */
    public function stop(): void
    {
        $this->running = false;

        if ($this->server !== null) {
            try {
                fclose($this->server);
            } catch (Throwable) {
                // ignore
            }
            $this->server = null;
        }
    }

    public function isRunning(): bool
    {
        return $this->running;
    }
}
