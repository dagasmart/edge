<?php

declare(strict_types=1);

namespace DagaSmart\Edge\Support;

use Throwable;

/**
 * Prometheus 指标收集与 HTTP 暴露（stream_socket 版 · Windows 兼容）
 *
 * ✅ 纯 PHP stream，Windows/Linux 通用
 * ✅ 修复 stop() 竞态（先置 null 再 fclose）
 * ✅ 修复 stream_select TypeError
 */
class PrometheusExporter
{
    private const string INF_BUCKET = '+Inf';
    private const int MAX_HEADER_SIZE = 65536;

    /** @var array<string, mixed> */
    protected array $metrics = [];

    /**
     * @var array<string, array{sum: float, count: int, buckets: array<float, int>}>
     */
    protected array $histograms = [];

    protected bool $running = false;

    /** @var resource|null */
    protected $server = null;

    /** @var array<int, resource> */
    protected array $connections = [];

    protected int $maxConnections = 64;

    public function __construct()
    {
        $this->reset();
    }

    /* ==================== 指标 API ==================== */

    public function reset(): void
    {
        $this->metrics = [
            'edge_received_total' => ['type' => 'counter', 'help' => '从 MQTT 接收到的消息总数', 'value' => 0],
            'edge_high_write_success_total' => ['type' => 'counter', 'help' => '成功写入高优 Redis Stream 的消息总数', 'value' => 0],
            'edge_data_write_success_total' => ['type' => 'counter', 'help' => '成功写入普通 Redis Stream 的消息总数', 'value' => 0],
            'edge_dead_letter_total' => ['type' => 'counter', 'help' => '进入死信队列的消息总数', 'value' => 0],
            'edge_dropped_by_sampling_total' => ['type' => 'counter', 'help' => '因采样被丢弃的消息总数', 'value' => 0],
            'edge_redis_write_failed_total' => ['type' => 'counter', 'help' => 'Redis 写入失败总数', 'value' => 0],
            'edge_heartbeat_renew_total' => ['type' => 'counter', 'help' => '心跳续期 device:active:* 总次数', 'value' => 0],
            'edge_mqtt_probe_failed_total' => ['type' => 'counter', 'help' => 'MQTT 连通性探测失败总次数', 'value' => 0],
            'edge_mqtt_probe_consecutive_failures' => ['type' => 'gauge', 'help' => 'MQTT 连续探测失败次数', 'value' => 0],
            'edge_mqtt_connected' => ['type' => 'gauge', 'help' => 'MQTT 连接状态（1=已连接，0=已断开）', 'value' => 0],
            'edge_queue_backlog' => ['type' => 'gauge', 'help' => '当前 Channel 中的积压消息数', 'value' => 0],
            'edge_message_wait_ms_normal' => ['type' => 'gauge', 'help' => '普通通道消息平均等待时间（毫秒）', 'value' => 0.0],
            'edge_message_wait_ms_high' => ['type' => 'gauge', 'help' => '高优通道消息平均等待时间（毫秒）', 'value' => 0.0],
            'edge_message_wait_ms_dead' => ['type' => 'gauge', 'help' => '死信通道消息平均等待时间（毫秒）', 'value' => 0.0],
            'edge_qps' => ['type' => 'gauge', 'help' => '当前每秒处理消息数', 'value' => 0.0],
            'edge_active_workers_normal' => ['type' => 'gauge', 'help' => '普通 Worker 活跃数', 'value' => 0],
            'edge_active_workers_high' => ['type' => 'gauge', 'help' => '高优 Worker 活跃数', 'value' => 0],
            'edge_active_workers_dead' => ['type' => 'gauge', 'help' => '死信 Worker 活跃数', 'value' => 0],
            'edge_channel_backlog_normal' => ['type' => 'gauge', 'help' => '普通 Channel 积压消息数', 'value' => 0],
            'edge_channel_backlog_high' => ['type' => 'gauge', 'help' => '高优 Channel 积压消息数', 'value' => 0],
            'edge_channel_backlog_dead' => ['type' => 'gauge', 'help' => '死信 Channel 积压消息数', 'value' => 0],
            'edge_flush_latency_ms_normal' => ['type' => 'gauge', 'help' => '普通 Worker 平均 flush 耗时（毫秒）', 'value' => 0.0],
            'edge_flush_latency_ms_high' => ['type' => 'gauge', 'help' => '高优 Worker 平均 flush 耗时（毫秒）', 'value' => 0.0],
            'edge_flush_latency_ms_dead' => ['type' => 'gauge', 'help' => '死信 Worker 平均 flush 耗时（毫秒）', 'value' => 0.0],
            'edge_flushed_total_normal' => ['type' => 'counter', 'help' => '普通 Worker 已刷盘消息总数', 'value' => 0],
            'edge_flushed_total_high' => ['type' => 'counter', 'help' => '高优 Worker 已刷盘消息总数', 'value' => 0],
            'edge_flushed_total_dead' => ['type' => 'counter', 'help' => '死信 Worker 已刷盘消息总数', 'value' => 0],
            'edge_active_clients' => ['type' => 'gauge', 'help' => '当前活跃客户端（设备）数', 'value' => 0],
            'edge_target_workers_normal' => ['type' => 'gauge', 'help' => 'Auto-Scaler 目标普通 Worker 数', 'value' => 0],
            'edge_target_workers_high' => ['type' => 'gauge', 'help' => 'Auto-Scaler 目标高优 Worker 数', 'value' => 0],
            'edge_sample_rate' => ['type' => 'gauge', 'help' => '当前采样率', 'value' => 1.0],
            'edge_current_batch_size' => ['type' => 'gauge', 'help' => '当前自适应 batch 大小', 'value' => 0],
            'edge_current_flush_interval_ms' => ['type' => 'gauge', 'help' => '当前刷盘间隔（毫秒）', 'value' => 0],
        ];

        $this->histograms = [];
    }

    public function inc(string $name, int $value = 1): void
    {
        if (isset($this->metrics[$name]) && $this->metrics[$name]['type'] === 'counter') {
            $this->metrics[$name]['value'] += $value;
            return;
        }
        self::warnUnknownMetric($name, 'counter');
    }

    public function set(string $name, float|int $value): void
    {
        if (isset($this->metrics[$name]) && $this->metrics[$name]['type'] === 'gauge') {
            $this->metrics[$name]['value'] = $value;
            return;
        }
        self::warnUnknownMetric($name, 'gauge');
    }

    public function observe(string $name, float $value): void
    {
        $this->histograms[$name] ??= ['sum' => 0.0, 'count' => 0, 'buckets' => []];
        $this->histograms[$name]['sum'] += $value;
        $this->histograms[$name]['count']++;

        $buckets = (array) config('edge.mqtt.histogram_buckets', [1, 5, 10, 50, 100, 500, 1000, 5000]);
        if ($buckets === []) {
            $buckets = [1, 5, 10, 50, 100, 500, 1000, 5000];
        }

        foreach ($buckets as $b) {
            $key = self::bucketKey((float) $b);
            $this->histograms[$name]['buckets'][$key] ??= 0;
            if ($value <= (float) $b) {
                $this->histograms[$name]['buckets'][$key]++;
            }
        }

        $this->histograms[$name]['buckets'][self::INF_BUCKET]
            ??= 0;
        $this->histograms[$name]['buckets'][self::INF_BUCKET]++;
    }

    /* ==================== HTTP Server（stream_socket · 单协程） ==================== */

    /**
     * 启动 metrics HTTP server
     *
     * @return resource socket 资源
     */
    public function startMetricsServer(
        int $port = 9090,
        string $host = '0.0.0.0',
        ?callable $metricsProvider = null
    ) {
        $provider = $metricsProvider ?? fn(): array => $this->getMetrics();

        $this->server = @stream_socket_server(
            "tcp://{$host}:{$port}",
            $errno,
            $errstr,
            STREAM_SERVER_BIND | STREAM_SERVER_LISTEN
        );

        if ($this->server === false) {
            throw new \RuntimeException(
                "无法启动 Prometheus metrics server on {$host}:{$port}: {$errstr} (errno={$errno})"
            );
        }

        stream_set_blocking($this->server, false);

        $this->running = true;

        // ✅ 单协程处理 accept + read + respond
        \Swow\Coroutine::run(function () use ($provider): void {
            $timeout = 1; // stream_select 超时秒数

            while ($this->running && $this->server && is_resource($this->server)) {
                $read = [$this->server];
                $write = null;
                $except = null;

                // ✅ 防御：server 被关后 stream_select 会报 Warning
                $result = @stream_select($read, $write, $except, $timeout);

                if ($result === false) {
                    // socket 已关闭，退出循环
                    break;
                }

                if ($result === 0) {
                    // 超时，继续检查 running 状态
                    continue;
                }

                // ✅ 有连接进来
                $conn = @stream_socket_accept($this->server, 0, $peer);
                if ($conn === false) {
                    continue;
                }

                stream_set_blocking($conn, false);

                // ✅ 读取请求
                $buffer = '';
                $headersDone = false;

                $readAttempts = 0;
                while (!$headersDone && strlen($buffer) < self::MAX_HEADER_SIZE && $readAttempts < 100) {
                    $chunk = @fread($conn, 8192);
                    if ($chunk === '' || $chunk === false) {
                        usleep(10000); // 10ms 等数据
                        $readAttempts++;
                        continue;
                    }
                    $buffer .= $chunk;
                    if (str_contains($buffer, "\r\n\r\n")) {
                        $headersDone = true;
                    }
                    $readAttempts++;
                }

                // ✅ 构造响应
                if (!$headersDone || $buffer === '') {
                    $resp = "HTTP/1.1 400 Bad Request\r\nContent-Length: 0\r\nConnection: close\r\n\r\n";
                } else {
                    $path = $this->parsePath($buffer);
                    $metrics = $provider();

                    if ($path === '/api/metrics') {
                        $body = json_encode([
                            'code' => 0,
                            'msg' => 'success',
                            'data' => self::metricsToArray($metrics, $this->histograms),
                        ], JSON_UNESCAPED_UNICODE | JSON_UNESCAPED_SLASHES);

                        $resp = "HTTP/1.1 200 OK\r\n"
                            . "Content-Type: application/json; charset=utf-8\r\n"
                            . "Content-Length: " . strlen($body) . "\r\n"
                            . "Connection: close\r\n\r\n"
                            . $body;
                    } else {
                        $body = self::buildMetrics($metrics, $this->histograms);
                        $resp = "HTTP/1.1 200 OK\r\n"
                            . "Content-Type: text/plain; version=0.0.4; charset=utf-8\r\n"
                            . "Content-Length: " . strlen($body) . "\r\n"
                            . "Connection: close\r\n\r\n"
                            . $body;
                    }
                }

                @fwrite($conn, $resp);
                @fclose($conn);
            }
        });

        return $this->server;
    }

    /* ==================== 停机 ==================== */

    public function stop(): void
    {
        $this->running = false;

        // ✅ 先置 null，让循环条件变 false
        $server = $this->server;
        $this->server = null;

        // ✅ 再 shutdown + close
        if ($server !== null && is_resource($server)) {
            @stream_socket_shutdown($server, STREAM_SHUT_RDWR);
            @fclose($server);
        }
    }

    public function isRunning(): bool
    {
        return $this->running;
    }

    /* ==================== 工具方法 ==================== */

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

        foreach ($histograms as $name => $h) {
            $lines[] = "# HELP {$name} {$name}";
            $lines[] = "# TYPE {$name} histogram";

            $buckets = $h['buckets'] ?? [];
            uksort($buckets, function ($a, $b): int {
                $aInf = self::isInfBucket($a);
                $bInf = self::isInfBucket($b);
                if ($aInf && $bInf) return 0;
                if ($aInf) return 1;
                if ($bInf) return -1;
                if (!is_numeric($a) || !is_numeric($b)) return 0;
                return (float) $a <=> (float) $b;
            });

            $cum = 0;
            foreach ($buckets as $le => $count) {
                $cum += $count;
                $leStr = self::isInfBucket($le) ? '+Inf' : rtrim(rtrim(sprintf('%.6F', (float) $le), '0'), '.');
                $lines[] = sprintf('%s_bucket{le="%s"} %d', $name, $leStr, $cum);
            }

            $lines[] = sprintf('%s_sum %s', $name, rtrim(rtrim(sprintf('%.6F', $h['sum'] ?? 0.0), '0'), '.'));
            $lines[] = sprintf('%s_count %d', $name, $h['count'] ?? 0);
            $lines[] = '';
        }

        return implode("\n", $lines);
    }

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

    private static function bucketKey(float $b): string
    {
        return rtrim(rtrim(sprintf('%.6F', $b), '0'), '.');
    }

    private static function isInfBucket(mixed $key): bool
    {
        if ($key === INF || $key === self::INF_BUCKET) {
            return true;
        }
        return is_string($key) && in_array(strtolower($key), ['inf', '+inf'], true);
    }

    private static function warnUnknownMetric(string $name, string $expected): void
    {
        static $warned = [];
        if (isset($warned[$name])) {
            return;
        }
        $warned[$name] = true;
        error_log(sprintf('[PrometheusExporter] 忽略未注册指标 %s（期望 %s）', $name, $expected));
    }

    public function getMetrics(): array { return $this->metrics; }
    public function getHistograms(): array { return $this->histograms; }
}
