<?php

declare(strict_types=1);

namespace DagaSmart\Edge\Engine;

use PhpMqtt\Client\Exceptions\MqttClientException;
use PhpMqtt\Client\Facades\MQTT;
use PhpMqtt\Client\MqttClient;
use Swow\Channel;
use Swow\Coroutine;
use Throwable;

/**
 * NanoMQ 订阅封装（Swow 协程版）
 *
 * ★ FIX: loop(true, true) → loop(true, false)
 *        第二个参数为「队列空即退出」，true 会让订阅完成后立刻返回，造成连接风暴。
 *
 * ★ FIX: 不再把「loop() 返回」等同于「连接断开」。interrupt 残留、内部 socket
 *        超时都会让它提前返回；连接仍存活就续跑 loop，不触发重连与退避。
 *
 * ★ FIX: clearConnection() 不再调用 interrupt()。MQTT::connection() 复用 facade
 *        缓存实例，interrupt 标志残留会让下一轮 loop() 立刻返回。
 *
 * ★ FIX: 移除运行时 config()->set() 注入 client_id。
 *        ConnectionManager 构造函数保存的是配置数组快照，运行时改 config() 无效。
 *        client_id 必须直接写进 config/mqtt-client.php（或 config/mqtt.php）：
 *            'client_id' => 'ccs_edge_' . getmypid() . '_' . bin2hex(random_bytes(3))
 *
 * ★ FIX: 移除每轮前置 MQTT::disconnect()。ConnectionManager::connection() 自身
 *        会剔除 isConnected()==false 的缓存实例并新建连接，前置调用是多余的。
 *
 * ★ FIX: 启动时打印「TCP 探针地址」与「facade 实际连接地址」并对比。
 *        探针读 edge.mqtt.*，实际连接读 mqtt-client.php，两套配置不一致时
 *        TCP 预检通过完全不代表 MQTT 握手能成功。
 *
 * ★ FIX: 建连异常打印完整异常链（含 previous），不再静默成「持续 0.0s」。
 */
class NanoMQ
{
    private ?Coroutine $coroutine = null;

    private Channel $exitChannel;

    private ?MqttClient $connection = null;

    private bool $stopping = false;

    private int $lastPrecheckFail = 0;

    /** 最近一次收到消息的时间戳，用于识别「loop 仍在但连接已假死」 */
    private float $lastActivityAt = 0.0;

    /** 连续无消息多久判定为假死（秒）。心跳周期约 30s，留出 3 倍余量 */
    private const float IDLE_TIMEOUT_SEC = 90.0;

    public function __construct()
    {
        $this->exitChannel = new Channel(1);
    }

    public function publish(string $topic, string $message): static
    {
        try {
            MQTT::publish($topic, $message);
        } catch (Throwable $e) {
            fwrite(STDERR, '[MQTT] publish 异常：' . $this->formatException($e) . PHP_EOL);
        }

        return $this;
    }

    public function subscribe(string $topic, callable $callback): void
    {
        $this->subscribeMultiple([$topic], $callback);
    }

    public function subscribeMultiple(array $topics, callable $callback): void
    {
        $this->coroutine = Coroutine::run(function () use ($topics, $callback): void {
            $retryDelay = 1;
            $maxRetryDelay = 30;

            // 只打印一次，避免重连时刷屏
            $this->describeConnectionConfig();

            while (! $this->stopping) {
                // 0.0 表示本轮尚未完成「连接 + 订阅」
                $connectedAt = 0.0;

                try {
                    // ---------- 1) TCP 预检（edge.mqtt.*） ----------
                    if (! $this->precheckTcp($retryDelay, $maxRetryDelay)) {
                        continue;
                    }

                    // ---------- 2) 建连 + 订阅 ----------
                    // connection() 内部会 new MqttClient + connect()，异常就在这里抛
                    $mqtt = $this->connection = MQTT::connection();

                    foreach ($topics as $t) {
                        $mqtt->subscribe($t, function (string $topic, string $message) use ($callback): void {
                            $this->lastActivityAt = microtime(true);
                            try {
                                call_user_func($callback, $topic, $message);
                            } catch (Throwable $e) {
                                fwrite(STDERR, '[MQTT] 回调异常：' . $this->formatException($e) . PHP_EOL);
                            }
                        }, 0);
                    }

                    $connectedAt = microtime(true);
                    $this->lastActivityAt = $connectedAt;

                    fwrite(STDERR, sprintf(
                        "\033[32m[MQTT] 连接成功 | client_id=%s | 订阅 topics=%s\033[0m\n",
                        $this->safeGetClientId($mqtt),
                        implode(', ', $topics),
                    ));

                    // ---------- 3) 事件循环 ----------
                    $reason = $this->runEventLoop($mqtt);

                    if ($this->stopping) {
                        break;
                    }

                    $this->clearConnection();
                    $this->backoffAfterDisconnect($connectedAt, $retryDelay, $maxRetryDelay, $reason);

                } catch (MqttClientException $e) {
                    if ($this->stopping) {
                        break;
                    }

                    fwrite(STDERR, '[MQTT] 建连/订阅失败：' . $this->formatException($e) . PHP_EOL);

                    $this->clearConnection();
                    $this->backoffAfterDisconnect($connectedAt, $retryDelay, $maxRetryDelay, 'exception');

                } catch (Throwable $e) {
                    if ($this->stopping) {
                        break;
                    }

                    fwrite(STDERR, '[MQTT] 建连/订阅未知异常：' . $this->formatException($e) . PHP_EOL);

                    $this->clearConnection();
                    $this->backoffAfterDisconnect($connectedAt, $retryDelay, $maxRetryDelay, 'exception');
                }
            }

            $this->clearConnection();
            try {
                $this->exitChannel->push(true);
            } catch (Throwable) {
                // ignore
            }
        });
    }

    /**
     * 打印并对比两套配置，定位「TCP 探针通但 MQTT 握手失败」最常见的根因。
     */
    private function describeConnectionConfig(): void
    {
        $default = (string) config(
            'mqtt-client.default_connection',
            config('mqtt.default_connection', 'default'),
        );

        $cfg = [];
        foreach (["mqtt-client.connections.{$default}", "mqtt.connections.{$default}"] as $key) {
            $value = config($key);
            if (is_array($value)) {
                $cfg = $value;
                break;
            }
        }

        $probeHost = (string) config('edge.mqtt.host', '127.0.0.1');
        $probePort = (int) config('edge.mqtt.port', 1883);

        $host = (string) ($cfg['host'] ?? '(未读到)');
        $port = (int) ($cfg['port'] ?? 0);
        $clientId = (string) ($cfg['client_id'] ?? '(空)');
        $protocol = (string) ($cfg['protocol'] ?? '(默认 MQTT_3_1)');
        $clean = $cfg['use_clean_session'] ?? '(默认 true)';
        $settings = $cfg['connection_settings'] ?? [];
        $tls = $settings['tls']['enabled']
            ?? $settings['tls_enabled']
            ?? $settings['use_tls']
            ?? '(未配置)';

        fwrite(STDERR, sprintf(
            "[MQTT] 配置 | 探针 tcp://%s:%d | facade %s:%d | client_id=%s | protocol=%s"
            . " | clean_session=%s | tls=%s\n",
            $probeHost,
            $probePort,
            $host,
            $port,
            $clientId,
            $protocol,
            is_bool($clean) ? ($clean ? 'true' : 'false') : (string) $clean,
            is_bool($tls) ? ($tls ? 'true' : 'false') : (string) $tls,
        ));

        if ($host !== $probeHost || $port !== $probePort) {
            fwrite(STDERR,
                "[MQTT] 警告：探针地址与 facade 连接地址不一致！"
                . "TCP 预检只做三次握手，不能证明 MQTT 握手可用。"
                . "请核对 config/mqtt-client.php 与 config/edge.php。\n",
            );
        }

        if ($clientId !== '' && $clientId !== '(空)' && ! str_contains($clientId, (string) getmypid())) {
            fwrite(STDERR, sprintf(
                "\033[33m[MQTT] 提示：client_id=%s[超管用户]将在容器启动时缓存配置快照，必须先创建此超管缓存账号，并修改配置文件的client_id，否则无法启动任务!\033[0m\n",
                $clientId,
            ));
        }
    }

    /**
     * TCP 预检。失败时内部完成退避并返回 false。
     */
    private function precheckTcp(int &$retryDelay, int $maxRetryDelay): bool
    {
        $host = config('edge.mqtt.host', '127.0.0.1');
        $port = (int) config('edge.mqtt.port', 1883);

        $sock = @stream_socket_client(
            "tcp://{$host}:{$port}",
            $errno,
            $errstr,
            3,
        );

        if ($sock !== false) {
            fclose($sock);
            $this->lastPrecheckFail = 0;
            return true;
        }

        if ((time() - $this->lastPrecheckFail) > 30) {
            fwrite(STDERR, sprintf(
                "[MQTT] 预检 %s:%d 不可达（%s），静默等待...\n",
                $host,
                $port,
                $errstr ?: 'unknown',
            ));
            $this->lastPrecheckFail = time();
        }

        $this->clearConnection();
        swow_co_sleep((float) min($retryDelay, 10));
        $retryDelay = min($retryDelay * 2, $maxRetryDelay);

        return false;
    }

    /**
     * 事件循环守护。
     *
     * loop() 提前返回 ≠ 连接断开。真正断线时 loop() 会抛异常，由上层 catch 接管。
     *
     * @return string stopped|disconnected|idle|loop-unstable
     */
    private function runEventLoop(MqttClient $mqtt): string
    {
        $quickReturns = 0;

        while (! $this->stopping) {
            $loopStart = microtime(true);

            $mqtt->loop(true, false);

            $elapsed = microtime(true) - $loopStart;

            if ($this->stopping) {
                return 'stopped';
            }

            if (! $this->safeIsConnected($mqtt)) {
                return 'disconnected';
            }

            if ((microtime(true) - $this->lastActivityAt) >= self::IDLE_TIMEOUT_SEC) {
                fwrite(STDERR, sprintf(
                    "[MQTT] 连接假死（%.0fs 无消息），主动重建...\n",
                    microtime(true) - $this->lastActivityAt,
                ));
                return 'idle';
            }

            if ($elapsed < 0.1) {
                $quickReturns++;
                if ($quickReturns >= 10) {
                    fwrite(STDERR, "[MQTT] loop 连续快速返回，客户端状态异常，主动重建...\n");
                    return 'loop-unstable';
                }
            } else {
                $quickReturns = 0;
            }

            swow_co_sleep(0.05);
        }

        return 'stopped';
    }

    private function safeIsConnected(MqttClient $mqtt): bool
    {
        try {
            if (method_exists($mqtt, 'isConnected')) {
                return (bool) $mqtt->isConnected();
            }
        } catch (Throwable) {
            return false;
        }

        return true;
    }

    private function safeGetClientId(MqttClient $mqtt): string
    {
        try {
            return (string) $mqtt->getClientId();
        } catch (Throwable) {
            return '(unknown)';
        }
    }

    /**
     * 打印完整异常链。ConnectingToBrokerFailedException 会包装底层的
     * DataTransferException，只打最外层会丢掉「读 CONNACK 得到 EOF」这类关键信息。
     */
    private function formatException(Throwable $e): string
    {
        $msg = sprintf('%s(code=%d): %s', get_class($e), $e->getCode(), $e->getMessage());

        $prev = $e->getPrevious();
        $depth = 0;
        while ($prev !== null && $depth++ < 3) {
            $msg .= sprintf(
                ' | caused by %s(code=%d): %s',
                get_class($prev),
                $prev->getCode(),
                $prev->getMessage(),
            );
            $prev = $prev->getPrevious();
        }

        return $msg;
    }

    /**
     * 稳定连接（持续 >= 10s）重置为 1s；短连接 / 建连失败则指数退避至 30s。
     */
    private function backoffAfterDisconnect(
        float $connectedAt,
        int &$retryDelay,
        int $maxRetryDelay,
        string $reason,
    ): void {
        $duration = $connectedAt > 0.0 ? microtime(true) - $connectedAt : 0.0;

        if ($duration >= 10.0) {
            $retryDelay = 1;
        } else {
            $retryDelay = min($retryDelay * 2, $maxRetryDelay);
        }

        fwrite(STDERR, sprintf(
            "\033[31m[MQTT] 连接中断（持续 %.1fs，原因=%s），%ds 后重连...\033[0m\n",
            $duration,
            $reason,
            $retryDelay,
        ));
        swow_co_sleep((float) $retryDelay);
    }

    /**
     * 释放连接。不调用 interrupt()，避免污染 facade 缓存实例。
     * MQTT::disconnect() 会 unset ConnectionManager 里的缓存，下一轮自动新建。
     */
    private function clearConnection(): void
    {
        $this->connection = null;

        try {
            MQTT::disconnect();
        } catch (Throwable) {
            // ignore
        }
    }

    public function wait(): void
    {
        try {
            $this->exitChannel->pop();
        } catch (Throwable) {
            // ignore
        }
    }

    public function disconnect(): void
    {
        $this->stop();
    }

    public function isRunning(): bool
    {
        return $this->coroutine?->isAlive() ?? false;
    }

    public function stop(): void
    {
        $this->stopping = true;

        if ($this->connection !== null) {
            try {
                $this->connection->interrupt();
            } catch (Throwable) {
                // ignore
            }
        }

        $this->clearConnection();
    }
}
