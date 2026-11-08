<?php

declare(strict_types=1);

namespace DagaSmart\Edge\Engine;

use PhpMqtt\Client\Facades\MQTT;
use PhpMqtt\Client\MqttClient;
use PhpMqtt\Client\Exceptions\MqttClientException;
use Swow\Channel;
use Swow\Coroutine;
use Throwable;

class NanoMQ
{
    private ?Coroutine $coroutine = null;

    private Channel $exitChannel;

    private ?MqttClient $connection = null;

    private bool $stopping = false;

    private int $lastPrecheckFail = 0;

    public function __construct()
    {
        $this->exitChannel = new Channel(1);
    }

    public function publish(string $topic, string $message): static
    {
        try {
            MQTT::publish($topic, $message);
        } catch (Throwable $e) {
            fwrite(STDERR, '[MQTT] publish 异常：' . $e->getMessage() . PHP_EOL);
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

            while (! $this->stopping) {
                try {
                    // ★ TCP 预检
                    $host = config('edge.mqtt.host', '127.0.0.1');
                    $port = (int) config('edge.mqtt.port', 1883);

                    $sock = @stream_socket_client(
                        "tcp://{$host}:{$port}",
                        $errno,
                        $errstr,
                        3
                    );

                    if ($sock === false) {
                        if ((time() - $this->lastPrecheckFail) > 30) {
                            fwrite(STDERR, sprintf(
                                "[MQTT] 预检 %s:%d 不可达（%s），静默等待...\n",
                                $host, $port, $errstr ?: 'unknown'
                            ));
                            $this->lastPrecheckFail = time();
                        }

                        $this->clearConnection();
                        sleep(min($retryDelay, 10));
                        $retryDelay = min($retryDelay * 2, $maxRetryDelay);
                        continue;
                    }

                    fclose($sock);
                    $this->lastPrecheckFail = 0;

                    // ★ 预检通过，建立 MQTT 连接
                    $mqtt = $this->connection = MQTT::connection();

                    fwrite(STDERR, '[MQTT] 连接成功 | client_id=' . $mqtt->getClientId()
                        . ' | 订阅 topics=' . implode(', ', $topics) . PHP_EOL);

                    foreach ($topics as $t) {
                        $mqtt->subscribe($t, function (string $topic, string $message) use ($callback): void {
                            try {
                                call_user_func($callback, $topic, $message);
                            } catch (Throwable $e) {
                                fwrite(STDERR, '[MQTT] 回调异常：' . $e->getMessage() . PHP_EOL);
                            }
                        }, 0);
                    }

                    $retryDelay = 1;
                    $mqtt->loop(true, true);

                    if ($this->stopping) {
                        break;
                    }

                    $this->clearConnection();

                } catch (MqttClientException $e) {
                    if ($this->stopping) {
                        break;
                    }

                    $msg = $e->getMessage();
                    $code = $e->getCode();

                    // ★ 错误码 1 = CONNECTION_FAILED，静默
                    if ($code === 1 || $code === 66
                        || str_contains($msg, 'reached EOF')
                        || str_contains($msg, 'Transferring data')
                        || str_contains($msg, 'feof()')
                        || str_contains($msg, 'socket')
                        || str_contains($msg, 'data transfer')
                        || str_contains($msg, 'connection')
                    ) {
                        // 静默
                    } else {
                        fwrite(STDERR, '[MQTT] 订阅异常：' . $msg . PHP_EOL);
                    }

                    $this->clearConnection();
                    usleep($retryDelay * 1000000);
                    $retryDelay = min($retryDelay * 2, $maxRetryDelay);

                } catch (Throwable $e) {
                    if ($this->stopping) {
                        break;
                    }

                    $msg = $e->getMessage();

                    if (str_contains($msg, 'feof()')
                        || str_contains($msg, 'socket')
                        || str_contains($msg, 'stream')
                        || str_contains($msg, 'reached EOF')
                        || str_contains($msg, 'Transferring data')
                        || str_contains($msg, 'data transfer')
                    ) {
                        // 静默
                    } else {
                        fwrite(STDERR, '[MQTT] 未知异常：' . $msg . PHP_EOL);
                    }

                    $this->clearConnection();
                    usleep($retryDelay * 1000000);
                    $retryDelay = min($retryDelay * 2, $maxRetryDelay);
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

    private function clearConnection(): void
    {
        $conn = $this->connection;
        $this->connection = null;

        if ($conn !== null) {
            try {
                $conn->interrupt();
            } catch (Throwable) {
                // ignore
            }
        }

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
        $this->clearConnection();
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
