<?php

declare(strict_types=1);

namespace DagaSmart\Edge\Engine;

use PhpMqtt\Client\Facades\MQTT;
use PhpMqtt\Client\MqttClient;
use Swow\Channel;
use Swow\Coroutine;
use Throwable;

class NanoMQ
{
    private ?Coroutine $coroutine = null;

    private Channel $exitChannel;

    private ?MqttClient $connection = null;

    private bool $stopping = false;

    public function __construct()
    {
        $this->exitChannel = new Channel(1);
    }

    public function publish(string $topic, string $message): static
    {
        try {
            MQTT::publish($topic, $message);
        } catch (Throwable $e) {
            fwrite(STDERR, '[MQTT] publish 异常：'.$e->getMessage().PHP_EOL);
        }

        return $this;
    }

    /**
     * 单 topic 订阅（向后兼容）
     */
    public function subscribe(string $topic, callable $callback): void
    {
        $this->subscribeMultiple([$topic], $callback);
    }

    /**
     * 多 topic 订阅（核心方法，纯 Facade）
     */
    public function subscribeMultiple(array $topics, callable $callback): void
    {
        $this->coroutine = Coroutine::run(function () use ($topics, $callback): void {
            try {
                $mqtt = $this->connection = MQTT::connection();

                fwrite(STDERR, '[MQTT] 连接成功 | client_id='.$mqtt->getClientId()
                    .' | 订阅 topics='.implode(', ', $topics).PHP_EOL);

                foreach ($topics as $t) {
                    $mqtt->subscribe($t, function (string $topic, string $message) use ($callback): void {
                        try {
                            call_user_func($callback, $topic, $message);
                        } catch (Throwable $e) {
                            fwrite(STDERR, '[MQTT] 回调异常：'.$e->getMessage().PHP_EOL);
                        }
                    }, 0);
                }

                $mqtt->loop(true, true);

            } catch (Throwable $e) {
                if ($this->stopping) {
                    return;
                }
                if (! str_contains($e->getMessage(), 'feof()')) {
                    fwrite(STDERR, '[MQTT] 订阅异常：'.$e->getMessage().PHP_EOL);
                }
            } finally {
                $this->connection = null;
                try {
                    $this->exitChannel->push(true);
                } catch (Throwable) {
                    // ignore
                }
            }
        });
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
        $conn = $this->connection;
        if ($conn === null) {
            return;
        }
        try {
            $conn->disconnect();
        } catch (Throwable) {
            // ignore
        }
        $this->connection = null;
    }

    public function isRunning(): bool
    {
        return $this->coroutine?->isAlive() ?? false;
    }

    public function stop(): void
    {
        $this->stopping = true;
        $this->disconnect();
    }
}
