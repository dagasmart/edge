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

    /**
     * 缓存 MQTT 连接实例，确保 subscribe() 与 disconnect() 操作的是同一个对象
     */
    private ?MqttClient $connection = null;

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

    public function subscribe(string $topic, callable $callback): void
    {
        $this->coroutine = Coroutine::run(function () use ($topic, $callback): void {
            try {
                $mqtt = $this->connection = MQTT::connection();

                $mqtt->subscribe($topic, function (
                    string $topic,
                    string $message,
                ) use ($callback): void {
                    try {
                        call_user_func($callback, $topic, $message);
                    } catch (Throwable $e) {
                        fwrite(STDERR, '[MQTT] 回调异常：'.$e->getMessage().PHP_EOL);
                    }
                }, 0);

                $mqtt->loop(true, true);
            } catch (Throwable $e) {
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
    }

    public function isRunning(): bool
    {
        return $this->coroutine?->isAlive() ?? false;
    }
}
