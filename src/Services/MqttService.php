<?php
declare(strict_types=1);
namespace DagaSmart\Edge\Services;

use PhpMqtt\Client\Facades\MQTT;
use Illuminate\Console\Command;

class MqttService
{
    protected Command $log;
    protected MQTT $client;

    public function __construct()
    {
        $this->log = new Command;
        $this->client = new Mqtt();
        return $this;
    }

    public function publish($topic, $message): static
    {
        $this->client::publish($topic, $message);
        return $this;
    }

    public function subscribe($topic, callable $callback): void
    {
        try {
            $mqtt = $this->client::connection();
            //协程
            \Swow\Coroutine::run(static function () use($mqtt, $topic, $callback): void {
                $mqtt->subscribe($topic, function (string $topic, string $message) use ($callback) {
                    call_user_func($callback, $topic, $message); //动态回调
                }, 0);
                $mqtt->loop(true, true);
            });
            //$mqtt->loop(true, true);
            \Swow\Sync\waitAll();//等待协程结束
        } catch (\Throwable $e) {
            echo $e->getMessage();
        }
    }

    public function disconnect(): void
    {
        $this->client->disconnect();
    }

}
