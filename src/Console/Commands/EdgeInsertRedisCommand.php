<?php

declare(strict_types=1);

namespace DagaSmart\Edge\Console\Commands;

use DagaSmart\Edge\Models\EdgeDataStream;
use DagaSmart\Edge\Models\EdgeDataStreamFail;
use DateTimeImmutable;
use Illuminate\Console\Command;
use Swow\Buffer;
use Swow\Channel;
use Swow\ChannelException;
use Swow\Coroutine;
use Swow\Signal;
use Swow\Socket;
use Throwable;

$edgeRoot = dirname(__DIR__, 3);

require_once $edgeRoot . '/bin/bootstrap.php';
require_once $edgeRoot . '/bin/helper.php';

final class EdgeInsertRedisCommand extends Command
{
    protected $signature = 'edge:insert-redis';
    protected $description = '订阅 NanoMQ Topic: heartbeat/response，批量写入 Redis Stream（QoS1 PUBACK + 死信 + PG 入库）';

    private const string MQTT_HOST = '1.15.51.149';
    private const int MQTT_PORT = 21883;
    private const string STREAM = 'stream:device:data';
    private const string DEAD_STREAM = 'stream:device:dead';
    private const string PG_GROUP = 'edge-ingest';
    private const string PG_CONSUMER = 'worker-1';
    private const int MAX_LEN = 50000;
    private const int DEAD_MAX_LEN = 10000;
    private const int RECONNECT_BASE_DELAY = 1;
    private const int RECONNECT_MAX_DELAY = 30;
    private const int BATCH_SIZE = 100;
    private const float BATCH_TIMEOUT = 0.05;
    private const int PG_BATCH_SIZE = 200;
    private const int PG_BLOCK_MS = 1000;
    private const int KEEPALIVE = 60;
    private const int PING_INTERVAL = 30;
    private const int SOCKET_TIMEOUT_USEC = 1000000; // 1s

    private Channel $batchChannel;
    private bool $stopping = false;

    public function handle(): int
    {
        $this->info('MQTT → Redis Stream 启动，PID=' . getmypid());

        $this->initRedisPool();
        $this->batchChannel = new Channel(self::BATCH_SIZE * 4);

        // 批量刷写协程
        Coroutine::run(function (): void {
            $batch = [];
            while (!$this->stopping || !$this->batchChannel->isEmpty()) {
                try {
                    $item = $this->batchChannel->pop((int) (self::BATCH_TIMEOUT * 1000000));
                } catch (ChannelException) {
                    break;
                }
                if ($item !== null) {
                    $batch[] = $item;
                }
                if (count($batch) >= self::BATCH_SIZE) {
                    $this->flushBatch($batch);
                    $batch = [];
                }
            }
            if ($batch !== []) {
                $this->flushBatch($batch);
            }
        });

        // PG 消费协程
        Coroutine::run(function (): void {
            $this->runPgConsumer();
        });

        // 信号监听
        foreach ([Signal::INT, Signal::TERM] as $sig) {
            Coroutine::run(function () use ($sig): void {
                Signal::wait($sig);
                $this->info('收到退出信号，准备优雅停机');
                $this->stopping = true;
            });
        }

        try {
            $this->runMqttLoop();
        } finally {
            $this->stopping = true;
            try {
                $this->batchChannel->close();
            } catch (Throwable) {
                // 已关闭
            }
        }

        $this->info('已停止');
        return 0;
    }

    /* ==================== MQTT 主循环 ==================== */

    private function runMqttLoop(): void
    {
        while (!$this->stopping) {
            try {
                $socket = $this->connectMqtt();
            } catch (Throwable $e) {
                $this->error('无法建立 MQTT 连接：' . $e->getMessage());
                usleep(self::RECONNECT_BASE_DELAY * 1000000);
                continue;
            }

            $buffer = '';
            $lastPing = time();
            $readBuf = new Buffer(65535);

            while (!$this->stopping) {
                try {
                    $packet = $this->readMqttPacket($socket, $readBuf, $buffer);
                } catch (Throwable) {
                    $this->warn('MQTT 读取异常，准备重连');
                    break;
                }

                if ($packet === null) {
                    if (time() - $lastPing >= self::PING_INTERVAL) {
                        try {
                            $this->sendPing($socket);
                            $lastPing = time();
                        } catch (Throwable) {
                            break;
                        }
                    }
                    continue;
                }

                $this->dispatchPacket($socket, $packet);
                $lastPing = time();
            }

            try {
                $socket->close();
            } catch (Throwable) {
                // ignore
            }

            if (!$this->stopping) {
                $this->warn('MQTT 连接断开，准备重连');
                usleep(self::RECONNECT_BASE_DELAY * 1000000);
            }
        }
    }

    private function readMqttPacket(Socket $socket, Buffer $readBuf, string &$buffer): ?string
    {
        while (!$this->stopping) {
            $len = $this->tryParsePacketLength($buffer);
            if ($len !== null && $len <= strlen($buffer)) {
                $packet = substr($buffer, 0, $len);
                $buffer = substr($buffer, $len);

                // ===== 暴力调试：打印每一个完整包的十六进制 =====
                $hex = implode(' ', array_map(fn($c) => sprintf('%02X', ord($c)), str_split(substr($packet, 0, min(20, strlen($packet))))));
                $type = ord($packet[0]) >> 4;
                $this->line(sprintf('[PKT] type=%d(%s) len=%d header=%s',
                    $type,
                    match($type) {
                        1 => 'CONNECT', 2 => 'CONNACK', 3 => 'PUBLISH', 4 => 'PUBACK',
                        8 => 'SUBSCRIBE', 9 => 'SUBACK', 12 => 'PINGREQ', 13 => 'PINGRESP',
                        default => 'UNKNOWN'
                    },
                    $len, $hex
                ));
                // ==============================================

                return $packet;
            }

            $readBuf->clear();
            try {
                $n = $socket->read($readBuf, 0);
            } catch (Throwable $e) {
                throw new MQTTConnectionException('读取失败：' . $e->getMessage());
            }

            if ($n === 0) {
                return null;
            }

            $chunk = $readBuf->toString();
            $buffer .= $chunk;

            // ===== 打印每次 read 到的原始字节数 =====
            $this->line('[READ] bytes=' . strlen($chunk) . ' total_buffer=' . strlen($buffer));
            // ======================================
        }
        return null;
    }

    private function tryParsePacketLength(string $buffer): ?int
    {
        if ($buffer === '') {
            return null;
        }
        $multiplier = 1;
        $pos = 1;
        $remaining = 0;
        while (true) {
            if (!isset($buffer[$pos])) {
                return null;
            }
            $byte = ord($buffer[$pos++]);
            $remaining += ($byte & 127) * $multiplier;
            $multiplier *= 128;
            if (($byte & 128) === 0) {
                break;
            }
            if ($multiplier > 128 ** 3) {
                throw new MQTTProtocolException('Remaining Length 编码非法');
            }
        }
        return $pos + $remaining;
    }

    private function dispatchPacket(Socket $socket, string $packet): void
    {
        $type = ord($packet[0]) >> 4;

        if ($type === 3) {
            $this->handlePublish($socket, $packet);
        }
    }

    private function handlePublish(Socket $socket, string $packet): void
    {
        $msg = $this->parsePublish($packet);
        if ($msg === null) {
            return;
        }

        if ($msg) {
            $this->line(sprintf(
                '[MQTT] topic=%s qos=%d payload=%s',
                $msg['topic'],
                $msg['qos'],
                $msg['payload']
            ));
        }

        // ===== 调试：打印原始 MQTT PUBLISH =====
        $this->line(sprintf(
            '[MQTT] topic=%s qos=%d packet_id=%s payload_len=%d payload=%s',
            $msg['topic'],
            $msg['qos'],
            $msg['packet_id'] ?? '-',
            strlen($msg['payload']),
            $msg['payload']
        ));
        // =====================================

        if ($msg['qos'] === 1 && $msg['packet_id'] !== null) {
            try {
                $this->sendPubAck($socket, $msg['packet_id']);
            } catch (Throwable) {
                $this->warn('PUBACK 发送失败');
            }
        }

        // topic: device/{client_id}/...
        $parts = explode('/', $msg['topic'], 3);
        $clientId = $parts[1] ?? 'unknown';

        $this->pushToBatch([
            'client_id' => $clientId,
            'topic' => $msg['topic'],
            'payload' => $msg['payload'],
            'ts' => microtime(true),
        ]);
    }

    /* ==================== MQTT 连接 ==================== */

    private function connectMqtt(): Socket
    {
        $attempt = 0;
        $delay = self::RECONNECT_BASE_DELAY;

        while (!$this->stopping) {
            try {
                $socket = new Socket(Socket::TYPE_TCP);
                $socket->setReadTimeout(self::SOCKET_TIMEOUT_USEC);
                $socket->setWriteTimeout(self::SOCKET_TIMEOUT_USEC);
                $socket->connect(self::MQTT_HOST, self::MQTT_PORT);

                $this->sendConnect($socket);
                $this->readConnectAck($socket);
                $this->sendSubscribe($socket);
                $this->readSubscribeAck($socket);

                if ($attempt > 0) {
                    $this->warn('MQTT 重连成功');
                } else {
                    $this->info('MQTT 已连接并订阅 Topic: heartbeat/response');
                }
                return $socket;

            } catch (Throwable $e) {
                $attempt++;
                $this->error(sprintf(
                    'MQTT 连接失败（第 %d 次）：%s，%d 秒后重试',
                    $attempt,
                    $e->getMessage(),
                    $delay
                ));
                usleep($delay * 1000000);
                $delay = min($delay * 2, self::RECONNECT_MAX_DELAY);
            }
        }

        throw new MQTTConnectionException('MQTT 客户端已停止');
    }

    private function sendConnect(Socket $socket): void
    {
        // 加上 PID 和随机数，确保每次唯一
        $clientId = 'laravel-edge-admin-' . getmypid() . '-' . random_int(1000, 9999);
        $username = 'admin';
        $password = '123456';

        $var = pack('n', 4) . 'MQTT' . chr(4);
        $flags = 0b00000010 | 0b10000000 | 0b01000000; // Clean + User + Pass
        $var .= chr($flags) . pack('n', self::KEEPALIVE);

        $payload = pack('n', strlen($clientId)) . $clientId;
        $payload .= pack('n', strlen($username)) . $username;
        $payload .= pack('n', strlen($password)) . $password;

        $this->writePacket($socket, 0x10, $var . $payload);
    }

    private function readConnectAck(Socket $socket): void
    {
        $resp = $this->readExact($socket, 4);
        if (ord($resp[0]) !== 0x20) {
            throw new MQTTProtocolException('未收到 CONNACK');
        }
        $rc = ord($resp[3]);
        if ($rc !== 0) {
            throw new MQTTProtocolException('MQTT 认证失败，return_code=' . $rc);
        }
    }

    private function sendSubscribe(Socket $socket): void
    {
        $pid = random_int(1, 65535);
        $topic = 'heartbeat/response';
        $payload = pack('n', $pid) . pack('n', strlen($topic)) . $topic . chr(0);
        $this->writePacket($socket, 0x82, $payload);
    }

    private function readSubscribeAck(Socket $socket): void
    {
        $data = $socket->readString(5);
        if (strlen($data) < 5 || ((ord($data[0]) >> 4) !== 9)) {
            throw new MQTTProtocolException('未收到 SUBACK');
        }
    }

    private function sendPing(Socket $socket): void
    {
        $this->writePacket($socket, 0xC0, '');
    }

    private function sendPubAck(Socket $socket, int $pid): void
    {
        $this->writePacket($socket, 0x40, pack('n', $pid));
    }

    private function writePacket(Socket $socket, int $type, string $payload): void
    {
        $frame = chr($type) . $this->encodeLength(strlen($payload)) . $payload;
        $this->writeExact($socket, $frame);
    }

    /**
     * Swow 2: write(array $vector, int $offset=0)
     */
    private function writeExact(Socket $socket, string $data): void
    {
        if ($data === '') {
            return;
        }
        $socket->write([$data]);
    }

    private function readExact(Socket $socket, int $length): string
    {
        $result = '';
        while (strlen($result) < $length) {
            $need = $length - strlen($result);
            $chunk = $socket->readString($need);
            if ($chunk === '') {
                throw new MQTTConnectionException('读取超时');
            }
            $result .= $chunk;
        }
        return $result;
    }

    private function encodeLength(int $len): string
    {
        $out = '';
        do {
            $byte = $len % 128;
            $len = intdiv($len, 128);
            if ($len > 0) {
                $byte |= 0x80;
            }
            $out .= chr($byte);
        } while ($len > 0);
        return $out;
    }

    private function parsePublish(string $packet): ?array
    {
        if (strlen($packet) < 3) {
            return null;
        }

        $pos = 1;
        $multiplier = 1;
        $remaining = 0;
        while (true) {
            if (!isset($packet[$pos])) {
                return null;
            }
            $byte = ord($packet[$pos++]);
            $remaining += ($byte & 127) * $multiplier;
            $multiplier *= 128;
            if (($byte & 128) === 0) {
                break;
            }
        }

        if (strlen($packet) < $pos + 2) {
            return null;
        }

        $topicLen = unpack('n', $packet[$pos] . $packet[$pos + 1])[1];
        $pos += 2;

        if (strlen($packet) < $pos + $topicLen) {
            return null;
        }

        $topic = substr($packet, $pos, $topicLen);
        $pos += $topicLen;

        $flags = ord($packet[0]);
        $qos = ($flags >> 1) & 0x03;
        $packetId = null;

        if ($qos > 0) {
            if (strlen($packet) < $pos + 2) {
                return null;
            }
            $packetId = unpack('n', $packet[$pos] . $packet[$pos + 1])[1];
            $pos += 2;
        }

        return [
            'topic' => $topic,
            'payload' => substr($packet, $pos),
            'qos' => $qos,
            'packet_id' => $packetId,
        ];
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
        \redisPoolInit((int) (\REDIS_POOL_SIZE ?? 64), (int) (\REDIS_DB ?? 2));
    }

    private function pushToBatch(array $fields): void
    {
        if ($this->stopping) {
            return;
        }
        try {
            $this->batchChannel->push($fields, 0);
        } catch (ChannelException) {
            $this->writeSingle($fields);
        }
    }

    private function flushBatch(array $batch): void
    {
        if (empty($batch)) {
            return;
        }

        $results = $this->batchXaddWithRetry($batch);
        foreach ($results as $i => $id) {
            if ($id === null && isset($batch[$i])) {
                $this->writeDeadLetter($batch[$i], 'xadd_failed_after_retry');
            }
        }
    }

    private function writeSingle(array $fields): void
    {
        if (!$this->xaddWithRetry($fields)) {
            $this->writeDeadLetter($fields, 'xadd_single_failed');
        }
    }

    private function xaddWithRetry(array $fields, int $attempts = 2): bool
    {
        while ($attempts-- > 0) {
            try {
                $ok = withRedis(function ($redis) use ($fields) {
                    $id = $redis->xAdd(self::STREAM, '*', $fields, self::MAX_LEN, true);
                    return is_string($id) && $id !== '';
                });
                if ($ok) {
                    return true;
                }
            } catch (Throwable $e) {
                $this->error('XADD 失败：' . $e->getMessage());
            }
            if ($attempts > 0) {
                usleep(10000);
            }
        }
        return false;
    }

    /**
     * @return array<int,?string>
     */
    private function batchXaddWithRetry(array $batch, int $attempts = 2): array
    {
        while ($attempts-- > 0) {
            try {
                return withRedis(function ($redis) use ($batch) {
                    $redis->pipeline();
                    foreach ($batch as $f) {
                        $redis->xAdd(self::STREAM, '*', $f, self::MAX_LEN, true);
                    }
                    $executed = $redis->exec();
                    $result = [];
                    $i = 0;
                    foreach ($batch as $f) {
                        $v = $executed[$i++] ?? false;
                        $result[] = is_string($v) && $v !== '' ? $v : null;
                    }
                    return $result;
                });
            } catch (Throwable $e) {
                $this->error('Pipeline XADD 失败：' . $e->getMessage());
            }
            if ($attempts > 0) {
                usleep(10000);
            }
        }
        return array_fill(0, count($batch), null);
    }

    private function writeDeadLetter(array $fields, string $reason): void
    {
        $fields['dead_reason'] = $reason;
        $fields['dead_at'] = microtime(true);
        try {
            withRedis(fn($redis) => $redis->xAdd(
                self::DEAD_STREAM, '*', $fields, self::DEAD_MAX_LEN, true
            ));
        } catch (Throwable $e) {
            $this->error('死信 Stream 写入失败：' . $e->getMessage());
        }
    }

    /* ==================== PG 消费（对齐 mcs_edge_data_stream） ==================== */

    private function runPgConsumer(): void
    {
        while (!$this->stopping) {
            try {
                $this->ensureStreamGroup();
                $msgs = $this->readPgBatch();
                if (empty($msgs)) {
                    usleep(200000);
                    continue;
                }
                $this->insertToPg($msgs);
            } catch (Throwable $e) {
                $this->error('PG 消费者异常：' . $e->getMessage());
                usleep(1000000);
            }
        }
    }

    private function ensureStreamGroup(): void
    {
        try {
            withRedis(fn($redis) => $redis->xGroup(
                'CREATE', self::STREAM, self::PG_GROUP, '0', true
            ));
        } catch (Throwable) {
            // 已存在
        }
    }

    private function readPgBatch(): array
    {
        $raw = withRedis(fn($redis) => $redis->xReadGroup(
            self::PG_GROUP,
            self::PG_CONSUMER,
            [self::STREAM => '>'],
            self::PG_BATCH_SIZE,
            self::PG_BLOCK_MS
        ));

        if (!is_array($raw) || !isset($raw[self::STREAM])) {
            return [];
        }

        $msgs = [];
        foreach ($raw[self::STREAM] as $id => $fields) {
            if (!is_array($fields)) {
                continue;
            }
            $normalized = [];
            foreach ($fields as $k => $v) {
                $normalized[(string) $k] = (string) $v;
            }
            $msgs[] = ['id' => (string) $id, 'fields' => $normalized];
        }
        return $msgs;
    }

    private function insertToPg(array $messages): void
    {
        if (empty($messages)) {
            return;
        }

        $now = now();
        $rows = [];

        foreach ($messages as $m) {
            $f = $m['fields'];

            $payloadRaw = $f['payload'] ?? '';
            $decoded = json_decode($payloadRaw, true);
            $payload = (is_array($decoded) || is_object($decoded))
                ? $decoded
                : ['raw' => $payloadRaw];

            $ts = isset($f['ts']) ? (float) $f['ts'] : microtime(true);
            $receivedMs = (int) ($ts * 1000);
            $receivedAt = DateTimeImmutable::createFromFormat(
                'U.u', sprintf('%.6f', $ts)
            );

            $rows[] = [
                'client_id'   => $f['client_id'] ?? 'unknown',
                'topic'       => $f['topic'] ?? '',
                'payload'     => $payload,
                'received_ms' => $receivedMs,
                'received_at' => $receivedAt,
                'created_at'  => $now,
            ];
        }


        $this->line('[PG] insertOrIgnore count=' . count($rows) . ' first=' . json_encode($rows[0] ?? null, JSON_UNESCAPED_UNICODE | JSON_INVALID_UTF8_SUBSTITUTE));


        // ON CONFLICT DO NOTHING，幂等
        EdgeDataStream::query()->insertOrIgnore($rows);

        $ids = array_column($messages, 'id');

        $this->line('[PG] ack+del ids=' . implode(',', $ids));

        withRedis(function ($redis) use ($ids) {
            $redis->xAck(self::STREAM, self::PG_GROUP, $ids);
            $redis->xDel(self::STREAM, $ids);
        });
    }

    private function writeDeadLetterToFail(array $fields, string $reason): void
    {
        try {
            $ts = isset($fields['ts']) ? (float) $fields['ts'] : microtime(true);
            $receivedMs = (int) ($ts * 1000);
            $receivedAt = DateTimeImmutable::createFromFormat(
                'U.u', sprintf('%.6f', $ts)
            );

            $payloadRaw = $fields['payload'] ?? '';
            $decoded = json_decode($payloadRaw, true);
            $payload = (is_array($decoded) || is_object($decoded))
                ? $decoded
                : ['raw' => $payloadRaw, '_dead_reason' => $reason];

            EdgeDataStreamFail::create([
                'client_id'   => $fields['client_id'] ?? 'unknown',
                'topic'       => $fields['topic'] ?? '',
                'payload'     => $payload,
                'received_ms' => $receivedMs,
                'received_at' => $receivedAt,
                'created_at'  => now(),
            ]);
        } catch (Throwable $e) {
            $this->error('死信写入 fail 表失败：' . $e->getMessage());
        }
    }
}

final class MQTTConnectionException extends \RuntimeException {}
final class MQTTProtocolException extends \RuntimeException {}
