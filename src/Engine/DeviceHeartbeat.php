<?php

declare(strict_types=1);

namespace DagaSmart\Edge\Engine;

use Illuminate\Support\Facades\DB;
use Throwable;

class DeviceHeartbeat
{
    public function insertBatch(array $rows): int
    {
        if ($rows === []) {
            return 0;
        }

        // 财务/设备数据用事务，保证原子性
        return DB::transaction(function () use ($rows) {
            $sql = <<<'SQL'
                INSERT INTO device_heartbeat (sn, topic, payload, received_at, created_at)
                VALUES (:sn, :topic, :payload, :received_at, NOW())
            SQL;

            $count = 0;
            foreach ($rows as $row) {
                try {
                    DB::insert($sql, [
                        'device_sn'          => $row['sn'],
                        'topic'       => $row['topic'],
                        'payload'     => $row['payload'], // 已是 JSON 字符串
                        'received_at' => $row['received_at'],
                    ]);
                    $count++;
                } catch (Throwable $e) {
                    // 单条失败不影响整体，记录后可进死信
                    fwrite(STDERR, '[PG] insert failed: '.$e->getMessage().PHP_EOL);
                }
            }

            return $count;
        });
    }
}
