<?php
declare(strict_types=1);
namespace DagaSmart\Edge\Console\Commands;

use DagaSmart\Edge\Facades\EdgeAuth;
use Illuminate\Console\Command;
use Illuminate\Support\Facades\Redis;

class EdgeDeviceSecretBatchCommand extends Command
{
    protected $signature = 'edge:device-secret-batch
        {--count=100 : 生成设备数量}
        {--prefix=device : 设备ID前缀}
        {--secret-len=32 : 密钥长度（字节）}
        {--db=2 : Redis 库号}
        {--ttl=0 : 密钥过期时间（秒，0=不过期）}
        {--dry-run : 只打印不写入}
        {--start=1 : 起始序号}
        {--batch-size=50 : 每批写入数量}';

    protected $description = '批量生成设备密钥并写入 Redis';

    public function handle(): int
    {
        $count     = (int)$this->option('count');
        $prefix    = $this->option('prefix');
        $secLen    = (int)$this->option('secret-len');
        $db        = (int)$this->option('db');
        $ttl       = (int)$this->option('ttl');
        $dryRun    = (bool)$this->option('dry-run');
        $start     = (int)$this->option('start');
        $batchSize = (int)$this->option('batch-size');

        if ($count <= 0 || $count > 100000) {
            $this->error('count 范围: 1 ~ 100000');
            return self::FAILURE;
        }

        $this->info('=== Edge Device Secret Batch ===');
        $this->line("数量: {$count} | 前缀: {$prefix} | 密钥长度: {$secLen}");
        $this->line("Redis DB: {$db} | TTL: " . ($ttl > 0 ? "{$ttl}s" : '不过期'));
        $this->line("Dry Run: " . ($dryRun ? 'YES' : 'NO'));
        $this->newLine();

        $redis = Redis::connection()->client();
        if (method_exists($redis, 'select')) {
            $redis->select($db);
        }

        $bar = $this->output->createProgressBar($count);
        $bar->start();

        $written = 0;
        $samples = [];

        for ($i = 0; $i < $count; $i += $batchSize) {
            $current = min($batchSize, $count - $i);

            for ($j = 0; $j < $current; $j++) {
                $seq      = $start + $i + $j;
                $clientId = "{$prefix}-{$seq}";
                $secret   = $this->genSecret($secLen);
                $key      = "device:secret:{$clientId}";

                if (!$dryRun) {
                    try {
                        if ($ttl > 0) {
                            $redis->setex($key, $ttl, $secret);
                        } else {
                            $redis->set($key, $secret);
                        }
                        $written++;
                    } catch (\Throwable $e) {
                        $this->warn("\n写入失败 {$clientId}");
                    }
                }

                if (count($samples) < 3) {
                    $samples[] = ['clientId' => $clientId, 'secret' => $secret];
                }
                $bar->advance();
            }

            if ($i + $batchSize < $count) {
                usleep(10000);
            }
        }

        $bar->finish();
        $this->newLine(2);

        if ($dryRun) {
            $this->warn('DRY RUN — 未写入');
        } else {
            $this->info("写入成功: {$written}");
        }

        // ---------- 签名示例（统一用 MqttAuth Facade） ----------
        if (!empty($samples)) {
            $this->newLine();
            $this->line('<fg=cyan>=== 签名示例 ===</>');

            $s = $samples[0];
            $records = EdgeAuth::sign($s['clientId'], $s['secret']);

            $this->line("clientId: <fg=yellow>{$records['client_id']}</>");
            $this->line("password: <fg=green>{$records['password']}</>");
            $this->newLine();
            $this->line('curl 测试:');
            $this->line("<fg=gray>curl.exe -X POST http://127.0.0.1:9503/mqtt/auth \\</>");
            $this->line("<fg=gray>  -d \"clientid={$records['client_id']}&username={$records['username']}&password={$records['password']}\"</>");
        }

        $this->newLine();
        $this->info('=== Done ===');
        return self::SUCCESS;
    }

    private function genSecret(int $len): string
    {
        return rtrim(strtr(base64_encode(random_bytes($len)), '+/', '-_'), '=');
    }
}
