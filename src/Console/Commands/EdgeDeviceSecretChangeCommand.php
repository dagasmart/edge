<?php
declare(strict_types=1);

namespace App\Console\Commands;

use DagaSmart\Edge\Facades\EdgeAuth;
use Illuminate\Console\Command;
use Illuminate\Support\Facades\Redis;
use Throwable;

class EdgeDeviceSecretChangeCommand extends Command
{
    protected $signature = 'edge:device-secret-change
        {clientId? : 设备ID（不填则交互输入）}
        {--secret-len=32 : 新密钥长度（字节）}
        {--grace=300 : 新旧密钥共存过渡期（秒）}
        {--db=2 : Redis 库号}
        {--dry-run : 只显示不执行}
        {--batch-file= : 从文件批量轮换（每行一个clientId）}
        {--prefix= : 按前缀批量轮换}
        {--revoke-old : 过渡期结束后自动吊销旧密钥}';

    protected $description = '轮换设备密钥（支持单设备/批量/前缀模式）';

    public function handle(): int
    {
        $db = (int)$this->option('db');
        $redis = Redis::connection()->client();
        if (method_exists($redis, 'select')) {
            $redis->select($db);
        }

        // 批量文件模式
        if ($batchFile = $this->option('batch-file')) {
            return $this->rotateBatchFromFile($batchFile);
        }

        // 前缀模式
        if ($prefix = $this->option('prefix')) {
            return $this->rotateByPrefix($prefix);
        }

        // 单设备模式
        $clientId = $this->argument('clientId') ?: $this->ask('请输入设备ID');
        if (!$clientId) {
            $this->error('设备ID不能为空');
            return self::FAILURE;
        }

        return $this->rotateSingle($clientId);
    }

    private function rotateSingle(string $clientId): int
    {
        $secretLen = (int)$this->option('secret-len');
        $grace     = (int)$this->option('grace');
        $dryRun    = (bool)$this->option('dry-run');

        $this->info("=== 轮换设备密钥: {$clientId} ===");

        // 1. 读取旧密钥
        $oldSecret = Redis::get("device:secret:{$clientId}");
        if (!$oldSecret && !$dryRun) {
            $this->warn("设备 {$clientId} 没有旧密钥，将直接创建新密钥");
        }

        // 2. 生成新密钥
        $newSecret = $this->generateSecret($secretLen);

        // 3. 显示信息
        $this->line("旧密钥: " . ($oldSecret ? substr($oldSecret, 0, 16) . '...' : '（无）'));
        $this->line("新密钥: " . substr($newSecret, 0, 16) . '...');
        $this->line("过渡期: {$grace}s");

        if ($dryRun) {
            $this->warn('DRY RUN — 未执行');
            $creds = EdgeAuth::sign($clientId, $newSecret);
            $this->line("\n新密钥签名示例:");
            $this->line("password: {$creds['password']}");
            return self::SUCCESS;
        }

        // 4. 执行轮换
        if ($oldSecret && $grace > 0) {
            Redis::setex("device:secret:old:{$clientId}", $grace, $oldSecret);
            $this->line("旧密钥已存入过渡期（{$grace}s）");
        }

        Redis::set("device:secret:{$clientId}", $newSecret);
        $this->info("新密钥已写入");

        Redis::del("auth:hmac:{$clientId}");
        $this->line("认证缓存已清除");

        if ($this->option('revoke-old') && $grace > 0) {
            $this->line("过渡期结束后旧密钥自动失效（Redis key 过期）");
        }

        // 5. 生成签名示例
        $creds = EdgeAuth::sign($clientId, $newSecret);
        $this->newLine();
        $this->line('<fg=cyan>=== 新密钥连接凭据 ===</>');
        $this->line("clientId: {$creds['client_id']}");
        $this->line("username: {$creds['username']}");
        $this->line("password: {$creds['password']}");

        $this->newLine();
        $this->line('curl 验证:');
        $this->line("curl.exe -s -o NUL -w \"%{http_code}\" -X POST http://127.0.0.1:9503/mqtt/auth -d \"clientid={$clientId}&username={$clientId}&password={$creds['password']}\"");

        if ($grace > 0 && $oldSecret) {
            $this->newLine();
            $this->warn("注意：过渡期 {$grace}s 内，新旧密钥均可使用");
            $oldCreds = EdgeAuth::sign($clientId, $oldSecret);
            $this->line("旧密钥仍然有效，password: {$oldCreds['password']}");
        }

        return self::SUCCESS;
    }

    private function rotateBatchFromFile(string $file): int
    {
        if (!file_exists($file)) {
            $this->error("文件不存在: {$file}");
            return self::FAILURE;
        }

        $lines = array_filter(array_map('trim', file($file)));
        $this->info("批量轮换: " . count($lines) . " 个设备");

        $dryRun = (bool)$this->option('dry-run');
        if ($dryRun) {
            $this->warn('DRY RUN 模式 — 不会写入 Redis');
        }

        $bar = $this->output->createProgressBar(count($lines));
        $bar->start();

        $success = 0;
        $failed  = 0;

        foreach ($lines as $clientId) {
            try {
                $this->doRotate($clientId, (int)$this->option('secret-len'), (int)$this->option('grace'), $dryRun);
                $success++;
            } catch (Throwable $e) {
                $failed++;
                $this->warn("\n失败: {$clientId} — " . $e->getMessage());
            }
            $bar->advance();
        }

        $bar->finish();
        $this->newLine(2);
        $this->info("成功: {$success} | 失败: {$failed}");
        return $failed > 0 ? self::FAILURE : self::SUCCESS;
    }

    private function rotateByPrefix(string $prefix): int
    {
        // ✅ 改用 KEYS 替代 SCAN，避免 Windows pphpredis 返回格式问题
        $redis = Redis::connection()->client();
        $pattern = "device:secret:{$prefix}*";

        $keys = $redis->keys($pattern);

        if (empty($keys)) {
            $this->info("前缀 {$prefix} 未找到任何设备");
            return self::SUCCESS;
        }

        // 去掉 Laravel Redis 前缀（如果有）
        $prefix_str = config('database.redis.options.prefix', '');
        $clientIds = [];
        foreach ($keys as $key) {
            $key = str_replace($prefix_str, '', $key);
            $clientIds[] = substr($key, strlen("device:secret:"));
        }

        $this->info("前缀 {$prefix} 找到 " . count($clientIds) . " 个设备");

        $dryRun = (bool)$this->option('dry-run');
        if ($dryRun) {
            $this->warn('DRY RUN 模式 — 不会写入 Redis');
        }

        $bar = $this->output->createProgressBar(count($clientIds));
        $bar->start();

        $success = 0;
        $failed  = 0;

        foreach ($clientIds as $clientId) {
            try {
                $this->doRotate($clientId, (int)$this->option('secret-len'), (int)$this->option('grace'), $dryRun);
                $success++;
            } catch (Throwable $e) {
                $failed++;
                $this->warn("\n失败: {$clientId}");
            }
            $bar->advance();
        }

        $bar->finish();
        $this->newLine();
        $this->info("批量轮换完成: 成功 {$success} | 失败 {$failed}");
        return $failed > 0 ? self::FAILURE : self::SUCCESS;
    }

    private function doRotate(string $clientId, int $secretLen, int $grace, bool $dryRun = false): void
    {
        $oldSecret = Redis::get("device:secret:{$clientId}");
        $newSecret = $this->generateSecret($secretLen);

        if ($dryRun) {
            return; // dry-run 什么都不写
        }

        if ($oldSecret && $grace > 0) {
            Redis::setex("device:secret:old:{$clientId}", $grace, $oldSecret);
        }

        Redis::set("device:secret:{$clientId}", $newSecret);
        Redis::del("auth:hmac:{$clientId}");
    }

    private function generateSecret(int $length): string
    {
        return rtrim(strtr(base64_encode(random_bytes($length)), '+/', '-_'), '=');
    }
}
