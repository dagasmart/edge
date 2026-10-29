<?php
declare(strict_types=1);
namespace DagaSmart\Edge\Console\Commands;

use DagaSmart\Edge\Models\EdgeAuthorize;
use DagaSmart\Edge\Services\EdgeGatewayService;
use Illuminate\Console\Command;
use Illuminate\Support\Facades\Log;

/**
 * 全量同步 EdgeAuthorize → Redis
 *
 * 使用场景：
 *   - 首次部署，DB 已有设备但 Redis 是空的
 *   - secrets.php 密钥迁移到 DB 后需要全量刷新
 *   - 运维手动修复 Redis 数据不一致
 *
 * 用法：
 *   php artisan edge:sync-redis              # 同步所有 active=1 的设备
 *   php artisan edge:sync-redis --force      # 同步全部（含 inactive，会 DEL 对应 key）
 *   php artisan edge:sync-redis --dry-run    # 只打印不执行
 */
class EdgeSyncRedisCommand extends Command
{
    protected $signature = 'edge:sync-redis
                            {--force : 同步全部记录（含 inactive，inactive 会 DEL key）}
                            {--dry-run : 只打印计划操作，不实际执行}
                            {--service= : 只同步指定 service，如 nano}';

    protected $description = '全量同步 EdgeAuthorize 设备密钥到 Redis';

    protected EdgeGatewayService $service;

    public function __construct(EdgeGatewayService $service)
    {
        parent::__construct();
        $this->service = $service;
    }

    public function handle(): int
    {
        $force   = $this->option('force');
        $dryRun  = $this->option('dry-run');
        $service = $this->option('service');

        $this->info('╔══════════════════════════════════════════╗');
        $this->info('║       EdgeAuth → Redis 全量同步           ║');
        $this->info('╚══════════════════════════════════════════╝');
        $this->newLine();

        if ($dryRun) {
            $this->warn('🔍 DRY-RUN 模式：只打印，不执行任何 Redis 操作');
            $this->newLine();
        }

        // ---- 构建查询 ----
        $query = EdgeAuthorize::query();

        if (!$force) {
            $query->where('active', true);
        }

        if ($service) {
            $query->where('service', $service);
        }

        $total = $query->count();
        $activeCount = EdgeAuthorize::query()
            ->when($service, fn($q) => $q->where('service', $service))
            ->where('active', true)
            ->count();
        $inactiveCount = $total - $activeCount;

        $this->line("📊 总计: <fg=cyan>{$total}</> 条记录");
        $this->line("   ✅ active:   <fg=green>{$activeCount}</>");
        $this->line("   ❌ inactive: <fg=red>{$inactiveCount}</>");
        $this->newLine();

        if ($total === 0) {
            $this->warn('没有找到任何记录，退出。');
            return self::SUCCESS;
        }

        // ---- 逐条处理 ----
        $synced  = 0;
        $removed = 0;
        $skipped = 0;
        $errors  = 0;

        $bar = $this->output->createProgressBar($total);
        $bar->start();

        $query->chunkById(100, function ($devices) use (
            $dryRun, $force, &$synced, &$removed, &$skipped, &$errors, $bar
        ) {
            foreach ($devices as $device) {
                try {
                    if (empty($device->client_id) || empty($device->username)) {
                        $skipped++;
                        $bar->advance();
                        continue;
                    }

                    if ($dryRun) {
                        $action = $device->active ? 'SET' : 'DEL';
                        $this->line("  [{$action}] device:secret:{$device->client_id}:{$device->username}");
                        $bar->advance();
                        continue;
                    }

                    if ($device->active) {
                        $this->service->syncRedisOnSave($device);
                        $synced++;
                    } else {
                        $this->service->removeRedisOnDelete($device);
                        $removed++;
                    }
                } catch (\Throwable $e) {
                    $errors++;
                    Log::channel('edge')->error('edge:sync-redis: single device failed', [
                        'error'  => $e->getMessage(),
                        'device' => $device->only(['id', 'client_id', 'username']),
                    ]);
                }

                $bar->advance();
            }
        });

        $bar->finish();
        $this->newLine(2);

        // ---- 汇总 ----
        $this->info('╔══════════════════════════════════════════╗');
        $this->info('║              同步完成汇总                 ║');
        $this->info('╚══════════════════════════════════════════╝');
        $this->line("  ✅ SET (synced): <fg=green>{$synced}</>");
        $this->line("  🗑️  DEL (removed): <fg=yellow>{$removed}</>");
        $this->line("  ⏭️  skipped:      <fg=gray>{$skipped}</>");
        $this->line("  ❌ errors:       <fg=red>{$errors}</>");

        if ($dryRun) {
            $this->newLine();
            $this->comment('这是 DRY-RUN 结果，未实际执行。去掉 --dry-run 重新运行以执行。');
        }

        // 写一条汇总日志
        Log::channel('edge')->info('edge:sync-redis completed', [
            'total'   => $total,
            'synced'  => $synced,
            'removed' => $removed,
            'skipped' => $skipped,
            'errors'  => $errors,
            'force'   => $force,
            'dry_run' => $dryRun,
            'service' => $service,
        ]);

        return $errors > 0 ? self::FAILURE : self::SUCCESS;
    }
}
