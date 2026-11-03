<?php

declare(strict_types=1);

namespace DagaSmart\Edge\Console\Commands;

use Illuminate\Console\Command;
use Illuminate\Support\Facades\DB;
use Throwable;

/**
 * 按月自动维护 device_heartbeat 分区。
 *
 * 设计目标：消除"下个月忘了建分区 → 写入失败进死信"的运维风险。
 *
 * 策略（借鉴 pg_partman 思路，但零依赖、纯 SQL）：
 *   - PRECREATE_MONTHS：预建未来 N 个月（默认 2，推荐生产设 3）
 *   - RETAIN_MONTHS：保留最近 N 个月（默认 6，0=永久）
 *   - 每月 1 号由 task-schedule 触发；也可随时手动 php artisan edge:partition-maintain
 *   - 幂等：分区已存在则忽略，可安全重复执行
 *
 * 部署（与你现有 task-schedule 风格一致）：
 *   $schedule->command('edge:partition-maintain')->monthlyOn(1, '00:05');
 */
class EdgePartitionMaintainCommand extends Command
{
    protected $signature = 'edge:partition-maintain
                            {--table=device_heartbeat : 主分区表}
                            {--precreate=2 : 预建未来月份数}
                            {--retain=6 : 保留月份数，0=永久}
                            {--dry-run : 只打印将要执行的操作，不实际执行}
                            {--list : 列出当前已有分区}';

    protected $description = '按月自动预建/清理 device_heartbeat 分区（幂等，可安全重复执行）';

    /** 分区键：与迁移中 partition by range (created_at) 一致 */
    private const string PARTITION_KEY = 'created_at';

    public function handle(): int
    {
        $table = $this->option('table');

        if ($this->option('list')) {
            return $this->listPartitions($table);
        }

        $precreate = (int) $this->option('precreate');
        $retain = (int) $this->option('retain');
        $dryRun = (bool) $this->option('dry-run');

        if ($precreate < 0 || $retain < 0) {
            $this->error('[PARTITION] --precreate / --retain 不能为负数');

            return 1;
        }

        try {
            $this->ensureParentTable($table);
        } catch (Throwable $e) {
            $this->error('[PARTITION] 父表不存在或不支持分区：'.$e->getMessage());

            return 1;
        }

        $ok = true;
        $ok = $this->precreatePartitions($table, $precreate, $dryRun) && $ok;
        $ok = $this->retainPartitions($table, $retain, $dryRun) && $ok;

        if ($dryRun) {
            $this->warn('[PARTITION] 以上为 --dry-run 预览，未实际执行');

            return 0;
        }

        $this->info($ok ? '[PARTITION] 分区维护完成' : '[PARTITION] 维护完成，但存在警告，请检查上方输出');

        return $ok ? 0 : 1;
    }

    /* ==================== 预建 ==================== */

    private function precreatePartitions(string $table, int $precreate, bool $dryRun): bool
    {
        $this->info(sprintf('[PARTITION] 预建未来 %d 个月 → %s', $precreate, $table));

        $now = new \DateTimeImmutable('first day of this month 00:00:00');
        $allOk = true;

        for ($i = 0; $i <= $precreate; $i++) {
            $month = $now->modify('+' . $i . ' month');
            $name = $this->partitionName($table, $month);
            $from = $month->format('Y-m-d');
            $to = $month->modify('+1 month')->format('Y-m-d');

            if ($this->partitionExists($table, $name)) {
                $this->line(sprintf('  · 跳过（已存在）%s %s ~ %s', $name, $from, $to));

                continue;
            }

            if ($dryRun) {
                $this->line(sprintf('  · [dry-run] 将建 %s %s ~ %s', $name, $from, $to));

                continue;
            }

            try {
                DB::statement(sprintf(
                    'CREATE TABLE %s PARTITION OF %s FOR VALUES FROM (%s) TO (%s)',
                    $this->ident($name),
                    $this->ident($table),
                    $this->quote($from),
                    $this->quote($to),
                ));

                $this->line(sprintf('  ✅ 已建 %s %s ~ %s', $name, $from, $to));

                // 新分区补齐索引（继承父表的常规索引，但唯一约束/主键不继承，显式建）
                $this->ensurePartitionIndexes($table, $name);
            } catch (Throwable $e) {
                $this->error(sprintf('  ❌ 建 %s 失败：%s', $name, $e->getMessage()));
                $allOk = false;
            }
        }

        return $allOk;
    }

    /* ==================== 清理 ==================== */

    private function retainPartitions(string $table, int $retain, bool $dryRun): bool
    {
        if ($retain === 0) {
            $this->line('[PARTITION] 保留策略：永久（不清理旧分区）');

            return true;
        }

        $this->info(sprintf('[PARTITION] 仅保留最近 %d 个月，清理更早分区 → %s', $retain, $table));

        $cutoff = new \DateTimeImmutable('first day of this month 00:00:00');
        $cutoff = $cutoff->modify('-' . ($retain - 1) . ' month');

        $toDrop = [];
        foreach ($this->listPartitionNames($table) as $p) {
            $month = $this->monthFromName($table, $p);
            if ($month !== null && $month < $cutoff) {
                $toDrop[] = $p;
            }
        }

        if ($toDrop === []) {
            $this->line('  · 无需要清理的旧分区');

            return true;
        }

        $allOk = true;
        foreach ($toDrop as $name) {
            if ($dryRun) {
                $this->line(sprintf('  · [dry-run] 将删 %s', $name));

                continue;
            }

            try {
                // 生产数据务必先确认；CONCURRENTLY 不能在事务内，故用独立语句
                DB::statement(sprintf('DROP TABLE %s', $this->ident($name)));
                $this->line(sprintf('  🗑 已删 %s', $name));
            } catch (Throwable $e) {
                $this->error(sprintf('  ❌ 删 %s 失败：%s', $name, $e->getMessage()));
                $allOk = false;
            }
        }

        return $allOk;
    }

    /* ==================== 索引 ==================== */

    private function ensurePartitionIndexes(string $table, string $partition): void
    {
        // 迁移里 idx_heartbeat_sn / idx_heartbeat_created_at 是普通索引，会被子表继承；
        // 主键 uk_heartbeat_stream_id 是约束，需在每个分区上显式存在。
        // 这里做一次幂等补齐，防止个别 PG 版本继承行为差异。
        $constraint = $table . '_stream_id_unique';
        $indexSn = 'idx_' . $partition . '_sn';
        $indexCreated = 'idx_' . $partition . '_created_at';

        $sqls = [
            // stream_id 唯一约束：幂等写入（ON CONFLICT）的前提
            sprintf(
                'ALTER TABLE %s ADD CONSTRAINT %s UNIQUE (stream_id)',
                $this->ident($partition),
                $this->ident($constraint),
            ),
            // 单条查询 / 按设备拉最新心跳
            sprintf(
                'CREATE INDEX IF NOT EXISTS %s ON %s (sn)',
                $this->ident($indexSn),
                $this->ident($partition),
            ),
            // 时间范围扫描（看板 / 清理）
            sprintf(
                'CREATE INDEX IF NOT EXISTS %s ON %s (created_at)',
                $this->ident($indexCreated),
                $this->ident($partition),
            ),
        ];

        foreach ($sqls as $sql) {
            try {
                DB::statement($sql);
            } catch (Throwable $e) {
                // 约束名冲突等幂等失败，记日志不中断
                fwrite(STDERR, '[PARTITION-INDEX] ' . $e->getMessage() . PHP_EOL);
            }
        }
    }

    /* ==================== 元数据 ==================== */

    private function ensureParentTable(string $table): void
    {
        $exists = DB::selectOne(
            'SELECT 1 FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace ' .
            'WHERE c.relname=:t AND n.nspname=:s AND c.relkind=:k',
            ['t' => $table, 's' => 'public', 'k' => 'p'],
        );

        if ($exists === null) {
            throw new \RuntimeException('分区父表 ' . $table . ' 不存在，请先运行 migrate');
        }

        $partitioned = DB::selectOne(
            'SELECT 1 FROM pg_partitioned_table WHERE partrelid = :rel::regclass',
            ['rel' => 'public.' . $table],
        );

        if ($partitioned === null) {
            throw new \RuntimeException('表 ' . $table . ' 不是分区表，无法维护分区');
        }
    }

    private function partitionExists(string $table, string $partition): bool
    {
        $row = DB::selectOne(
            'SELECT 1 FROM pg_inherits i JOIN pg_class c ON c.oid=i.inhrelid ' .
            'JOIN pg_namespace n ON n.oid=c.relnamespace ' .
            'WHERE i.inhparent = :parent::regclass AND c.relname=:child AND n.nspname=:schema',
            ['parent' => 'public.' . $table, 'child' => $partition, 'schema' => 'public'],
        );

        return $row !== null;
    }

    /** @return string[] */
    private function listPartitionNames(string $table): array
    {
        $rows = DB::select(
            'SELECT c.relname AS name FROM pg_inherits i JOIN pg_class c ON c.oid=i.inhrelid ' .
            'JOIN pg_namespace n ON n.oid=c.relnamespace ' .
            'WHERE i.inhparent = :parent::regclass AND n.nspname=:schema ' .
            'ORDER BY c.relname',
            ['parent' => 'public.' . $table, 'schema' => 'public'],
        );

        return array_map(static fn ($r) => $r->name, $rows);
    }

    private function listPartitions(string $table): int
    {
        $this->info('当前分区：' . $table);
        $names = $this->listPartitionNames($table);

        if ($names === []) {
            $this->warn('  （无子分区）');

            return 0;
        }

        foreach ($names as $name) {
            $this->line('  · ' . $name);
        }

        $this->line('');
        $this->info('共 ' . count($names) . ' 个分区');

        return 0;
    }

    /* ==================== 命名 ==================== */

    private function partitionName(string $table, \DateTimeImmutable $month): string
    {
        return $table . '_p' . $month->format('Y_m');
    }

    private function monthFromName(string $table, string $partition): ?\DateTimeImmutable
    {
        $prefix = $table . '_p';
        if (!str_starts_with($partition, $prefix)) {
            return null;
        }

        $tail = substr($partition, strlen($prefix));
        if (!preg_match('/^\d{4}_\d{2}$/', $tail)) {
            return null;
        }

        $dt = \DateTimeImmutable::createFromFormat('Y_m', $tail);
        if ($dt === false) {
            return null;
        }

        return $dt->modify('first day of this month 00:00:00');
    }

    /* ==================== SQL 工具 ==================== */

    private function ident(string $name): string
    {
        return '"' . addslashes($name) . '"';
    }

    private function quote(string $value): string
    {
        return "'" . addslashes($value) . "'";
    }
}
