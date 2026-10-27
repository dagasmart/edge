<?php

declare(strict_types=1);

namespace DagaSmart\Edge\Console\Commands;

use DagaSmart\BizAdmin\Traits\MemoryTrait;
use DagaSmart\BizAdmin\Traits\OutputTrait;
use Illuminate\Console\Command as BaseCommand;
use Illuminate\Support\Facades\DB;
use Throwable;

abstract class Command extends BaseCommand
{
    // 指标追踪类
    use MemoryTrait;

    // 业务结果类
    use OutputTrait;

    /**
     * 核心原则：父类 handle() 是框架入口
     * 子类实现的是内部钩子，用 __ 前缀区分层级关系
     */
    abstract protected function __handle(): int;

    public function handle(): int
    {
        // ✅ 环境初始化，只执行一次
        $this->initEnvironment();
        // ✅ 记录内存追踪基线
        $this->startMemoryTracking();

        $taskId = (int) $this->option('task-id');
        $runId = (int) $this->option('run-id');

        try {

            return $this->__handle();

        } catch (Throwable $e) {

            $this->error('ERROR: '.$e->getMessage());
            $this->error('Command FAILED: '.$e->getTraceAsString());

            return self::FAILURE;

        } finally {
            if ($taskId > 0 && $runId > 0) {
                $this->emitMemoryMetrics($taskId, $runId);
                $this->flushOutputToRedis($taskId, $runId);
            }
        }
    }

    // ✅ 环境初始化
    private function initEnvironment(): void
    {
        ini_set('memory_limit', '512M');
        set_time_limit(0);
        try {
            DB::statement("SET statement_timeout TO '30000'"); // PG 单条语句 30 秒超时
            DB::disableQueryLog();
        } catch (Throwable $e) {
            // 未连接数据库时不阻塞，静默跳过
            error_log('MemoryTrait: DB init skipped - '.$e->getMessage());
        }
    }
}
