<?php

declare(strict_types=1);

namespace DagaSmart\Edge\Console\Commands;

use Illuminate\Console\Command;
use Throwable;

class EdgeAuthSwowCommand extends Command
{
    protected $signature = 'edge:auth-swow
                            {--workers=4 : Number of worker processes}
                            {--port=9503 : Listen port (REUSEPORT)}';

    protected $description = 'Start Swow MQTT HMAC Auth Server for NanoMQ (multi-worker with fork)';

    /** @var array<int, int> pid => worker_id 映射 */
    protected array $children = [];

    protected bool $stopping = false;

    /** @var array<int, array<int>> worker_id => 重启时间戳列表 */
    protected array $restartLog = [];

    /** 上次 SIGHUP 处理时间，用于防抖 */
    protected float $lastHupTime = 0.0;

    public function handle(): int
    {
        if (! extension_loaded('swow')) {
            $this->error('Swow extension not loaded.');

            return self::FAILURE;
        }

        error_reporting(E_ALL);
        ini_set('display_errors', '1');

        register_shutdown_function(static function (): void {
            $e = error_get_last();
            if ($e !== null && in_array($e['type'], [E_ERROR, E_PARSE, E_CORE_ERROR, E_COMPILE_ERROR], true)) {
                fwrite(STDERR, sprintf("FATAL: %s in %s:%d\n", $e['message'], $e['file'], $e['line']));
            }
        });

        set_exception_handler(static function (Throwable $e): void {
            fwrite(STDERR, sprintf(
                "UNCAUGHT: %s: %s in %s:%d\n%s\n",
                get_class($e),
                $e->getMessage(),
                $e['file'] ?? $e->getFile(),
                $e['line'] ?? $e->getLine(),
                $e->getTraceAsString()
            ));
        });

        $workers = (int) $this->option('workers');
        $port = (int) $this->option('port');

        if ($workers < 1) {
            $this->error('workers must be >= 1');

            return self::FAILURE;
        }

        if (! defined('AUTH_PORT')) {
            define('AUTH_PORT', $port);
        }

        $serverFile = base_path('extensions/edge/bin/server.php');
        if (! is_file($serverFile)) {
            $this->error("Server file not found: {$serverFile}");

            return self::FAILURE;
        }

        // ========== 降级判断 ==========
        if (! extension_loaded('pcntl') || strtoupper(substr(PHP_OS, 0, 3)) === 'WIN') {
            if (strtoupper(substr(PHP_OS, 0, 3)) === 'WIN') {
                $this->warn('Windows detected, running in single-process mode (no fork).');
            } else {
                $this->warn('pcntl not loaded, running in single-process mode.');
            }

            $this->info("Starting Edge Auth Server on port {$port} (single worker)...");
            $this->info('Laravel base_path: '.base_path());
            $this->info('PHP '.PHP_VERSION.' | Swow '.(phpversion('swow') ?: 'unknown'));

            require $serverFile;

            return self::SUCCESS;
        }

        // ========== Linux + pcntl ==========
        $this->info('Starting Edge Auth Server...');
        $this->info('Laravel base_path: '.base_path());
        $this->info('PHP '.PHP_VERSION.' | Swow '.(phpversion('swow') ?: 'unknown'));
        $this->info("Port: {$port} | Workers: {$workers} | Mode: REUSEPORT fork");
        $this->info("Server file: {$serverFile}");

        // 先忽略 SIGHUP，避免窗口期内被误杀
        pcntl_signal(SIGHUP, SIG_IGN);

        pcntl_async_signals(true);

        pcntl_signal(SIGTERM, function (): void {
            $this->stopAllChildren('SIGTERM');
        });

        pcntl_signal(SIGINT, function (): void {
            $this->stopAllChildren('SIGINT');
        });

        // SIGHUP 防抖 + 只 reload，不做退出
        pcntl_signal(SIGHUP, function (): void {
            $now = microtime(true);
            if ($now - $this->lastHupTime < 2.0) {
                return;
            }
            $this->lastHupTime = $now;
            $this->info('SIGHUP received, reloading workers...');
            $this->stopAllChildren('SIGHUP', true);
        });

        for ($i = 1; $i <= $workers; $i++) {
            $this->spawnWorker($i, $serverFile);
            usleep(100000);
        }

        $this->info("All {$workers} workers forked. Parent pid=".getmypid());

        // 父进程永久阻塞循环
        while ($this->children !== []) {
            // 确保信号处理器能及时触发
            pcntl_signal_dispatch();

            $pid = pcntl_waitpid(-1, $status, WNOHANG);

            if ($pid > 0) {
                $workerId = $this->children[$pid] ?? '?';
                unset($this->children[$pid]);

                if (pcntl_wifexited($status)) {
                    $code = pcntl_wexitstatus($status);
                    $this->warn("Worker {$workerId} (pid={$pid}) exited with code {$code}");
                } elseif (pcntl_wifsignaled($status)) {
                    $sig = pcntl_wtermsig($status);
                    $this->warn("Worker {$workerId} (pid={$pid}) killed by signal {$sig}");
                }

                if (! $this->stopping) {
                    $newWorkerId = is_numeric($workerId) ? (int) $workerId : (count($this->children) + 1);
                    if ($this->shouldRestart($newWorkerId)) {
                        $this->warn("Restarting worker {$newWorkerId}...");
                        $this->spawnWorker($newWorkerId, $serverFile);
                    } else {
                        $this->error("Worker {$newWorkerId} exceeded restart limit, not restarting");
                    }
                }
            }

            usleep(200000);
        }

        $this->info('Parent process exiting.');

        return self::SUCCESS;
    }

    protected function spawnWorker(int $workerId, string $serverFile): void
    {
        $pid = pcntl_fork();

        if ($pid === -1) {
            $this->error("Failed to fork worker {$workerId}");

            return;
        }

        if ($pid === 0) {
            // 子进程：重置信号处理器（继承的闭包 $this 可能已失效）
            pcntl_signal(SIGTERM, SIG_DFL);
            pcntl_signal(SIGINT, SIG_DFL);
            pcntl_signal(SIGHUP, SIG_DFL);

            cli_set_process_title("edgeauth-worker-{$workerId}");

            if (function_exists('pcntl_async_signals')) {
                pcntl_async_signals(true);
            }

            // 解除 Swow 继承的信号屏蔽，让 posix_kill 的 SIGTERM 能到达子进程
            if (function_exists('pcntl_sigprocmask')) {
                pcntl_sigprocmask(SIG_UNBLOCK, [SIGTERM, SIGINT, SIGHUP]);
            }

            $GLOBALS['worker_id'] = $workerId;

            $bootstrapFile = base_path('extensions/edge/bin/bootstrap.php');
            if (is_file($bootstrapFile)) {
                require_once $bootstrapFile;
            } else {
                fwrite(STDERR, "FATAL: bootstrap.php not found at {$bootstrapFile}".PHP_EOL);
                exit(1);
            }

            if (! defined('AUTH_PORT')) {
                define('AUTH_PORT', (int) $this->option('port'));
            }

            require $serverFile;
            exit(0);
        }

        $this->children[$pid] = $workerId;
        $this->info("Worker {$workerId} forked, pid={$pid}");
    }

    protected function stopAllChildren(string $signalName, bool $reload = false): void
    {
        // 防止重入
        static $inProgress = false;
        if ($inProgress) {
            return;
        }
        $inProgress = true;

        $this->stopping = true;
        $this->info("{$signalName} received, stopping all workers...");

        foreach ($this->children as $pid => $workerId) {
            $this->info("Sending SIGTERM to worker {$workerId} (pid={$pid})");
            posix_kill($pid, SIGTERM);
        }

        // 3 秒优雅退出窗口
        $deadline = time() + 3;
        while ($this->children !== [] && time() < $deadline) {
            $pid = pcntl_waitpid(-1, $status, WNOHANG);
            if ($pid > 0) {
                $workerId = $this->children[$pid];
                unset($this->children[$pid]);
                $this->info("Worker {$workerId} (pid={$pid}) exited gracefully");
            }
            usleep(100000);
        }

        // 强杀剩余
        foreach ($this->children as $pid => $workerId) {
            $this->warn("Worker {$workerId} (pid={$pid}) did not exit in time, sending SIGKILL");
            posix_kill($pid, SIGKILL);
        }

        while ($this->children !== []) {
            $pid = pcntl_waitpid(-1, $status);
            if ($pid > 0) {
                $workerId = $this->children[$pid];
                unset($this->children[$pid]);
                $this->info("Worker {$workerId} (pid={$pid}) force-killed");
            }
        }

        $inProgress = false;

        if ($reload) {
            // reload 完成，重置状态让主循环继续
            $this->stopping = false;
            $this->info('Reload complete. Workers will be respawned by main loop.');

            return;
        }

        $this->info('All workers stopped. Parent exiting.');
        exit(0);
    }

    protected function shouldRestart(int $workerId): bool
    {
        $now = time();
        $this->restartLog[$workerId] = array_filter(
            $this->restartLog[$workerId] ?? [],
            static fn (int $ts): bool => $now - $ts < 60
        );

        $this->restartLog[$workerId][] = $now;

        return count($this->restartLog[$workerId]) <= 10;
    }
}
