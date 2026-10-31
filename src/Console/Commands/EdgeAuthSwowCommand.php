<?php

declare(strict_types=1);

namespace DagaSmart\Edge\Console\Commands;

use Illuminate\Console\Command;
use Throwable;

class EdgeAuthSwowCommand extends Command
{
    protected $signature = 'edge:auth-swow
                            {--workers=4 : Number of worker processes}
                            {--port=9503 : Listen port (SO_REUSEPORT)}';

    protected $description = 'Start Swow MQTT HMAC Auth Server for NanoMQ (per-worker REUSEPORT fork model)';

    /** @var array<int, int> pid => worker_id */
    protected array $children = [];

    protected bool $stopping = false;

    public function handle(): int
    {
        if (! extension_loaded('swow')) {
            $this->error('Swow extension not loaded.');
            return self::FAILURE;
        }

        if (! extension_loaded('pcntl')) {
            $this->warn('pcntl not loaded, fallback to single-process mode.');
            return $this->runSingleWorker();
        }

        if (strtoupper(substr(PHP_OS, 0, 3)) === 'WIN') {
            $this->warn('Windows detected, fallback to single-process mode.');
            return $this->runSingleWorker();
        }

        // ---------- 错误兜底 ----------
        error_reporting(E_ALL);
        ini_set('display_errors', '1');

        register_shutdown_function(static function (): void {
            $e = error_get_last();
            if ($e !== null && in_array($e['type'], [E_ERROR, E_PARSE, E_CORE_ERROR, E_COMPILE_ERROR], true)) {
                fwrite(STDERR, sprintf(
                    "FATAL: %s in %s:%d\n",
                    $e['message'],
                    $e['file'],
                    $e['line']
                ));
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

        // ---------- master 不建 socket，只 fork ----------
        $this->info('Starting Edge Auth Server (per-worker REUSEPORT model)...');
        $this->info('Laravel base_path: ' . base_path());
        $this->info('PHP ' . PHP_VERSION . ' | Swow ' . (defined('SWOW_VERSION') ? SWOW_VERSION : 'unknown'));
        $this->info("Port: {$port} | Workers: {$workers} | Mode: per-worker SO_REUSEPORT");
        $this->info("Server file: {$serverFile}");

        pcntl_async_signals(true);
        pcntl_signal(SIGTERM, fn () => $this->stopAllChildren('SIGTERM'));
        pcntl_signal(SIGINT, fn () => $this->stopAllChildren('SIGINT'));
        pcntl_signal(SIGHUP, function (): void {
            $this->info('SIGHUP received, reloading workers...');
            $this->stopAllChildren('SIGHUP', true);
        });

        // ---------- fork workers ----------
        for ($i = 1; $i <= $workers; $i++) {
            $this->spawnWorker($i, $serverFile);
        }

        $this->info("All {$workers} workers forked. Parent pid=" . getmypid());

        // ---------- master 事件循环 ----------
        while ($this->children !== []) {
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

                // ★ 非停止状态自动拉起
                if (! $this->stopping) {
                    $newWorkerId = is_numeric($workerId) ? (int) $workerId : count($this->children) + 1;
                    $this->warn("Restarting worker {$newWorkerId}...");
                    $this->spawnWorker($newWorkerId, $serverFile);
                }
            }

            usleep(200000);
        }

        $this->info('Parent process exiting.');
        return self::SUCCESS;
    }

    /**
     * fork 一个 worker
     * 关键点：worker 内自己创建 REUSEPORT socket
     */
    protected function spawnWorker(int $workerId, string $serverFile): void
    {
        $pid = pcntl_fork();

        if ($pid === -1) {
            $this->error("Failed to fork worker {$workerId}");
            return;
        }

        if ($pid === 0) {
            // ========== 子进程 ==========
            pcntl_signal(SIGTERM, SIG_DFL);
            pcntl_signal(SIGINT, SIG_DFL);
            pcntl_signal(SIGHUP, SIG_DFL);

            cli_set_process_title("edgeauth-worker-{$workerId}");

            // ★ worker_id 传给 server.php
            $GLOBALS['worker_id'] = $workerId;

            // ★ 每个 worker 自己加载 bootstrap
            $bootstrapFile = base_path('extensions/edge/bin/bootstrap.php');
            if (is_file($bootstrapFile)) {
                require_once $bootstrapFile;
            } else {
                fwrite(STDERR, "FATAL: bootstrap.php not found at {$bootstrapFile}" . PHP_EOL);
                exit(1);
            }

            if (! defined('AUTH_PORT')) {
                define('AUTH_PORT', (int) $this->option('port'));
            }

            // ★ server.php 内会自己建 REUSEPORT socket + listen
            require $serverFile;
            exit(0);
        }

        // ========== 父进程 ==========
        $this->children[$pid] = $workerId;
        $this->info("Worker {$workerId} forked, pid={$pid}");
    }

    protected function stopAllChildren(string $signalName, bool $reload = false): void
    {
        $this->stopping = true;
        $this->info("{$signalName} received, stopping all workers...");

        foreach ($this->children as $pid => $workerId) {
            $this->info("Sending SIGTERM to worker {$workerId} (pid={$pid})");
            posix_kill($pid, SIGTERM);
        }

        $deadline = time() + 5;
        while ($this->children !== [] && time() < $deadline) {
            $pid = pcntl_waitpid(-1, $status, WNOHANG);
            if ($pid > 0) {
                $workerId = $this->children[$pid];
                unset($this->children[$pid]);
                $this->info("Worker {$workerId} (pid={$pid}) exited gracefully");
            }
            usleep(100000);
        }

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

        $this->stopping = false;

        if ($reload) {
            $this->info('Reload complete. Workers will be respawned by main loop.');
        } else {
            $this->info('All workers stopped. Parent exiting.');
            exit(0);
        }
    }

    /**
     * 单进程降级模式
     */
    protected function runSingleWorker(): int
    {
        $port = (int) $this->option('port');

        if (! defined('AUTH_PORT')) {
            define('AUTH_PORT', $port);
        }

        $serverFile = base_path('extensions/edge/bin/server.php');
        if (! is_file($serverFile)) {
            $this->error("Server file not found: {$serverFile}");
            return self::FAILURE;
        }

        $this->info("Starting Edge Auth Server on port {$port} (single worker)...");
        $this->info('Laravel base_path: ' . base_path());
        $this->info('PHP ' . PHP_VERSION . ' | Swow ' . (defined('SWOW_VERSION') ? SWOW_VERSION : 'unknown'));

        $GLOBALS['worker_id'] = 0;

        $bootstrapFile = base_path('extensions/edge/bin/bootstrap.php');
        if (is_file($bootstrapFile)) {
            require_once $bootstrapFile;
        }

        require $serverFile;
        return self::SUCCESS;
    }
}
