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

    public function handle(): int
    {
        if (!extension_loaded('swow')) {
            $this->error('Swow extension not loaded.');
            return self::FAILURE;
        }

        // ---------- 错误兜底 ----------
        error_reporting(E_ALL);
        ini_set('display_errors', '1');

        register_shutdown_function(function () {
            $e = error_get_last();
            if ($e !== null && in_array($e['type'], [E_ERROR, E_PARSE, E_CORE_ERROR, E_COMPILE_ERROR])) {
                fwrite(STDERR, sprintf(
                    "FATAL: %s in %s:%d\n",
                    $e['message'], $e['file'], $e['line']
                ));
            }
        });

        set_exception_handler(function (Throwable $e) {
            fwrite(STDERR, sprintf(
                "UNCAUGHT: %s: %s in %s:%d\n%s\n",
                get_class($e), $e->getMessage(),
                $e['file'] ?? $e->getFile(),
                $e['line'] ?? $e->getLine(),
                $e->getTraceAsString()
            ));
        });

        $workers = (int) $this->option('workers');
        $port    = (int) $this->option('port');

        if (!defined('AUTH_PORT')) {
            define('AUTH_PORT', $port);
        }

        $serverFile = base_path('extensions/edge/bin/server.php');
        if (!is_file($serverFile)) {
            $this->error("Server file not found: {$serverFile}");
            return self::FAILURE;
        }

        // ========== 降级判断（唯一入口） ==========
        if (!extension_loaded('pcntl') || strtoupper(substr(PHP_OS, 0, 3)) === 'WIN') {
            if (strtoupper(substr(PHP_OS, 0, 3)) === 'WIN') {
                $this->warn('Windows detected, running in single-process mode (no fork).');
            } else {
                $this->warn('pcntl not loaded, running in single-process mode.');
            }

            $this->info("Starting Edge Auth Server on port {$port} (single worker)...");
            $this->info('Laravel base_path: ' . base_path());
            $this->info('PHP ' . PHP_VERSION . ' | Swow ' . (defined('SWOW_VERSION') ? SWOW_VERSION : 'unknown'));

            require $serverFile;
            return self::SUCCESS;
        }

        // ========== Linux + pcntl：fork 多 worker ==========
        $this->info("Starting Edge Auth Server...");
        $this->info("Laravel base_path: " . base_path());
        $this->info("PHP " . PHP_VERSION . " | Swow " . (defined('SWOW_VERSION') ? SWOW_VERSION : 'unknown'));
        $this->info("Port: {$port} | Workers: {$workers} | Mode: REUSEPORT fork");
        $this->info("Server file: {$serverFile}");

        pcntl_async_signals(true);

        pcntl_signal(SIGTERM, function () {
            $this->stopAllChildren('SIGTERM');
        });

        pcntl_signal(SIGINT, function () {
            $this->stopAllChildren('SIGINT');
        });

        pcntl_signal(SIGHUP, function () {
            $this->info('SIGHUP received, reloading workers...');
            $this->stopAllChildren('SIGHUP', true);
        });

        for ($i = 1; $i <= $workers; $i++) {
            $this->spawnWorker($i, $serverFile);
        }

        $this->info("All {$workers} workers forked. Parent pid=" . getmypid());

        while (count($this->children) > 0) {
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

                if (!$this->stopping) {
                    $newWorkerId = $workerId === '?' ? count($this->children) + 1 : $workerId;
                    $this->warn("Restarting worker {$newWorkerId}...");
                    $this->spawnWorker((int) $newWorkerId, $serverFile);
                }
            }

            usleep(500000);
        }

        $this->info('Parent process exiting.');
        return self::SUCCESS;
    }

    protected bool $stopping = false;

    protected function spawnWorker(int $workerId, string $serverFile): void
    {
        $pid = pcntl_fork();

        if ($pid === -1) {
            $this->error("Failed to fork worker {$workerId}");
            return;
        }

        if ($pid === 0) {
            cli_set_process_title("edgeauth-worker-{$workerId}");
            pcntl_signal(SIGTERM, SIG_DFL);
            pcntl_signal(SIGINT, SIG_DFL);
            pcntl_signal(SIGHUP, SIG_DFL);

            if (!defined('AUTH_PORT')) {
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
        $this->stopping = true;
        $this->info("{$signalName} received, stopping all workers...");

        foreach ($this->children as $pid => $workerId) {
            $this->info("Sending SIGTERM to worker {$workerId} (pid={$pid})");
            posix_kill($pid, SIGTERM);
        }

        $deadline = time() + 5;
        while (count($this->children) > 0 && time() < $deadline) {
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

        while (count($this->children) > 0) {
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
}
