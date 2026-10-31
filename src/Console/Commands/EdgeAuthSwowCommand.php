<?php
declare(strict_types=1);
namespace DagaSmart\Edge\Console\Commands;

use Illuminate\Console\Command;
use Symfony\Component\Process\Process;

class EdgeAuthSwowCommand extends Command
{
    protected $signature = 'edge:auth-swow
        {--workers=4 : Worker 进程数}
        {--port=9503 : 监听端口}
        {--max-restarts=100 : 单个 worker 最大重启次数（0=不限）}
        {--restart-window=60 : 重启计数窗口秒数}
        {--backlog=4096 : listen backlog}';

    protected $description = '启动 Edge Auth Swow Server（master-fd 继承模型）';

    protected int $port;
    protected int $workers;
    protected int $backlog;
    protected string $serverFile;
    protected string $bootstrapFile;

    /** @var array<int,int> pid => workerId */
    protected array $children = [];

    /** @var array<int,int> workerId => restart timestamps */
    protected array $restartCounts = [];

    protected int $restartWindow;
    protected int $maxRestarts;
    protected int $nextWorkerId = 1;

    protected $listenStream;

    public function handle(): int
    {
        $this->port = (int) $this->option('port');
        $this->workers = (int) $this->option('workers');
        $this->backlog = (int) $this->option('backlog');
        $this->maxRestarts = (int) $this->option('max-restarts');
        $this->restartWindow = (int) $this->option('restart-window');

        $base = base_path();
        $this->serverFile = $base . '/extensions/edge/bin/server.php';
        $this->bootstrapFile = $base . '/extensions/edge/bin/bootstrap.php';

        if (! is_file($this->serverFile)) {
            $this->error("server.php not found: {$this->serverFile}");

            return 1;
        }

        // ---------- master 建 listen socket ----------
        $this->listenStream = socket_create_listen($this->port, $this->backlog);
        if ($this->listenStream === false) {
            $err = function_exists('socket_strerror')
                ? socket_strerror(socket_last_error())
                : 'socket_listen failed';
            $this->error("master socket_listen failed: {$err}");

            return 1;
        }

        socket_set_option($this->listenStream, SOL_SOCKET, SO_REUSEADDR, 1);
        socket_set_nonblock($this->listenStream);

        $isWindows = strtoupper(substr(PHP_OS, 0, 3)) === 'WIN';

        $this->info('Starting Edge Auth Server (master-fd inheritance model)...');
        $this->line("Laravel base_path: {$base}");
        $this->line('PHP ' . PHP_VERSION);
        $this->line("Port: {$this->port} | Workers: {$this->workers} | Mode: master-fd");
        $this->line("OS: " . PHP_OS . ($isWindows ? ' (Windows, single-process fallback)' : ''));
        $this->line("Server file: {$this->serverFile}");

        // ---------- Windows 降级：单进程直接跑 server.php ----------
        if ($isWindows) {
            $this->warn('Windows detected: pcntl not available, running in single-process mode.');
            $this->warn('For production multi-worker, deploy to Linux.');

            $GLOBALS['worker_id'] = 1;
            $GLOBALS['__edge_listen_resource'] = $this->listenStream;

            if (is_file($this->bootstrapFile)) {
                require_once $this->bootstrapFile;
            }

            require $this->serverFile;

            return 0;
        }

        // ---------- Linux：注册信号处理 ----------
        if (function_exists('pcntl_async_signals')) {
            pcntl_async_signals(true);
        }

        if (function_exists('pcntl_signal')) {
            pcntl_signal(SIGTERM, function (): void {
                $this->shutdown('SIGTERM');
            });
            pcntl_signal(SIGINT, function (): void {
                $this->shutdown('SIGINT');
            });
        }

        // ---------- 启动初始 worker ----------
        for ($i = 0; $i < $this->workers; $i++) {
            $this->spawnWorker();
            usleep(100_000);
        }

        $this->line("All {$this->workers} workers forked. Parent pid=" . getmypid());

        // ---------- master 守护循环 ----------
        while (true) {
            if (function_exists('pcntl_waitpid')) {
                $status = 0;
                $pid = pcntl_waitpid(-1, $status, WNOHANG);

                if ($pid > 0) {
                    $workerId = $this->children[$pid] ?? null;
                    unset($this->children[$pid]);

                    if ($workerId !== null) {
                        $exitCode = pcntl_wexitstatus($status);
                        $this->line("Worker {$workerId} (pid={$pid}) exited with code {$exitCode}");

                        if ($this->shouldRestart($workerId)) {
                            $this->line("Restarting worker {$workerId}...");
                            $this->spawnWorker($workerId);
                        } else {
                            $this->warn("Worker {$workerId} exceeded restart limit, not restarting");
                        }
                    }
                }
            }

            if (empty($this->children)) {
                $this->line('All workers exited. Parent exiting.');

                return 0;
            }

            usleep(100_000);
        }
    }

    protected function spawnWorker(?int $workerId = null): void
    {
        $workerId = $workerId ?? $this->nextWorkerId++;

        $cmd = [
            PHP_BINARY,
            'artisan',
            'edge:auth-swow:worker',
            '--port=' . $this->port,
            '--worker-id=' . $workerId,
            '--backlog=' . $this->backlog,
        ];

        $env = [...getenv()];

        // Linux：用 Socket 扩展拿真正的 int fd
        if (function_exists('socket_get_fd')) {
            $fd = socket_get_fd($this->listenStream);
            if ($fd > 0) {
                $env['__EDGE_LISTEN_FD'] = (string) $fd;
            }
        }

        $process = new Process($cmd, base_path(), $env);
        $process->start();

        $pid = $process->getPid();
        $this->children[$pid] = $workerId;
        $this->line("Worker {$workerId} forked, pid={$pid}");
    }

    protected function shouldRestart(int $workerId): bool
    {
        if ($this->maxRestarts <= 0) {
            return true;
        }

        $now = time();
        $entries = $this->restartCounts[$workerId] ?? [];
        $entries = array_filter($entries, fn (int $t) => $now - $t < $this->restartWindow);
        $entries[] = $now;

        $this->restartCounts[$workerId] = $entries;

        return count($entries) <= $this->maxRestarts;
    }

    protected function shutdown(string $signal): void
    {
        $this->line("{$signal} received, stopping all workers...");

        foreach ($this->children as $pid => $workerId) {
            $this->line("Sending SIGTERM to worker {$workerId} (pid={$pid})");
            if (function_exists('posix_kill')) {
                posix_kill($pid, SIGTERM);
            }
        }

        $timeout = 5;
        $start = time();
        while (! empty($this->children) && (time() - $start) < $timeout) {
            if (function_exists('pcntl_waitpid')) {
                $status = 0;
                $pid = pcntl_waitpid(-1, $status, WNOHANG);
                if ($pid > 0) {
                    $workerId = $this->children[$pid] ?? null;
                    unset($this->children[$pid]);
                    if ($workerId !== null) {
                        $this->line("Worker {$workerId} (pid={$pid}) exited gracefully");
                    }
                }
            }
            usleep(100_000);
        }

        foreach ($this->children as $pid => $workerId) {
            $this->line("Force killing worker {$workerId} (pid={$pid})");
            if (function_exists('posix_kill')) {
                posix_kill($pid, SIGKILL);
            }
        }

        if (is_resource($this->listenStream)) {
            socket_close($this->listenStream);
        }

        $this->line('All workers stopped. Parent exiting.');
        exit(0);
    }
}
