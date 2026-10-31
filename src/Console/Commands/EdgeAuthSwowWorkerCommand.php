<?php

declare(strict_types=1);

namespace App\Console\Commands;

use Illuminate\Console\Command;

class EdgeAuthSwowWorkerCommand extends Command
{
    protected $signature = 'edge:auth-swow:worker
        {--port=9503 : 监听端口（仅用于日志）}
        {--worker-id=1 : Worker ID}
        {--backlog=4096 : backlog（仅用于日志）}';

    protected $description = 'Edge Auth Swow Worker（被 master 拉起，继承 listen fd）';

    public function handle(): int
    {
        $workerId = (int) $this->option('worker-id');
        $port = (int) $this->option('port');

        $GLOBALS['worker_id'] = $workerId;

        cli_set_process_title("edgeauth-worker-{$workerId}");

        $fd = (int) ($_ENV['__EDGE_LISTEN_FD']
            ?? getenv('__EDGE_LISTEN_FD')
            ?: 0
        );

        if ($fd <= 0) {
            $this->error('No __EDGE_LISTEN_FD received from master');
            return 1;
        }

        // Linux：用 Socket 扩展从 int fd 还原 socket resource
        if (! function_exists('socket_import_fd')) {
            $this->error('socket_import_fd not available, cannot inherit fd');
            return 1;
        }

        $listenResource = socket_import_fd($fd);
        if ($listenResource === false) {
            $err = function_exists('socket_strerror')
                ? socket_strerror(socket_last_error())
                : 'unknown';
            $this->error("socket_import_fd failed for fd={$fd}: {$err}");
            return 1;
        }

        $GLOBALS['__edge_listen_resource'] = $listenResource;

        $bootstrapFile = base_path('extensions/edge/bin/bootstrap.php');
        if (is_file($bootstrapFile)) {
            require_once $bootstrapFile;
        }

        $serverFile = base_path('extensions/edge/bin/server.php');
        if (! is_file($serverFile)) {
            $this->error("server.php not found: {$serverFile}");
            return 1;
        }

        require $serverFile;

        return 0;
    }
}
