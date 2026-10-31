<?php
declare(strict_types=1);
namespace DagaSmart\Edge\Console\Commands;

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

        // ---------- 从环境变量拿到 master 传来的 listen fd ----------
        $fd = (int) ($_ENV['__EDGE_LISTEN_FD'] ?? getenv('__EDGE_LISTEN_FD') ?: 0);

        if ($fd <= 0) {
            $this->error('No __EDGE_LISTEN_FD received from master');

            return 1;
        }

        // 把 fd 还原成 socket resource
        $listenResource = socket_import_stream($fd);
        if ($listenResource === false) {
            $this->error("socket_import_stream failed for fd={$fd}");

            return 1;
        }

        // 交给 server.php
        $GLOBALS['__edge_listen_resource'] = $listenResource;

        $serverFile = base_path('extensions/edge/bin/server.php');
        if (! is_file($serverFile)) {
            $this->error("server.php not found: {$serverFile}");

            return 1;
        }

        $bootstrapFile = base_path('extensions/edge/bin/bootstrap.php');
        if (is_file($bootstrapFile)) {
            require_once $bootstrapFile;
        }

        // server.php 会接管生命周期，这里不会返回
        require $serverFile;

        return 0;
    }
}
