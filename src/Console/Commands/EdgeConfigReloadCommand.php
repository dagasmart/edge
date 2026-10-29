<?php

declare(strict_types=1);

namespace DagaSmart\Edge\Console\Commands;

use DagaSmart\Edge\Services\EdgeGatewayConfigService;
use Illuminate\Console\Command;
use Illuminate\Support\Facades\Log;

class EdgeConfigReloadCommand extends Command
{
    protected $signature = 'edge:reload
                            {--gateway= : 网关实例ID，仅重载该网关配置}
                            {--pid= : 指定 EdgeAuth 主进程 PID}
                            {--signal=USR1 : 重载信号，默认 SIGUSR1}';

    protected $description = '重载 EdgeAuth 配置并平滑重启 Swow worker';

    public function handle(EdgeGatewayConfigService $service): int
    {
        $pid = $this->option('pid');

        if ($pid) {
            $this->reloadByPid((int) $pid);
        } elseif (function_exists('posix_kill')) {
            $pidFile = storage_path('edge-auth.pid');
            if (file_exists($pidFile)) {
                $pid = (int) trim((string) file_get_contents($pidFile));
                if ($pid <= 0) {
                    $this->warn('pid 文件内容无效，跳过 reload。');
                } elseif (!posix_kill($pid, 0)) {
                    $this->warn("PID {$pid} 已退出，清理过期 pid 文件。");
                    @unlink($pidFile);
                } else {
                    $this->reloadByPid($pid);
                }
            } else {
                $this->warn('未指定 --pid 且未找到 ' . $pidFile . '，仅刷新配置缓存，未发信号。');
            }
        } else {
            $this->warn('Windows 环境不支持 posix 信号，配置将在下次启动时生效。');
        }

        // 可选：刷新配置到缓存/常量
        $gatewayId = $this->option('gateway');
        $resolved = $service->resolve($gatewayId ? (int) $gatewayId : null);

        Log::channel('edge_auth')->info('Edge 配置已重载', [
            'gateway_id' => $gatewayId,
            'keys' => count($resolved),
        ]);

        $this->info('配置已重载，共 '.count($resolved).' 项生效。');

        return self::SUCCESS;
    }

    protected function reloadByPid(int $pid): void
    {
        $signal = $this->option('signal') === 'USR1' ? SIGUSR1 : SIGHUP;

        if (! posix_kill($pid, $signal)) {
            $this->error("发送信号失败 (pid={$pid})，进程可能已退出。");

            return;
        }

        $this->info("已向 EdgeAuth 主进程 {$pid} 发送 ".$this->option('signal').' 信号。');
    }
}
