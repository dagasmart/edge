<?php
declare(strict_types=1);

namespace DagaSmart\Edge\Console\Commands;

use Illuminate\Console\Command;
use Throwable;

class EdgeAuthSwowCommand extends Command
{
    protected $signature = 'edge:auth-swow';
    protected $description = 'Start Swow MQTT HMAC Auth Server for NanoMQ';

    public function handle(): int
    {
        if (!extension_loaded('swow')) {
            $this->error('Swow extension not loaded.');
            return self::FAILURE;
        }

        // ---------- 关键：错误兜底 ----------
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
                $e->getFile(), $e->getLine(),
                $e->getTraceAsString()
            ));
        });
        // ------------------------------------

        $this->info('Starting Edge Auth Server...');
        $this->info('Laravel base_path: ' . base_path());
        $this->info('PHP ' . PHP_VERSION . ' | Swow ' . (defined('SWOW_VERSION') ? SWOW_VERSION : 'unknown'));

        $serverFile = base_path('extensions/edge/bin/server.php');
        if (!is_file($serverFile)) {
            $this->error("Server file not found: {$serverFile}");
            return self::FAILURE;
        }

        // 在 require 之前定义常量，确保 server.php 里能用到
        if (!defined('AUTH_PORT')) {
            define('AUTH_PORT', 9503);
        }

        require $serverFile;

        // 注意：如果 server.php 里有 while(true)，这行永远不会执行
        return self::SUCCESS;
    }
}
