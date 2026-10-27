<?php

declare(strict_types=1);
use Carbon\Carbon;
use Illuminate\Support\Facades\Schedule;

/**
 * *************************************
 *  定时调度                             *
 *  注意: 任务不可重复                     *
 *  有任务没执行时清理死锁:                 *
 *  php artisan schedule:clear-cache    *
 * **************************************
 */

// 股票抓取：每天每小时一次
//Schedule::command('stock:symbol')
//    ->hourly()
//    ->timezone('Asia/Shanghai')
//    ->when(function () {
//        $t = now()->timezone('Asia/Shanghai')->format('H:i:s');
//
//        return $t >= '00:00:00' && $t <= '09:00:00';
//    })
//    ->withoutOverlapping(1) // 1分钟后自动过期释放
//    ->appendOutputTo(storage_path('logs/schedule.stock.symbol.log')); // 加日志确认每次执行

// 行情抓取：每30s，只在交易时段
//Schedule::command('stock:quote')
//    ->everyThirtySeconds()
//    // ->everyMinute()
//    ->when(function () {
//        $now = Carbon::now('Asia/Shanghai');
//        $t = $now->format('H:i:s');
//
//        return ($t >= '09:30:00' && $t <= '11:30:00')
//            || ($t >= '13:00:00' && $t <= '15:00:00');
//    })
//    ->timezone('Asia/Shanghai')
//    ->withoutOverlapping(1) // 1分钟后自动过期释放
//    ->appendOutputTo(storage_path('logs/schedule.stock.quote.log')); // 加日志确认每次执行

// 收盘快照：每天 15:10
//Schedule::command('stock:quote --force')
//    ->dailyAt('15:10')
//    ->timezone('Asia/Shanghai')
//    ->withoutOverlapping(1) // 1分钟后自动过期释放
//    ->appendOutputTo(storage_path('logs/schedule.stock.quote.force.log')); // 加日志确认每次执行
