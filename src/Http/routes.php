<?php

use DagaSmart\Edge\Http\Controllers;
use DagaSmart\Edge\Http\Middleware\Middleware;
use Illuminate\Routing\Router;
use Illuminate\Support\Facades\Route;
use DagaSmart\BizAdmin\Middleware\Authenticate;
use DagaSmart\BizAdmin\Middleware\Permission;

// 需登录与鉴权
Route::group([
    'prefix' => 'extension',
    'middleware' => [
        Middleware::class,
    ],
], function (Router $router) {
    Route::get('edge', [Controllers\EdgeController::class, 'index']);

    Route::post('edge/nano/auth', [Controllers\NanoController::class, 'auth'])
        ->withoutMiddleware([Permission::class, Authenticate::class, Middleware::class]);

    Route::post('edge/nano/super', [Controllers\NanoController::class, 'super'])
        ->withoutMiddleware([Permission::class, Authenticate::class]);

    Route::post('edge/nano/acl', [Controllers\NanoController::class, 'acl'])
        ->withoutMiddleware([Permission::class, Authenticate::class]);

    Route::post('edge/mqtt/auth', [Controllers\MqttController::class, 'auth']);

    // resource必须放最后面
    $router->resource('edge/mqtt/index', Controllers\MqttController::class);
    $router->resource('edge/nano/index', Controllers\NanoController::class);
});

// 免登录无限制
// Route::get('official', [Controllers\OfficialController::class, 'index'])->withoutMiddleware([Authenticate::class, Permission::class]);


