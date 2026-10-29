<?php
declare(strict_types=1);
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
    // Route::get('edge', [Controllers\EdgeController::class, 'index']);

    Route::post('edge/auth', [Controllers\EdgeController::class, 'auth'])
        ->withoutMiddleware([Permission::class, Authenticate::class, Middleware::class]);

    Route::post('edge/super', [Controllers\EdgeController::class, 'super'])
        ->withoutMiddleware([Permission::class, Authenticate::class]);

    Route::post('edge/acl', [Controllers\EdgeController::class, 'acl'])
        ->withoutMiddleware([Permission::class, Authenticate::class]);

    // resource必须放最后面
    $router->resource('edge/gateway', Controllers\EdgeGatewayController::class);
    $router->resource('edge/authorize', Controllers\EdgeAuthorizeController::class);
});

// 免登录无限制
// Route::get('official', [Controllers\OfficialController::class, 'index'])->withoutMiddleware([Authenticate::class, Permission::class]);


