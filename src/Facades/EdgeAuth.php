<?php
declare(strict_types=1);

namespace DagaSmart\Edge\Facades;

use DagaSmart\Edge\Services\EdgeAuthSignerService;
use Illuminate\Support\Facades\Facade;

/**
 * 边缘网关验签门面
 * @method static array sign(string $clientId, string $secret, ?int $timestamp = null)
 * @method static bool verify(string $clientId, string $password, string $secret, int $timeDrift = 300)
 * @method static array signFromRedis(string $clientId, string|null $redisKey = null)
 *
 * @see EdgeAuthSignerService
 */
class EdgeAuth extends Facade
{
    protected static function getFacadeAccessor(): string
    {
        return EdgeAuthSignerService::class;
    }
}


//use DagaSmart\Edge\Facades\EdgeAuth;
//$c = EdgeAuth::sign('device-1', 'EqNGZn95UL5m0QGoX1MiG5BVzda1Lp5tQ3RSkPnggH4');
//$c['password']
//"abc123...|1701234567|a1b2c3d4"
