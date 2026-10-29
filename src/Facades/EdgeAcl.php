<?php
declare(strict_types=1);
namespace DagaSmart\Edge\Facades;

use DagaSmart\Edge\Services\EdgeAclEngineService;
use Illuminate\Support\Facades\Facade;

/**
 * @method static bool decide(string $clientId, string $username, int $access, string $topic, string $ip = '')
 * @method static bool isRevoked(string $clientId)
 * @method static bool isSuperUser(string $username)
 * @method static string getDeviceType(string $clientId)
 * @method static string getDeviceTenant(string $clientId)
 * @method static bool matchRule(string $type, string $tenant, string $clientId, int $access, string $topic)
 * @method static void setDeviceMeta(string $clientId, string $type, string $tenant)
 * @method static void revoke(string $clientId, int $ttl = 0)
 * @method static void unrevoke(string $clientId)
 * @method static void setSuperUser(string $username, bool $isSuper = true)
 *
 * @see EdgeAclEngineService
 */
class EdgeAcl extends Facade
{
    protected static function getFacadeAccessor(): string
    {
        return EdgeAclEngineService::class;
    }
}
