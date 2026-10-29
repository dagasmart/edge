<?php
declare(strict_types=1);
namespace DagaSmart\Edge\Http\Middleware;

use Closure;
use Illuminate\Http\Request;

class Middleware
{
    public function handle(Request $request, Closure $next)
    {
        if (admin_extension_expiry('dagasmart.stock')) {
            return admin_response()->fail('软件已过期,请续费');
        }
        if (! admin_extension_enabled('dagasmart.stock')) {
            return admin_response()->fail('软件已禁用，请开启');
        }

        $params = [];
        $request->attributes->add($params);

        return $next($request);
    }
}
