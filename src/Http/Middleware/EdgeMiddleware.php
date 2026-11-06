<?php
declare(strict_types=1);

namespace DagaSmart\Edge\Http\Middleware;

use Closure;
use Illuminate\Http\Request;
use Symfony\Component\HttpFoundation\Response;

/**
 * Edge 内部接口鉴权中间件
 *
 * 保护 Swow → Laravel 的内部回源接口，
 * 仅允许携带合法内部 Token 的请求访问。
 */
class EdgeMiddleware
{
    public function handle(Request $request, Closure $next): Response
    {
        $expectedToken = config('edge.internal_token')
            ?? env('EDGE_INTERNAL_TOKEN');

        if ($expectedToken === null || $expectedToken === '' || $expectedToken === '0') {
            // 未配置 Token，拒绝访问（fail-closed）
            abort(503, 'Edge internal token not configured');
        }

        $providedToken = $request->header('X-Edge-Internal');

        if ($providedToken === null || ! hash_equals($expectedToken, $providedToken)) {
            abort(403, 'Forbidden');
        }

        return $next($request);
    }
}
