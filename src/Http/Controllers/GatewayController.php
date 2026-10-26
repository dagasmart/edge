<?php

namespace DagaSmart\Edge\Http\Controllers;

use DagaSmart\BizAdmin\Renderers\Json;
use Illuminate\Http\JsonResponse;
use Illuminate\Http\Request;
use Illuminate\Support\Facades\Hash;
use DagaSmart\Edge\Services\EdgeGatewayService;


class GatewayController extends AdminController
{
    protected string $serviceName = EdgeGatewayService::class;

    /**
     * NanoMQ 客户端鉴权
     * - NanoMQ 会把 username / password 以 form 形式 POST 过来
     * - Hash::make('123456')
     */
    public function auth(Request $request)
    {
        $username = $request->input('username');
        $password = $request->input('password');
        $clientid = $request->input('clientid', '');
        $ipaddr   = $request->input('ipaddr', $request->ip());

        if (empty($clientid)) {
            return response()->json(['result' => 'deley'], 300);
        }

        dd(Hash::make('123456'));

        $user = $this->service->auth($username, $password, $clientid, $ipaddr);

        if ($user) {
            // ✅ 认证通过：返回 204 No Content
            return response()->json(['result' => 'allow'], 200);
        }

        // ❌ 认证失败：返回 401
        return response()->json(['result' => 'deley'], 300);
    }
}
