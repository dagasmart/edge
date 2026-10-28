<?php
declare(strict_types=1);
namespace DagaSmart\Edge\Http\Controllers;

use DagaSmart\Edge\Services\EdgeGatewayService;
use Illuminate\Http\Request;

class EdgeGatewayController extends AdminController
{
    protected string $serviceName = EdgeGatewayService::class;

    /**
     * 边缘网关 客户端认证
     * ✅ NanoMQ 主认证已切到 EdgeAuth Swow (:9503)
     * 以下方法仅用于后台管理/手动测试/降级备用
     * POST /admin-api/extension/edge/gateway/auth
     */
    public function auth(Request $request)
    {
        $username = $request->input('username');
        $password = $request->input('password');
        $clientid = $request->input('clientid', '');
        $ipaddr = $request->input('ipaddr', $request->ip());
        $service = $request->input('service', 'nano');

        if (empty($clientid)) {
            return response()->json(['result' => 'deley'], 400);
        }

        $user = $this->service->auth($username, $password, $clientid, $ipaddr, $service);

        if ($user) {
            return response()->json(['result' => 'allow'], 200);
        }

        return response()->json(['result' => 'deley'], 300);
    }

    /**
     * NanoMQ 超级用户判定
     * POST /admin-api/extension/edge/gateway/super
     */
    public function super(Request $request)
    {
        $username = $request->input('username');
        $clientid = $request->input('clientid', '');

        // admin / gateway 视为超级用户
        if ($username === 'admin' || $clientid === 'gateway01') {
            return response()->json(['result' => 'allow'], 200);
        }

        // 也可以走 service 层查 DB
        $isSuper = $this->service->isSuperUser($username, $clientid);
        if ($isSuper) {
            return response()->json(['result' => 'allow'], 200);
        }

        // 已对齐 EdgeAuth handleSuper 逻辑
        // 返回 deny（superuser 由 EdgeAuth 的 ACL 规则覆盖）
        return response()->json(['result' => 'deley'], 300);
    }

    /**
     * NanoMQ ACL 鉴权
     * POST /admin-api/extension/edge/gateway/acl
     */
    public function acl(Request $request)
    {
        // 已对齐 EdgeAuth handleAcl 逻辑
        $clientid = $request->input('clientid', '');
        $username = $request->input('username', '');
        $topic = $request->input('topic', '');
        $access = (int) $request->input('access', 0); // 1=sub, 2=pub
        $ipaddr = $request->input('ipaddr', $request->ip());

        // 空字段拒绝
        if ($clientid === '' || $username === '' || $topic === '') {
            return response()->json(['result' => 'deley'], 300);
        }

        // 超级用户全放行
        if ($username === 'admin' || $clientid === 'gateway01') {
            return response()->json(['result' => 'allow'], 200);
        }

        // 走 service 层做细粒度判定
        $allowed = $this->service->checkAcl($clientid, $username, $topic, $access, $ipaddr);

        if ($allowed) {
            return response()->json(['result' => 'allow'], 200);
        }

        return response()->json(['result' => 'deley'], 300);
    }
}
