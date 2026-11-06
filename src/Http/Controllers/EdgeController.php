<?php
declare(strict_types=1);
namespace DagaSmart\Edge\Http\Controllers;

use DagaSmart\Edge\Services\EdgeService;
use Illuminate\Http\JsonResponse;
use Illuminate\Http\Request;

class EdgeController extends AdminController
{
    protected string $serviceName = EdgeService::class;

    /**
     * 边缘网关 客户端认证
     * ✅ NanoMQ 主认证已切到 EdgeAuth Swow (:9503)
     * 以下方法仅用于后台管理/手动测试/降级备用
     * POST /admin-api/extension/edge/auth
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
     * POST /admin-api/extension/edge/super
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
     * POST /admin-api/extension/edge/acl
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

    /**
     * EdgeAuth Redis miss 回源接口
     *
     * 由 Swow 端 getDeviceSecret() 在 Redis 双 miss 时调用。
     * 从 PG 加载 bcrypt 密码哈希和 HMAC secret，返回 JSON。
     *
     * 安全：由 EdgeInternalMiddleware 校验 X-Edge-Internal 头
     */
    public function authFallback(Request $request): JsonResponse
    {
        $clientId = $request->query('client_id', '');
        $username = $request->query('username', '');
        $service = $request->query('service', 'nano');

        if ($clientId === '' || $username === '') {
            return response()->json([
                'error' => 'missing_params',
                'message' => 'client_id and username are required',
            ], 400);
        }

        // 参数长度防护（与 DB 字段长度匹配）
        if (strlen($clientId) > 128 || strlen($username) > 64) {
            return response()->json([
                'error' => 'invalid_params',
            ], 400);
        }

        /** @var EdgeAuthorizeService $service */
        $svc = app(EdgeAuthorizeService::class);
        $result = $svc->loadSecretFromPg($clientId, $username, $service);

        if ($result === null) {
            return response()->json([
                'error' => 'not_found',
            ], 404);
        }

        return response()->json([
            'password' => $result['password'],
            'secret' => $result['secret'],
        ]);
    }



}
