<?php

namespace DagaSmart\Edge\Services;

use DagaSmart\Edge\Models\EdgeAuthorize;
use DagaSmart\Edge\Models\EdgeGateway;
use Illuminate\Support\Facades\Hash;

/**
 * 网关-服务类
 */
class EdgeGatewayService extends AdminService
{
    protected string $modelName = EdgeGateway::class;

    /**
     * 网关用户认证
     */
    public function auth($username, $password, $clientid = null, $ipaddr = null): ?EdgeAuthorize
    {
        // 查询用户表（按你的实际表结构调整）
        $user = EdgeAuthorize::query()
            ->where(['username' => $username])
            ->where(['active' => true])
            ->first();

        // 密码校验：推荐用 password_hash() 存储
        if ($user && Hash::check($password, $user->password)) {
            return $user;
        }

        return null;
    }
}
