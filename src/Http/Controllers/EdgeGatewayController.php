<?php
declare(strict_types=1);
namespace DagaSmart\Edge\Http\Controllers;

use DagaSmart\BizAdmin\Renderers\Form;
use DagaSmart\BizAdmin\Renderers\Page;
use DagaSmart\Edge\Services\EdgeGatewayService;
use Illuminate\Http\Request;

class EdgeGatewayController extends AdminController
{
    protected string $serviceName = EdgeGatewayService::class;

    // ========== CRUD 列表 ==========
    public function list(): Page
    {
        $crud = $this->baseCRUD()
            ->filterTogglable(false)
            ->headerToolbar([
                $this->createButton('dialog', '250'),
            ])
            ->autoGenerateFilter()
            ->affixHeader()
            ->columnsTogglable()
            ->footable(['expand' => 'first'])
            ->autoFillHeight(true)
            ->interval(10000)
            ->silentPolling()
            ->rowClassNameExpr('${ change > 0 ? "text-red-500" : (change < 0 ? "text-green-500" : "") }')
            ->columns([
                amis()->TableColumn('id', 'ID')
                    ->sortable()
                    ->set('fixed', 'left')
                    ->width(80),
                amis()->TableColumn('gateway', '网关名称')
                    ->searchable(['name' => 'gateway', 'type' => 'input-text', 'placeholder' => '请输入网关名称'])
                    ->copyable()
                    ->set('fixed', 'left'),
                amis()->TableColumn('code', '标识code')
                    ->searchable(['name' => 'code', 'type' => 'input-text', 'placeholder' => '请输入服务标识'])
                    ->set('fixed', 'left'),
                amis()->TableColumn('ip', '域名ip')
                    ->quickEdit([
                        'type' => 'text',
                    ])
                    ->copyable(),
                amis()->TableColumn('port', '端口port')
                    ->quickEdit([
                        'type' => 'number',
                    ])
                    ->copyable(),
                amis()->TableColumn(' ca', 'CA证书')
                    ->copyable(),
                amis()->TableColumn('updated_at', '更新时间')
                    ->type('datetime')
                    ->width(150),
                $this->rowActions([
                    amis()->LinkAction()
                        ->label('设置')
                        ->level('link')
                        ->actionType('drawer')
                        ->drawer(
                            amis()->Drawer()
                                ->title(false)
                                ->closeOnEsc()
                                ->closeOnOutside()
                                ->body(
                                    amis()->Service()->api(admin_url('task-schedule/${id}/preview'))
                                        ->interval(30000)
                                        ->silentPolling()
                                        ->body([
                                            amis()->Tabs()->tabsMode('strong')->tabs([
                                                amis()->Tab()->title('网关设置')->icon('menu')->body([
                                                    amis()->TextControl('gateway', '网关名称')
                                                        ->clearable()
                                                        ->size('md')
                                                        ->required(),
                                                    amis()->SelectControl('code', '标识code')
                                                        ->options([
                                                            ['label' => 'NANO', 'value' => 'nano'],
                                                            ['label' => 'MQTT', 'value' => 'mqtt'],
                                                            ['label' => 'EMQX', 'value' => 'emqx'],
                                                        ])
                                                        ->clearable()
                                                        ->size('md')
                                                        ->required(),
                                                    amis()->TextControl('ip', '域名ip')
                                                        ->clearable()
                                                        ->size('md')
                                                        ->required(),
                                                    amis()->NumberControl('port', '端口port')
                                                        ->min(80)
                                                        ->max(49150)
                                                        ->size('md')
                                                        ->required(),
                                                ]),
                                            ]),
                                        ]),
                                ),
                        ),
                    $this->rowShowButton('drawer'),
                    $this->rowEditButton('drawer'),
                    $this->rowDeleteButton(),
                ])
                    ->set('align', 'center')
                    ->set('fixed', 'right')
                    ->set('width', 200),
            ]);

        return $this->baseList($crud);
    }

    public function form($isEdit = false): Form
    {
        return $this->baseForm()->body([
            amis()->Tabs()->tabsMode('line')->tabs([
                amis()->Tab()->title('网关信息')->icon('menu')->body([
                    amis()->TextControl('gateway', '网关名称')
                        ->clearable()
                        ->size('md')
                        ->required(),
                    amis()->SelectControl('code', '标识code')
                        ->options([
                            ['label' => 'NANO', 'value' => 'nano'],
                            ['label' => 'MQTT', 'value' => 'mqtt'],
                            ['label' => 'EMQX', 'value' => 'emqx'],
                        ])
                        ->clearable()
                        ->size('md')
                        ->required(),
                    amis()->TextControl('ip', '域名ip')
                        ->clearable()
                        ->size('md')
                        ->required(),
                    amis()->NumberControl('port', '端口port')
                        ->min(80)
                        ->max(49150)
                        ->size('md')
                        ->required(),
                ]),

            ]),
        ]);
    }

    public function detail(): Form
    {
        return $this->baseDetail()->body([
            amis()->Tabs()->tabsMode('line')->tabs([
                amis()->Tab()->title('网关信息')->icon('menu')->body([
                    amis()->TextControl('gateway', '网关名称')
                        ->clearable()
                        ->size('md')
                        ->required(),
                    amis()->SelectControl('code', '标识code')
                        ->options([
                            ['label' => 'NANO', 'value' => 'nano'],
                            ['label' => 'MQTT', 'value' => 'mqtt'],
                            ['label' => 'EMQX', 'value' => 'emqx'],
                        ])
                        ->clearable()
                        ->size('md')
                        ->required(),
                    amis()->TextControl('ip', '域名ip')
                        ->clearable()
                        ->size('md')
                        ->required(),
                    amis()->TextControl('port', '端口port')
                        ->min(80)
                        ->max(49150)
                        ->size('md')
                        ->required(),
                ]),
                amis()->Tab()->title('网关设置')->icon('menu')->body([
                    amis()->TextControl('gateway', '网关名称')
                        ->clearable()
                        ->size('md')
                        ->required(),
                    amis()->SelectControl('code', '标识code')
                        ->options([
                            ['label' => 'NANO', 'value' => 'nano'],
                            ['label' => 'MQTT', 'value' => 'mqtt'],
                            ['label' => 'EMQX', 'value' => 'emqx'],
                        ])
                        ->clearable()
                        ->size('md')
                        ->required(),
                    amis()->TextControl('ip', '域名ip')
                        ->clearable()
                        ->size('md')
                        ->required(),
                    amis()->TextControl('port', '端口port')
                        ->min(80)
                        ->max(49150)
                        ->size('md')
                        ->required(),
                ]),
            ]),
        ])->static();
    }


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
}
