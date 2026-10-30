<?php
declare(strict_types=1);
namespace DagaSmart\Edge\Http\Controllers;

use DagaSmart\BizAdmin\Renderers\Form;
use DagaSmart\BizAdmin\Renderers\Page;
use DagaSmart\Edge\Services\EdgeAuthorizeService;

/**
 * 边缘网关认证
 */
class EdgeAuthorizeController extends AdminController
{
    protected string $serviceName = EdgeAuthorizeService::class;

    // ========== CRUD 列表 ==========
    public function list(): Page
    {
        $crud = $this->baseCRUD()
            ->filterTogglable(false)
            ->headerToolbar([
                $this->createButton('dialog', '250'),
                ...$this->baseHeaderToolBar(),
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
                amis()->TableColumn('username', '账号')
                    ->searchable(['name' => 'username', 'type' => 'input-text', 'placeholder' => '请输入账号'])
                    ->copyable()
                    ->set('fixed', 'left'),
                amis()->TableColumn('client_id', '终端sn')
                    ->searchable(['name' => 'client_id', 'type' => 'input-text', 'placeholder' => '请输入终端sn'])
                    ->set('fixed', 'left'),
                amis()->TableColumn('active', '状态')
                    ->set('type', 'switch')
                    ->set('onText', '开启')
                    ->set('offText', '禁用')
                    ->width(80),
                amis()->TableColumn('is_super', '超管用户')
                    ->set('type', 'switch')
                    ->set('onText', '是')
                    ->set('offText', '否')
                    ->width(80),
                amis()->TableColumn('secret', '密钥secret')
                    ->copyable(),
                amis()->TableColumn('secret_change_at', '更新时间')
                    ->type('datetime')
                    ->width(150),
                $this->rowActions(true, '250')
                    ->set('align', 'center')
                    ->set('fixed', 'right')
                    ->set('width', 180),
            ]);

        return $this->baseList($crud);
    }

    public function form($isEdit = false): Form
    {
        return $this->baseForm()->body([
            amis()->Tabs()->tabsMode('line')->tabs([
                amis()->Tab()->title('认证信息')->icon('menu')->body([
                    amis()->TextControl('client_id', '终端sn')
                        ->clearable()
                        ->size('md')
                        ->required(),
                    amis()->TextControl('username', '账号')
                        ->clearable()
                        ->size('md')
                        ->required(),
                    amis()->TextControl('password', '密码')
                        ->type('input-password')   // ← 密码输入框，不回显明文
                        ->clearable()
                        ->size('md')
                        ->description('新增必填；编辑时留空则不修改')
                        ->required(),
                    amis()->SwitchControl('is_super', '超管')
                        ->onText('是')
                        ->offText('否')
                        ->value(false),
                    amis()->SwitchControl('active', '状态')
                        ->onText('开启')
                        ->offText('禁用')
                        ->value(true),
                ]),
                amis()->Tab()->title('网关信息')->icon('menu')->body([
                    amis()->SelectControl('service', '网关')
                        ->options([
                            ['label' => 'NANO', 'value' => 'nano'],
                            ['label' => 'MQTT', 'value' => 'mqtt'],
                            ['label' => 'EMQX', 'value' => 'emqx'],
                        ])
                        ->clearable()
                        ->size('md')
                        ->required(),
                    amis()->TextareaControl('remark', '描述')
                        ->clearable()
                        ->size('md'),
                ]),
            ]),
        ]);
    }

    public function detail(): Form
    {
        return $this->baseDetail()->body([
            amis()->Tabs()->tabsMode('line')->tabs([
                amis()->Tab()->title('认证信息')->icon('menu')->body([
                    amis()->TextControl('client_id', '终端sn')
                        ->clearable()
                        ->copyable()
                        ->size('md')
                        ->static(),
                    amis()->TextControl('username', '账号')
                        ->clearable()
                        ->size('md')
                        ->required(),
                    amis()->TextControl('password_', '密码')
                        ->clearable()
                        ->size('md')
                        ->value('******')
                        ->required(),
                    amis()->SwitchControl('is_super', '超管')
                        ->onText('是')
                        ->offText('否')
                        ->value(false)
                        ->static(false)
                        ->disabled(),
                    amis()->SwitchControl('active', '激活')
                        ->onText('是')
                        ->offText('否')
                        ->value(true)
                        ->static(false)
                        ->disabled(),
                ]),
                amis()->Tab()->title('网关信息')->icon('menu')->body([
                    amis()->SelectControl('service', '网关')
                        ->options([
                            ['label' => 'NANO', 'value' => 'nano'],
                            ['label' => 'MQTT', 'value' => 'mqtt'],
                            ['label' => 'EMQX', 'value' => 'emqx'],
                        ])
                        ->clearable()
                        ->size('md')
                        ->required(),
                    amis()->TextareaControl('remark', '网关描述')
                        ->clearable()
                        ->size('md'),
                    amis()->TextareaControl('secret', '签名密钥')
                        ->clearable()
                        ->size('md'),
                    amis()->DateTimeControl('secret_change_at', '生成时间')
                        ->clearable()
                        ->size('md'),
                ]),
            ]),
        ])->static();
    }
}
