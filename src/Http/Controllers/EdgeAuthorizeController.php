<?php

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
                $this->createButton('dialog', 250),
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
                amis()->TableColumn('secret', '密钥secret')
                    ->copyable(),
                amis()->TableColumn('secret_change_at', '更新时间')
                    ->type('datetime')
                    ->width(150),
                amis()->TableColumn('created_at', '创建时间')
                    ->type('datetime')
                    ->sortable()
                    ->width(150),
                $this->rowActions()
                    ->set('align', 'center')
                    ->set('fixed', 'right')
                    ->set('width', 180),
            ]);

        return $this->baseList($crud);
    }

    public function form($isEdit = false): Form
    {
        return $this->baseForm()->body([]);
    }

    public function detail(): Form
    {
        return $this->baseDetail()->body([])->static();
    }
}
