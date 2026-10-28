<?php

declare(strict_types=1);

namespace DagaSmart\Edge;

use DagaSmart\BizAdmin\Extend\ServiceProvider;
use DagaSmart\BizAdmin\Renderers\Form;
use DagaSmart\BizAdmin\Renderers\TextControl;
use DagaSmart\Edge\Services\EdgeAclEngineService;
use DagaSmart\Edge\Services\EdgeAuthSignerService;
use Exception;
use Illuminate\Console\Command;
use ReflectionClass;

class EdgeServiceProvider extends ServiceProvider
{
    protected $menu = [
        [
            'parent' => null,
            'title' => '边缘网关',
            'url' => '/extension/edge',
            'url_type' => 1,
            'icon' => 'carbon:gateway-security',
        ],
        [
            'parent' => '边缘网关',
            'title' => '网关服务',
            'url' => '/admin-api/extension/edge/gateway',
            'url_type' => 1,
            'icon' => 'carbon:ibm-cloud-transit-gateway',
        ],
        [
            'parent' => '边缘网关',
            'title' => '网关认证',
            'url' => '/extension/edge/authorize',
            'url_type' => 1,
            'icon' => 'hugeicons:authorized',
        ],
    ];

    protected $auth = [
        // 网关服务
        ['name' => '新增', 'namespace' => 'dagasmart.edge',  'code' => 'extension.edge.gateway', 'abbr' => 'create', 'custom_order' => 1],
        ['name' => '删除', 'namespace' => 'dagasmart.edge',  'code' => 'extension.edge.gateway', 'abbr' => 'delete', 'custom_order' => 2],
        ['name' => '编辑', 'namespace' => 'dagasmart.edge',  'code' => 'extension.edge.gateway', 'abbr' => 'update', 'custom_order' => 3],
        ['name' => '查看', 'namespace' => 'dagasmart.edge',  'code' => 'extension.edge.gateway', 'abbr' => 'showed', 'custom_order' => 4],
        ['name' => '筛选', 'namespace' => 'dagasmart.edge',  'code' => 'extension.edge.gateway', 'abbr' => 'search', 'custom_order' => 5],
        ['name' => '设置', 'namespace' => 'dagasmart.edge',  'code' => 'extension.edge.gateway', 'abbr' => 'config', 'custom_order' => 6],
        // 网关认证
        ['name' => '新增', 'namespace' => 'dagasmart.edge',  'code' => 'extension.edge.authorize', 'abbr' => 'create', 'custom_order' => 1],
        ['name' => '删除', 'namespace' => 'dagasmart.edge',  'code' => 'extension.edge.authorize', 'abbr' => 'delete', 'custom_order' => 2],
        ['name' => '编辑', 'namespace' => 'dagasmart.edge',  'code' => 'extension.edge.authorize', 'abbr' => 'update', 'custom_order' => 3],
        ['name' => '查看', 'namespace' => 'dagasmart.edge',  'code' => 'extension.edge.authorize', 'abbr' => 'showed', 'custom_order' => 4],
        ['name' => '筛选', 'namespace' => 'dagasmart.edge',  'code' => 'extension.edge.authorize', 'abbr' => 'search', 'custom_order' => 5],
    ];


    /**
     * @throws Exception
     */
    public function register(): void
    {
        parent::register();

        /** ✅ 批量注册配置文件 */
        $this->registerConfigs([
            'services.stock' => 'stock.php',
            'services.finance' => 'finance.php',
        ]);

        /**✅ 加载路由*/
        parent::registerRoutes(__DIR__.'/Http/routes.php');

        /**✅ 加载语言包*/
        if ($lang = parent::getLangPath()) {
            $this->loadTranslationsFrom($lang, $this->getCode());
        }

        /** ✅ 命令在 register 阶段就注册（Artisan 需要） */
        if ($this->app->runningInConsole()) {
            $this->commands($this->discoverCommands());
        }
        //网关签名验签服务
        $this->app->singleton(EdgeAuthSignerService::class, function () {
            return new EdgeAuthSignerService();
        });
        // 网关规则服务
        $this->app->singleton(EdgeAclEngineService::class, function () {
            return new EdgeAclEngineService();
        });

    }

    /** 批量注册配置文件 */
    protected function registerConfigs(array $maps): void
    {
        foreach ($maps as $key => $file) {
            $path = __DIR__."/../config/{$file}";
            if (file_exists($path)) {
                $this->mergeConfigFrom($path, $key);
            }
        }
    }

    /**
     * 自动扫描 Console/Commands 目录下的所有命令类
     *
     * @return array<class-string<Command>>
     */
    protected function discoverCommands(): array
    {
        $commands = [];
        $dir = __DIR__.'/Console/Commands';

        if (! is_dir($dir)) {
            return $commands;
        }

        foreach (glob($dir.'/*.php') ?: [] as $file) {
            $class = __NAMESPACE__.'\\Console\\Commands\\'.pathinfo($file, PATHINFO_FILENAME);

            if (! class_exists($class)) {
                continue;
            }

            if ((new ReflectionClass($class))->isAbstract()) {
                continue;
            }

            if (is_subclass_of($class, Command::class)) {
                $commands[] = $class;
            }
        }

        return $commands;
    }

    public function settingForm(): ?Form
    {
        return $this->baseSettingForm()->body([
            TextControl::make()->name('value')->label('Value')->required(),
        ]);
    }
}
