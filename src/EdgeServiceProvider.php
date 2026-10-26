<?php
declare(strict_types=1);
namespace DagaSmart\Edge;

use Exception;
use DagaSmart\BizAdmin\Renderers\Form;
use DagaSmart\BizAdmin\Renderers\TextControl;
use DagaSmart\BizAdmin\Extend\ServiceProvider;
use Illuminate\Console\Command;
use ReflectionClass;


class EdgeServiceProvider extends ServiceProvider
{
	protected $menu = [];

    /**
     * @return void
     * @throws Exception
     */
    public function register(): void
    {
        parent::register();

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
