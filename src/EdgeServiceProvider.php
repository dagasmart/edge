<?php
declare(strict_types=1);
namespace DagaSmart\Edge;

use Exception;
use DagaSmart\BizAdmin\Renderers\Form;
use DagaSmart\BizAdmin\Renderers\TextControl;
use DagaSmart\BizAdmin\Extend\ServiceProvider;


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

        /**加载路由**/
        parent::registerRoutes(__DIR__.'/Http/routes.php');
        /**加载语言包**/
        if ($lang = parent::getLangPath()) {
            $this->loadTranslationsFrom($lang, $this->getCode());
        }

    }

	public function settingForm(): ?Form
	{
	    return $this->baseSettingForm()->body([
            TextControl::make()->name('value')->label('Value')->required(),
	    ]);
	}
}
