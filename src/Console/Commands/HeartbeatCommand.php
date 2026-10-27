<?php
declare(strict_types=1);
namespace DagaSmart\Edge\Console\Commands;


class HeartbeatCommand extends Command
{

    protected $signature = 'edge:heartbeat';

    protected $description = 'nanomq活检';

    public function __handle(): int
    {
        $this->info('任务开始...');

        //新函数 json_validate,该函数简化了 JSON 验证，使其更加高效和简单
        $jsonString = '{"姓名": "小明", "年龄": 20}';
        $isJsonValid = json_validate($jsonString); //检测json有效性
        $this->info('是否json', [$isJsonValid]);

        //Randomizer 增强,新增了从字符串生成随机字节以及获取指定范围内的随机浮点数
        $randomizer = new \Random\Randomizer();
        $randomBytes = $randomizer->getBytesFromString('abcdefghijkmnpqrstuvwxyz1234567890', 4);
        $randomFloat = $randomizer->getFloat(0.0, 1.0);

        $this->info('任务执行', [
            'task_id' => time(),
            'command' => 'heartbeat',
            'duration' => [$randomBytes, $randomFloat],
        ]);

        $this->info('任务完成');

        return self::SUCCESS;
    }
}
