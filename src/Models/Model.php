<?php

namespace DagaSmart\Edge\Models;

use DagaSmart\BizAdmin\Models\BaseModel;

/**
 *基座模型
 */
class Model extends BaseModel
{
    const ?string schema = 'master'; // 空值默认数据库

    public function __construct()
    {
        if (! empty(self::schema)) {
            $this->setConnection(self::schema);
        }
        parent::__construct();
    }

    protected static function booted(): void
    {
        parent::booted();
    }
}
