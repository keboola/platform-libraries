<?php

declare(strict_types=1);

namespace Keboola\OutputMapping\Storage;

class TableInfo
{
    public array $primaryKey;

    public function __construct(private readonly array $tableInfo) {
        $this->primaryKey = $this->tableInfo['primaryKey'];
    }

    /** @deprecated  */
    public function asArray()
    {
        return $this->tableInfo;
    }
}
