<?php

declare(strict_types=1);

namespace Keboola\InputMapping\Tests\Table\Strategy;

use Keboola\InputMapping\Table\Strategy\Local;
use Keboola\StorageApi\TableExporter;

class TestLocalStrategy extends Local
{
    private TableExporter $tableExporter;

    public function setTableExporter(TableExporter $tableExporter): void
    {
        $this->tableExporter = $tableExporter;
    }

    protected function createTableExporter(): TableExporter
    {
        return $this->tableExporter;
    }
}
