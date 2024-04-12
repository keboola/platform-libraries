<?php

declare(strict_types=1);

namespace Keboola\OutputMapping\Tests\Storage;

use Keboola\OutputMapping\Storage\TableInfo;
use PHPUnit\Framework\TestCase;

class TableInfoTest extends TestCase
{
    public function testBasic(): void
    {
        $this->tableInfo = new TableInfo([
            'id' => 'tableId',
            'columns' => ['column1', 'column2'],
            'isTyped' => true,
            'primaryKey' => ['column1'],
        ]);

        $this->assertEquals(['column1', 'column2'], $this->tableInfo->getColumns());
        $this->assertEquals('tableId', $this->tableInfo->getId());
        $this->assertTrue($this->tableInfo->isTyped());
        $this->assertEquals(['column1'], $this->tableInfo->getPrimaryKey());
    }
}
