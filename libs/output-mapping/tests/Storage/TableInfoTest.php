<?php

declare(strict_types=1);

namespace Keboola\OutputMapping\Tests\Storage;

use Keboola\OutputMapping\Storage\TableInfo;
use PHPUnit\Framework\TestCase;

class TableInfoTest extends TestCase
{
    public function testBasic(): void
    {
        $tableInfo = new TableInfo([
            'id' => 'tableId',
            'columns' => ['column1', 'column2'],
            'isTyped' => true,
            'primaryKey' => ['column1'],
        ]);

        $this->assertEquals(['column1', 'column2'], $tableInfo->getColumns());
        $this->assertEquals('tableId', $tableInfo->getId());
        $this->assertTrue($tableInfo->isTyped());
        $this->assertEquals(['column1'], $tableInfo->getPrimaryKey());
    }
}
