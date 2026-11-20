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
        $this->assertTrue($tableInfo->isDescriptionSystemManaged());
    }

    public function testIsDescriptionSystemManaged(): void
    {
        $tableInfo = new TableInfo([
            'id' => 'tableId',
            'columns' => [],
            'isTyped' => true,
            'primaryKey' => [],
            'isDescriptionSystemManaged' => false,
        ]);

        $this->assertFalse($tableInfo->isDescriptionSystemManaged());
    }

    public function testGetLayout(): void
    {
        $layout = [
            'timePartitioning' => ['type' => 'DAY', 'field' => 'report_date', 'expirationMs' => '1000'],
            'rangePartitioning' => [
                'field' => 'id',
                'range' => ['start' => '0', 'end' => '100', 'interval' => '10'],
            ],
            'clustering' => ['fields' => ['country']],
        ];
        $tableInfo = new TableInfo([
            'id' => 'tableId',
            'definition' => $layout + [
                'primaryKeysNames' => [],
                'columns' => [],
                'requirePartitionFilter' => false,
                'partitions' => [],
            ],
        ]);

        self::assertSame($layout, $tableInfo->getLayout());
    }

    public function testGetLayoutWithoutLayout(): void
    {
        self::assertSame([], (new TableInfo(['id' => 'tableId']))->getLayout());
        self::assertSame(
            [],
            (new TableInfo(['id' => 'tableId', 'definition' => ['columns' => []]]))->getLayout(),
        );
    }
}
