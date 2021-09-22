<?php

namespace Keboola\OutputMapping\Tests\Table;

use Keboola\InputMapping\Table\Result\TableInfo;
use Keboola\OutputMapping\Table\Result;
use PHPUnit\Framework\TestCase;

class ResultTest extends TestCase
{
    public function testAddTable()
    {
        $tablesResult = new Result();

        /** @var TableInfo[] $tables */
        $tables = iterator_to_array($tablesResult->getTables());
        self::assertCount(0, $tables);

        $table1 = new TableInfo([
            'id' => 'in.c-main.table',
            'name' => 'table',
            'displayName' => 'My table',
            'columns' => ['id', 'name'],
            'lastImportDate' => null,
        ]);

        $tablesResult->addTable($table1);

        $table2 = new TableInfo([
            'id' => 'in.c-main.other-table',
            'name' => 'other-table',
            'displayName' => 'Other table',
            'columns' => ['id'],
            'lastImportDate' => null,
        ]);

        $tablesResult->addTable($table2);

        /** @var TableInfo[] $tables */
        $tables = iterator_to_array($tablesResult->getTables());
        self::assertCount(2, $tables);

        $table2 = array_pop($tables);
        $table1 = array_pop($tables);

        self::assertSame('in.c-main.table', $table1->getId());
        self::assertNull($table1->getLastImportDate());
        self::assertSame('in.c-main.other-table', $table2->getId());

        $expectedImportData = '2019-08-12T21:16:41+0200';
        $table1Update = new TableInfo([
            'id' => 'in.c-main.table',
            'name' => 'table',
            'displayName' => 'My table',
            'columns' => ['id', 'name'],
            'lastImportDate' => $expectedImportData,

        ]);

        $tablesResult->addTable($table1Update);

        /** @var TableInfo[] $tables */
        $tables = iterator_to_array($tablesResult->getTables());
        self::assertCount(3, $tables);

        $table1 = array_pop($tables);
        self::assertSame('in.c-main.table', $table1->getId());
        self::assertSame($expectedImportData, $table1->getLastImportDate());
    }
}
