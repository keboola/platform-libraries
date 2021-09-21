<?php

namespace Keboola\InputMapping\Tests\Table\Result;

use Keboola\InputMapping\Table\Result\Column;
use Keboola\InputMapping\Table\Result\TableInfo;
use PHPUnit\Framework\TestCase;

class TableInfoTest extends TestCase
{
    public function testAccessors()
    {
        $tableInfoArray = [
            'id' => 'in.c-application-testing.cashier-data',
            'name' => 'cashier-data',
            'displayName' => 'cashier-data-display',
            'sourceTable' => [
                'id' => 'in.c-source.table',
            ],
            'columns' => [
                'time_spent_in_shop',
                'number_of_items',
            ],
            'lastImportDate' => '2015-11-03T10:58:31+0100',
            'lastChangeDate' => '2020-11-03T10:58:31+0100',
        ];
        $tableInfo = new TableInfo($tableInfoArray);
        self::assertSame('in.c-application-testing.cashier-data', $tableInfo->getId());
        self::assertSame('cashier-data', $tableInfo->getName());
        self::assertSame('cashier-data-display', $tableInfo->getDisplayName());
        self::assertSame('2020-11-03T10:58:31+0100', $tableInfo->getLastChangeDate());
        self::assertSame('2015-11-03T10:58:31+0100', $tableInfo->getLastImportDate());
        self::assertSame('in.c-source.table', $tableInfo->getSourceTableId());
        self::assertEquals(
            [
                new Column('time_spent_in_shop', []),
                new Column('number_of_items', []),
            ],
            iterator_to_array($tableInfo->getColumns())
        );
    }

    /**
     * @dataProvider metadataTableInfoProvider
     */
    public function testMetadata(array $tableInfoArray, array $expected)
    {
        $tableInfo = new TableInfo($tableInfoArray);
        self::assertEquals(
            $expected,
            iterator_to_array($tableInfo->getColumns())
        );
    }

    public function testMetadataSource()
    {
        $tableInfo = [
            'id' => 'in.c-application-testing.cashier-data',
            'name' => 'cashier-data',
            'displayName' => 'cashier-data-display',
            'sourceTable' => [
                'id' => 'in.c-source.table',
                'columnMetadata' => [
                    'number_of_items' => [[
                        'id' => '207947778',
                        'key' => 'KBC.datatype.basetype',
                        'value' => 'INTEGER',
                        'provider' => 'user',
                        'timestamp' => '2019-08-14T16:55:34+0200',
                    ]],
                ],
            ],
            'columns' => [
                'time_spent_in_shop',
                'number_of_items',
            ],
            'columnMetadata' => [
                'number_of_items' => [[
                    'id' => '207947778',
                    'key' => 'KBC.datatype.basetype',
                    'value' => 'INTEGER',
                    'provider' => 'user',
                    'timestamp' => '2019-08-14T16:55:34+0200',
                ]],
            ],
            'lastImportDate' => '2015-11-03T10:58:31+0100',
            'lastChangeDate' => '2020-11-03T10:58:31+0100',
        ];
        $tableInfo = new TableInfo($tableInfo);
        self::assertEquals(
            [
                new Column('time_spent_in_shop', []),
                new Column(
                    'number_of_items',
                    [[
                        'key' => 'KBC.datatype.basetype',
                        'value' => 'INTEGER',
                        'provider' => 'user',
                        'timestamp' => '2019-08-14T16:55:34+0200',
                    ]]
                ),
            ],
            iterator_to_array($tableInfo->getColumns())
        );
    }

    public function metadataTableInfoProvider()
    {
        yield 'base' => [
            'tableInfo' => [
                'id' => 'in.c-application-testing.cashier-data',
                'name' => 'cashier-data',
                'displayName' => 'cashier-data-display',
                'sourceTable' => [
                    'id' => 'in.c-source.table',
                    'columnMetadata' => [],
                ],
                'columns' => [
                    'time_spent_in_shop',
                    'number_of_items',
                ],
                'columnMetadata' => [
                    'number_of_items' => [[
                        'id' => '207947778',
                        'key' => 'KBC.datatype.basetype',
                        'value' => 'INTEGER',
                        'provider' => 'user',
                        'timestamp' => '2019-08-14T16:55:34+0200',
                    ]],
                ],
                'lastImportDate' => '2015-11-03T10:58:31+0100',
                'lastChangeDate' => '2020-11-03T10:58:31+0100',
            ],
            'expected' => [
                new Column('time_spent_in_shop', []),
                new Column(
                    'number_of_items',
                    [[
                        'key' => 'KBC.datatype.basetype',
                        'value' => 'INTEGER',
                        'provider' => 'user',
                        'timestamp' => '2019-08-14T16:55:34+0200',
                    ]]
                ),
            ],
        ];
        yield 'source' => [
            'tableInfo' => [
                'id' => 'in.c-application-testing.cashier-data',
                'name' => 'cashier-data',
                'displayName' => 'cashier-data-display',
                'sourceTable' => [
                    'id' => 'in.c-source.table',
                    'columnMetadata' => [
                        'number_of_items' => [[
                            'id' => '207947778',
                            'key' => 'KBC.datatype.basetype',
                            'value' => 'INTEGER',
                            'provider' => 'user',
                            'timestamp' => '2019-08-14T16:55:34+0200',
                        ]],
                    ],
                ],
                'columns' => [
                    'time_spent_in_shop',
                    'number_of_items',
                ],
                'columnMetadata' => [],
                'lastImportDate' => '2015-11-03T10:58:31+0100',
                'lastChangeDate' => '2020-11-03T10:58:31+0100',
            ],
            'expected' => [
                new Column('time_spent_in_shop', []),
                new Column(
                    'number_of_items',
                    [[
                        'key' => 'KBC.datatype.basetype',
                        'value' => 'INTEGER',
                        'provider' => 'user',
                        'timestamp' => '2019-08-14T16:55:34+0200',
                    ]]
                ),
            ],
        ];
        yield 'base and source' => [
            'tableInfo' => [
                'id' => 'in.c-application-testing.cashier-data',
                'name' => 'cashier-data',
                'displayName' => 'cashier-data-display',
                'sourceTable' => [
                    'id' => 'in.c-source.table',
                    'columnMetadata' => [
                        'number_of_items' => [[
                            'id' => '2079477781',
                            'key' => 'KBC.datatype.basetype1',
                            'value' => 'INTEGER1',
                            'provider' => 'user1',
                            'timestamp' => '2020-08-14T16:55:34+0200',
                        ]],
                    ],
                ],
                'columns' => [
                    'time_spent_in_shop',
                    'number_of_items',
                ],
                'columnMetadata' => [
                    'number_of_items' => [[
                        'id' => '207947778',
                        'key' => 'KBC.datatype.basetype',
                        'value' => 'INTEGER',
                        'provider' => 'user',
                        'timestamp' => '2019-08-14T16:55:34+0200',
                    ]],
                ],
                'lastImportDate' => '2015-11-03T10:58:31+0100',
                'lastChangeDate' => '2020-11-03T10:58:31+0100',
            ],
            'expected' => [
                new Column('time_spent_in_shop', []),
                new Column(
                    'number_of_items',
                    [[
                        'key' => 'KBC.datatype.basetype',
                        'value' => 'INTEGER',
                        'provider' => 'user',
                        'timestamp' => '2019-08-14T16:55:34+0200',
                    ]]
                ),
            ],
        ];
    }
}
