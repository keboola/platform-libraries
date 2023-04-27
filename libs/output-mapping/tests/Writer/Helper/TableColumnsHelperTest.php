<?php

declare(strict_types=1);

namespace Keboola\OutputMapping\Tests\Writer\Helper;

use Generator;
use Keboola\OutputMapping\Writer\Helper\TableColumnsHelper;
use Keboola\StorageApi\Client;
use PHPUnit\Framework\TestCase;

class TableColumnsHelperTest extends TestCase
{
    private const TEST_COLUMNS_METADATA = [
        'id' => [],
        'name' => [],
        'address' => [
            [
                'key' => 'KBC.datatype.type',
                'value' => 'VARCHAR',
            ],
            [
                'key' => 'KBC.datatype.basetype',
                'value' => 'STRING',
            ],
        ],
        'crm id' => [
            [
                'key' => 'KBC.datatype.type',
                'value' => 'INT',
            ],
            [
                'key' => 'KBC.datatype.basetype',
                'value' => 'INTEGER',
            ],
        ],
    ];

    private const TEST_SNOWFLAKE_BACKEND_TABLE_METADATA = [
        [
            'key' => 'KBC.datatype.backend',
            'value' => 'snowflake',
        ],
    ];

    private const TEST_RANDOM_BACKEND_TABLE_METADATA = [
        [
            'key' => 'KBC.datatype.backend',
            'value' => 'random',
        ],
    ];

    public function addMissingColumnsToTypedTableProvider(): Generator
    {
        yield 'typed table - extra metadata columns in config' => [
            [
                'id' => 'in.c-output-mapping.testTable1',
                'isTyped' => true,
                'columns' => [
                    'id',
                    'name',
                ],
            ],
            [
                'metadata' => self::TEST_SNOWFLAKE_BACKEND_TABLE_METADATA,
                'column_metadata' => self::TEST_COLUMNS_METADATA,
            ],
            [
                [
                    'in.c-output-mapping.testTable1',
                    'address',
                    [
                        'type' => 'VARCHAR',
                        'length' => null,
                        'nullable' => true,
                    ],
                    null,
                ],
                [
                    'in.c-output-mapping.testTable1',
                    'crm_id',
                    [
                        'type' => 'INT',
                        'length' => null,
                        'nullable' => true,
                    ],
                    null,
                ],
            ],
        ];

        yield 'typed table - extra columns in config' => [
            [
                'id' => 'in.c-output-mapping.testTable1',
                'isTyped' => true,
                'columns' => [
                    'id',
                    'name',
                ],
            ],
            [
                'metadata' => self::TEST_SNOWFLAKE_BACKEND_TABLE_METADATA,
                'columns' => array_keys(self::TEST_COLUMNS_METADATA),
            ],
            [
                [
                    'in.c-output-mapping.testTable1',
                    'address',
                    null,
                    'STRING',
                ],
                [
                    'in.c-output-mapping.testTable1',
                    'crm_id',
                    null,
                    'STRING',
                ],
            ],
        ];

        yield 'typed table - extra metadata columns and columns in config' => [
            [
                'id' => 'in.c-output-mapping.testTable1',
                'isTyped' => true,
                'columns' => [
                    'id',
                    'name',
                ],
            ],
            [
                'metadata' => self::TEST_SNOWFLAKE_BACKEND_TABLE_METADATA,
                'columns' => array_keys(
                    array_slice(
                        self::TEST_COLUMNS_METADATA,
                        0,
                        -1,
                        true
                    )
                ),
                'column_metadata' => array_slice(
                    self::TEST_COLUMNS_METADATA,
                    -1,
                    1,
                    true
                ),
            ],
            [
                [
                    'in.c-output-mapping.testTable1',
                    'crm_id',
                    [
                        'type' => 'INT',
                        'length' => null,
                        'nullable' => true,
                    ],
                    null,
                ],
                [
                    'in.c-output-mapping.testTable1',
                    'address',
                    null,
                    'STRING',
                ],
            ],
        ];

        yield 'typed table - same extra metadata columns and columns in config' => [
            [
                'id' => 'in.c-output-mapping.testTable1',
                'isTyped' => true,
                'columns' => [
                    'id',
                    'name',
                ],
            ],
            [
                'metadata' => self::TEST_SNOWFLAKE_BACKEND_TABLE_METADATA,
                'columns' => array_keys(self::TEST_COLUMNS_METADATA),
                'column_metadata' => self::TEST_COLUMNS_METADATA,
            ],
            [
                [
                    'in.c-output-mapping.testTable1',
                    'address',
                    [
                        'type' => 'VARCHAR',
                        'length' => null,
                        'nullable' => true,
                    ],
                    null,
                ],
                [
                    'in.c-output-mapping.testTable1',
                    'crm_id',
                    [
                        'type' => 'INT',
                        'length' => null,
                        'nullable' => true,
                    ],
                    null,
                ],
            ],
        ];

        yield 'random backend - typed table - extra columns in config' => [
            [
                'id' => 'in.c-output-mapping.testTable2',
                'isTyped' => true,
                'columns' => [
                    'id',
                    'name',
                ],
            ],
            [
                'metadata' => self::TEST_RANDOM_BACKEND_TABLE_METADATA,
                'column_metadata' => self::TEST_COLUMNS_METADATA,
            ],
            [
                [
                    'in.c-output-mapping.testTable2',
                    'address',
                    null,
                    'STRING',
                ],
                [
                    'in.c-output-mapping.testTable2',
                    'crm_id',
                    null,
                    'INTEGER',
                ],
            ],
        ];
    }

    public function doNotAddColumnsProvider(): Generator
    {
        yield 'typed table - extra columns in table' => [
            [
                'isTyped' => true,
                'columns' => [
                    'id',
                    'name',
                    'address',
                    'crm_id',
                    'created',
                ],
            ],
            [
                'metadata' => self::TEST_SNOWFLAKE_BACKEND_TABLE_METADATA,
                'column_metadata' => self::TEST_COLUMNS_METADATA,
            ],
        ];

        yield 'typed table - same columns' => [
            [
                'isTyped' => true,
                'columns' => [
                    'id',
                    'name',
                    'address',
                    'crm_id',
                ],
            ],
            [
                'metadata' => self::TEST_SNOWFLAKE_BACKEND_TABLE_METADATA,
                'column_metadata' => self::TEST_COLUMNS_METADATA,
            ],
        ];

        yield 'random backend - typed table - extra columns in table' => [
            [
                'isTyped' => true,
                'columns' => [
                    'id',
                    'name',
                    'address',
                    'crm_id',
                    'created',
                ],
            ],
            [
                'metadata' => self::TEST_RANDOM_BACKEND_TABLE_METADATA,
                'column_metadata' => self::TEST_COLUMNS_METADATA,
            ],
        ];

        yield 'random backend - typed table - same columns' => [
            [
                'isTyped' => true,
                'columns' => [
                    'id',
                    'name',
                    'address',
                    'crm_id',
                ],
            ],
            [
                'metadata' => self::TEST_RANDOM_BACKEND_TABLE_METADATA,
                'column_metadata' => self::TEST_COLUMNS_METADATA,
            ],
        ];

        yield 'non-typed table - extra columns in table' => [
            [
                'isTyped' => false,
                'columns' => [
                    'id',
                    'name',
                    'address',
                    'crm_id',
                    'created',
                ],
            ],
            [
                'metadata' => [],
                'column_metadata' => self::TEST_COLUMNS_METADATA,
            ],
        ];

        yield 'non-typed table - same metadata columns' => [
            [
                'isTyped' => false,
                'columns' => [
                    'id',
                    'name',
                    'address',
                    'crm_id',
                ],
            ],
            [
                'metadata' => [],
                'column_metadata' => self::TEST_COLUMNS_METADATA,
            ],
        ];

        yield 'non-typed table - same columns' => [
            [
                'isTyped' => false,
                'columns' => [
                    'id',
                    'name',
                    'address',
                    'crm_id',
                ],
            ],
            [
                'metadata' => [],
                'column_metadata' => self::TEST_COLUMNS_METADATA,
            ],
        ];

        yield 'typed table - case mismatch' => [
            [
                'isTyped' => true,
                'columns' => [
                    'ID',
                    'name',
                    'address',
                    'crm_id',
                    'created',
                ],
            ],
            [
                'metadata' => [],
                'column_metadata' => self::TEST_COLUMNS_METADATA,
            ],
        ];

        yield 'non-typed table - case mismatch' => [
            [
                'isTyped' => false,
                'columns' => [
                    'ID',
                    'name',
                    'address',
                    'crm_id',
                    'created',
                ],
            ],
            [
                'metadata' => [],
                'column_metadata' => self::TEST_COLUMNS_METADATA,
            ],
        ];

        yield 'non-typed table - case mismatch metadata columns' => [
            [
                'isTyped' => false,
                'columns' => [
                    'ID',
                    'NAME',
                    'address',
                    'crm_id',
                ],
            ],
            [
                'metadata' => [],
                'columns' => [
                    'id',
                    'name',
                    'ADDRESS',
                    'CRM_ID',
                ],
                'column_metadata' => self::TEST_COLUMNS_METADATA,
            ],
        ];
    }

    /**
     * @dataProvider doNotAddColumnsProvider
     */
    public function testDoNotAddColumns(array $currentTableInfo, array $newTableConfiguration): void
    {
        $clientMock = $this->createMock(Client::class);
        $clientMock->expects(self::never())
            ->method('addTableColumn');

        TableColumnsHelper::addMissingColumns(
            $clientMock,
            $currentTableInfo,
            $newTableConfiguration,
            'snowflake'
        );
    }

    /**
     * @dataProvider addMissingColumnsToTypedTableProvider
     */
    public function testAddMissingColumnsToTypedTable(
        array $currentTableInfo,
        array $newTableConfiguration,
        array $addTableColumnWithConsecutiveParams
    ): void {
        $clientMock = $this->createMock(Client::class);
        $clientMock
            ->expects(self::exactly(count($addTableColumnWithConsecutiveParams)))
            ->method('addTableColumn')
            ->willReturnCallback(
                function (
                    string $tableId,
                    string $columnName,
                    ?array $definition,
                    ?string $baseType
                ) use (&$addTableColumnWithConsecutiveParams) {
                    list (
                        $xpectedTableId,
                        $expectedColumnName,
                        $expectedDefinition,
                        $expectedDataTypes
                    ) = array_shift($addTableColumnWithConsecutiveParams);

                    self::assertSame(
                        $xpectedTableId,
                        $tableId,
                        'Table id does not match'
                    );
                    self::assertSame(
                        $expectedColumnName,
                        $columnName,
                        'Column name does not match'
                    );
                    self::assertSame(
                        $expectedDefinition,
                        $definition,
                        'Column definition does not match'
                    );
                    self::assertSame(
                        $expectedDataTypes,
                        $baseType,
                        'Column datatype does not match'
                    );
                }
            )
        ;

        TableColumnsHelper::addMissingColumns(
            $clientMock,
            $currentTableInfo,
            $newTableConfiguration,
            'snowflake'
        );
    }

    public function addMissingColumnsToNonTypedTableProvider(): Generator
    {
        yield 'non-typed table - extra metadata columns in config' => [
            [
                'id' => 'in.c-output-mapping.testTable1',
                'isTyped' => false,
                'columns' => [
                    'id',
                    'name',
                ],
            ],
            [
                'metadata' => self::TEST_SNOWFLAKE_BACKEND_TABLE_METADATA,
                'column_metadata' => self::TEST_COLUMNS_METADATA,
            ],
            [
                [
                    'in.c-output-mapping.testTable1',
                    'address',
                    null,
                    null,
                ],
                [
                    'in.c-output-mapping.testTable1',
                    'crm_id',
                    null,
                    null,
                ],
            ],
        ];

        yield 'non-typed table - extra columns in config' => [
            [
                'id' => 'in.c-output-mapping.testTable1',
                'isTyped' => false,
                'columns' => [
                    'id',
                    'name',
                ],
            ],
            [
                'metadata' => self::TEST_SNOWFLAKE_BACKEND_TABLE_METADATA,
                'columns' => array_keys(self::TEST_COLUMNS_METADATA),
            ],
            [
                [
                    'in.c-output-mapping.testTable1',
                    'address',
                    null,
                    null,
                ],
                [
                    'in.c-output-mapping.testTable1',
                    'crm_id',
                    null,
                    null,
                ],
            ],
        ];

        yield 'non-typed - extra metadata columns and columns in config' => [
            [
                'id' => 'in.c-output-mapping.testTable1',
                'isTyped' => false,
                'columns' => [
                    'id',
                    'name',
                ],
            ],
            [
                'metadata' => self::TEST_SNOWFLAKE_BACKEND_TABLE_METADATA,
                'columns' => array_keys(
                    array_slice(
                        self::TEST_COLUMNS_METADATA,
                        0,
                        -1,
                        true
                    )
                ),
                'column_metadata' => array_slice(
                    self::TEST_COLUMNS_METADATA,
                    -1,
                    1,
                    true
                ),
            ],
            [
                [
                    'in.c-output-mapping.testTable1',
                    'crm_id',
                    null,
                    null,
                ],
                [
                    'in.c-output-mapping.testTable1',
                    'address',
                    null,
                    null,
                ],
            ],
        ];

        yield 'non-typed table - same extra metadata columns and columns in config' => [
            [
                'id' => 'in.c-output-mapping.testTable1',
                'isTyped' => false,
                'columns' => [
                    'id',
                    'name',
                ],
            ],
            [
                'metadata' => self::TEST_SNOWFLAKE_BACKEND_TABLE_METADATA,
                'columns' => array_keys(self::TEST_COLUMNS_METADATA),
                'column_metadata' => self::TEST_COLUMNS_METADATA,
            ],
            [
                [
                    'in.c-output-mapping.testTable1',
                    'address',
                    null,
                    null,
                ],
                [
                    'in.c-output-mapping.testTable1',
                    'crm_id',
                    null,
                    null,
                ],
            ],
        ];
    }

    /**
     * @dataProvider addMissingColumnsToNonTypedTableProvider
     */
    public function testAddMissingColumnsToNonTypedTable(
        array $currentTableInfo,
        array $newTableConfiguration,
        array $addTableColumnWithConsecutiveParams
    ): void {
        $clientMock = $this->createMock(Client::class);
        $clientMock
            ->expects(self::exactly(2))
            ->method('addTableColumn')
            ->willReturnCallback(
                function (
                    string $tableId,
                    string $columnName,
                    ?array $definition,
                    ?string $baseType
                ) use (&$addTableColumnWithConsecutiveParams) {
                    list (
                        $xpectedTableId,
                        $expectedColumnName,
                        $expectedDefinition,
                        $expectedDataTypes
                        ) = array_shift($addTableColumnWithConsecutiveParams);

                    self::assertSame(
                        $xpectedTableId,
                        $tableId,
                        'Table id does not match'
                    );
                    self::assertSame(
                        $expectedColumnName,
                        $columnName,
                        'Column name does not match'
                    );
                    self::assertSame(
                        $expectedDefinition,
                        $definition,
                        'Column definition does not match'
                    );
                    self::assertSame(
                        $expectedDataTypes,
                        $baseType,
                        'Column datatype does not match'
                    );
                }
            )
        ;

        TableColumnsHelper::addMissingColumns(
            $clientMock,
            $currentTableInfo,
            $newTableConfiguration,
            'snowflake'
        );
    }
}
