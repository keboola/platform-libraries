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

    public function testUniversalFailure(): void
    {
        $clientMock = $this->createMock(Client::class);
        $clientMock->expects(self::never())
            ->method('addTableColumn');

        TableColumnsHelper::addMissingColumns(
            $clientMock,
            array(
                'uri' => 'https://connection.csas.keboola.cloud/v2/storage/tables/out.c-global_control_group.dami_semaphore',
                'id' => 'out.c-global_control_group.dami_semaphore',
                'name' => 'dami_semaphore',
                'displayName' => 'dami_semaphore',
                'transactional' => false,
                'primaryKey' => array('status'),
                'indexType' => 'CLUSTERED COLUMNSTORE INDEX',
                'indexKey' => array(),
                'distributionType' => 'ROUND_ROBIN',
                'distributionKey' => array('status'),
                'syntheticPrimaryKeyEnabled' => false,
                'created' => '2022-02-07T17:20:00+0100',
                'lastImportDate' => '2023-04-20T15:06:57+0200',
                'lastChangeDate' => '2023-04-20T15:06:57+0200',
                'rowsCount' => 1,
                'dataSizeBytes' => 991232,
                'isAlias' => false,
                'isAliasable' => true,
                'isTyped' => false,
                'columns' => array('status'),
                'columnMetadata' => array(
                    'status' => array(
                        array(
                            'id' => '54035897',
                            'key' => 'KBC.datatype.type',
                            'value' => 'INT',
                            'provider' => 'storage',
                            'timestamp' => '2022-02-07T17:20:02+0100'
                        ),
                        array(
                            'id' => '54035898',
                            'key' => 'KBC.datatype.nullable',
                            'value' => '',
                            'provider' => 'storage',
                            'timestamp' => '2022-02-07T17:20:02+0100'
                        ),
                        array(
                            'id' => '54035899',
                            'key' => 'KBC.datatype.basetype',
                            'value' => 'INTEGER',
                            'provider' => 'storage',
                            'timestamp' => '2022-02-07T17:20:02+0100'
                        )
                    )
                ),
                'attributes' => array(),
                'metadata' => array(
                    array(
                        'id' => '54035896',
                        'key' => 'KBC.dataTypesEnabled',
                        'value' => 'true',
                        'provider' => 'storage',
                        'timestamp' => '2022-02-07T17:20:02+0100'
                    ),
                    array(
                        'id' => '54035900',
                        'key' => 'KBC.lastUpdatedBy.component.id',
                        'value' => 'keboola.ex-db-oracle',
                        'provider' => 'system',
                        'timestamp' => '2022-02-07T17:23:54+0100'
                    ),
                    array(
                        'id' => '54035901',
                        'key' => 'KBC.lastUpdatedBy.configuration.id',
                        'value' => '6917568',
                        'provider' => 'system',
                        'timestamp' => '2022-02-07T17:23:54+0100'
                    ),
                    array(
                        'id' => '54035902',
                        'key' => 'KBC.lastUpdatedBy.configurationRow.id',
                        'value' => '15982',
                        'provider' => 'system',
                        'timestamp' => '2022-02-07T17:23:54+0100'
                    )
                ),
                'bucket' => array(
                    'uri' => 'https://connection.csas.keboola.cloud/v2/storage/buckets/out.c-global_control_group',
                    'id' => 'out.c-global_control_group',
                    'name' => 'c-global_control_group',
                    'displayName' => 'global_control_group',
                    'stage' => 'out',
                    'description' => '',
                    'tables' => 'https://connection.csas.keboola.cloud/v2/storage/buckets/out.c-global_control_group',
                    'created' => '2022-02-04T16:39:39+0100',
                    'lastChangeDate' => '2023-04-22T15:06:27+0200',
                    'isReadOnly' => false,
                    'dataSizeBytes' => 846168064,
                    'rowsCount' => 12968533,
                    'isMaintenance' => false,
                    'backend' => 'synapse',
                    'sharing' => null,
                    'hasExternalSchema' => false,
                    'databaseName' => '',
                    'metadata' => array()
                ),
                'definition' => array(
                    'primaryKeysNames' => array('status'),
                    'columns' => array(
                        array(
                            'name' => 'status',
                            'definition' => array(
                                'type' => 'INT',
                                'nullable' => false
                            ),
                            'basetype' => 'INTEGER')
                    ),
                    'distribution' => array(
                        'type' => 'ROUND_ROBIN',
                        'distributionColumnsNames' => array('status')
                    ),
                    'index' => array(
                        'type' => 'CLUSTERED COLUMNSTORE INDEX',
                        'indexColumnsNames' => array()
                    )
                )
            ),
            array(
                'destination' => 'out.c-global_control_group.dami_semaphore',
                'incremental' => false,
                'primary_key' => array('status'),
                'columns' => array('STATUS'),
                'distribution_key' => array(),
                'delete_where_values' => array(),
                'delete_where_operator' => 'eq',
                'delimiter' => ',',
                'enclosure' => '"',
                'metadata' => array(),
                'column_metadata' => array(),
                'write_always' => false,
                'tags' => array('componentId: keboola.ex-db-oracle', 'configurationId: 6917568', 'configurationRowId: 15982')),
            'synapse'
        );
    }
}
