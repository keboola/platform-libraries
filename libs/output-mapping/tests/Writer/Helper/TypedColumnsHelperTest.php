<?php

declare(strict_types=1);

namespace Keboola\OutputMapping\Tests\Writer\Helper;

use Generator;
use Keboola\OutputMapping\Writer\Helper\TypedColumnsHelper;
use Keboola\OutputMapping\Writer\Table\TableDefinition\TableDefinitionColumnFactory;
use Keboola\StorageApi\Client;
use PHPUnit\Framework\TestCase;

class TypedColumnsHelperTest extends TestCase
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

    public function addMissingColumnsDeciderProvider(): Generator
    {
        yield 'non-typed table - extra columns in config' => [
            [
                'isTyped' => false,
                'columns' => [
                    'id',
                    'name',
                ],
            ],
            [
                'metadata' => [],
                'column_metadata' => self::TEST_COLUMNS_METADATA,
            ],
            false,
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
            false,
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
            false,
        ];

        yield 'typed table - extra columns in config' => [
            [
                'isTyped' => true,
                'columns' => [
                    'id',
                    'name',
                ],
            ],
            [
                'metadata' => [],
                'column_metadata' => self::TEST_COLUMNS_METADATA,
            ],
            true,
        ];

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
                'metadata' => [],
                'column_metadata' => self::TEST_COLUMNS_METADATA,
            ],
            false,
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
                'metadata' => [],
                'column_metadata' => self::TEST_COLUMNS_METADATA,
            ],
            false,
        ];
    }

    public function addMissingColumnsProvider(): Generator
    {
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

    /**
     * @dataProvider addMissingColumnsDeciderProvider
     */
    public function testAddMissingColumnsDecider(
        array $currentTableInfo,
        array $newTableConfiguration,
        bool $expectedDecision
    ): void {
        self::assertSame(
            $expectedDecision,
            TypedColumnsHelper::addMissingColumnsDecider($currentTableInfo, $newTableConfiguration)
        );
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
    }

    /**
     * @dataProvider doNotAddColumnsProvider
     */
    public function testDoNotAddColumns(array $currentTableInfo, array $newTableConfiguration): void
    {
        $clientMock = $this->createMock(Client::class);
        $clientMock->expects(self::never())
            ->method('addTableColumn');

        TypedColumnsHelper::addMissingColumns(
            $clientMock,
            $currentTableInfo,
            $newTableConfiguration,
            $this->getBackendFromTableMetadata($newTableConfiguration['metadata'])
        );
    }

    /**
     * @dataProvider addMissingColumnsProvider
     */
    public function testAddMissingColumns(
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

        TypedColumnsHelper::addMissingColumns(
            $clientMock,
            $currentTableInfo,
            $newTableConfiguration,
            $this->getBackendFromTableMetadata($newTableConfiguration['metadata'])
        );
    }

    private function getBackendFromTableMetadata(array $metadata): string
    {
        foreach ($metadata as $metadatum) {
            if ($metadatum['key'] === TableDefinitionColumnFactory::NATIVE_TYPE_METADATA_KEY) {
                return $metadatum['value'];
            }
        }

        self::fail('Test metadata does not contains backend info.');
    }
}
