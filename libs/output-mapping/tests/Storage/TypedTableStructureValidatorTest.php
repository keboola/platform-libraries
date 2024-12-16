<?php

declare(strict_types=1);

namespace Keboola\OutputMapping\Tests\Storage;

use Generator;
use Keboola\Datatype\Definition\BaseType;
use Keboola\Datatype\Definition\Snowflake;
use Keboola\OutputMapping\Exception\InvalidOutputException;
use Keboola\OutputMapping\Exception\InvalidTableStructureException;
use Keboola\OutputMapping\Mapping\MappingFromConfigurationSchemaColumn;
use Keboola\OutputMapping\Storage\TypedTableStructureValidator;
use Keboola\StorageApi\Client;
use PHPUnit\Framework\TestCase;
use Psr\Log\NullLogger;

class TypedTableStructureValidatorTest extends TestCase
{
    public function testValidateStructure(): void
    {
        $schema = [
            new MappingFromConfigurationSchemaColumn([
                'name' => 'col1',
                'data_type' => [
                    'base' => [
                        'type' => 'STRING',
                        'length' => '255',
                    ],
                    'snowflake' => [
                        'type' => 'VARCHAR',
                        'length' => '255',
                    ],
                ],
                'nullable' => false,
                'primary_key' => true,
            ]),
            new MappingFromConfigurationSchemaColumn([
                'name' => 'col2',
                'data_type' => [
                    'base' => [
                        'type' => 'NUMERIC',
                    ],
                ],
                'nullable' => false,
            ]),
            new MappingFromConfigurationSchemaColumn([
                'name' => 'col3',
                'data_type' => [
                    'base' => [
                        'type' => 'NUMERIC',
                        'length' => '123',
                    ],
                    'snowflake' => [
                        'type' => 'INT',
                        'length' => '123',
                    ],
                ],
                'nullable' => true,
            ]),
        ];

        $validator = new TypedTableStructureValidator(
            new NullLogger(),
            $this->getClientMock()->getTable('in.c-main.table'),
        );
        $validator->validate($schema);

        self::assertTrue(true);
    }

    public function testMissingColumnInStorage(): void
    {
        $newColumn = new MappingFromConfigurationSchemaColumn([
            'name' => 'col4',
            'data_type' => [
                'base' => [
                    'type' => 'NUMERIC',
                ],
            ],
        ]);
        $schema = [
            new MappingFromConfigurationSchemaColumn([
                'name' => 'col1',
                'data_type' => [
                    'base' => [
                        'type' => 'STRING',
                        'length' => '255',
                    ],
                ],
                'primary_key' => true,
                'nullable' => false,
            ]),
            new MappingFromConfigurationSchemaColumn([
                'name' => 'col2',
                'data_type' => [
                    'base' => [
                        'type' => 'NUMERIC',
                    ],
                ],
                'nullable' => false,
            ]),
            new MappingFromConfigurationSchemaColumn([
                'name' => 'col3',
                'data_type' => [
                    'base' => [
                        'type' => 'NUMERIC',
                    ],
                ],
            ]),
            $newColumn,
        ];

        $validator = new TypedTableStructureValidator(
            new NullLogger(),
            $this->getClientMock()->getTable('in.c-main.table'),
        );
        $tableChangesStore = $validator->validate($schema);

        self::assertTrue($tableChangesStore->hasMissingColumns());
        self::assertEquals([$newColumn], $tableChangesStore->getMissingColumns());
    }

    public function testHasDifferentColumnAttribute(): void
    {
        $col1 = new MappingFromConfigurationSchemaColumn([
            'name' => 'col1',
            'data_type' => [
                'base' => [
                    'type' => 'STRING',
                    'length' => '230', // different length
                    'default' => 'new default value', // different default value
                ],
            ],
            'primary_key' => true,
            'nullable' => false,
        ]);
        $col2 = new MappingFromConfigurationSchemaColumn([
            'name' => 'col2',
            'data_type' => [
                'base' => [
                    'type' => 'NUMERIC',
                ],
            ],
            'nullable' => true, // different nullable
        ]);
        $schema = [
            $col1,
            $col2,
            new MappingFromConfigurationSchemaColumn([
                'name' => 'col3',
                'data_type' => [
                    'base' => [
                        'type' => 'NUMERIC',
                    ],
                ],
            ]),
        ];

        $validator = new TypedTableStructureValidator(
            new NullLogger(),
            $this->getClientMock()->getTable('in.c-main.table'),
        );
        $tableChangesStore = $validator->validate($schema);

        self::assertCount(2, $tableChangesStore->getDifferentColumnAttributes());
        self::assertEquals($col1, $tableChangesStore->getDifferentColumnAttributes()[0]);
        self::assertEquals($col2, $tableChangesStore->getDifferentColumnAttributes()[1]);
    }

    public function testErrorWrongCountColumns(): void
    {
        $schema = [
            new MappingFromConfigurationSchemaColumn([
                'name' => 'col1',
                'data_type' => [
                    'base' => [
                        'type' => 'STRING',
                        'length' => '255',
                    ],
                    'snowflake' => [
                        'type' => 'VARCHAR',
                        'length' => '255',
                    ],
                ],
            ]),
            new MappingFromConfigurationSchemaColumn([
                'name' => 'col2',
                'data_type' => [
                    'base' => [
                        'type' => 'NUMERIC',
                    ],
                ],
            ]),
        ];

        $validator = new TypedTableStructureValidator(
            new NullLogger(),
            $this->getClientMock()->getTable('in.c-main.table'),
        );
        $this->expectException(InvalidTableStructureException::class);
        $this->expectExceptionMessage(
            'Table "in.c-main.table" does not contain the same number of columns as the schema.'.
            ' Table columns: 3, schema columns: 2.',
        );
        $validator->validate($schema);
    }

    public function testErrorWrongSchemaColumnName(): void
    {
        $schema = [
            new MappingFromConfigurationSchemaColumn([
                'name' => 'col1',
                'data_type' => [
                    'base' => [
                        'type' => 'STRING',
                        'length' => '255',
                    ],
                    'snowflake' => [
                        'type' => 'VARCHAR',
                        'length' => '255',
                    ],
                ],
            ]),
            new MappingFromConfigurationSchemaColumn([
                'name' => 'col2',
                'data_type' => [
                    'base' => [
                        'type' => 'NUMERIC',
                    ],
                ],
            ]),
            new MappingFromConfigurationSchemaColumn([
                'name' => 'col4',
                'data_type' => [
                    'base' => [
                        'type' => 'NUMERIC',
                    ],
                ],
            ]),
        ];

        $validator = new TypedTableStructureValidator(
            new NullLogger(),
            $this->getClientMock()->getTable('in.c-main.table'),
        );
        $this->expectException(InvalidTableStructureException::class);
        $this->expectExceptionMessage('Table "in.c-main.table" does not contain columns: "col4".');
        $validator->validate($schema);
    }

    public function testErrorWrongColumnDataTypeBackendType(): void
    {
        $schema = [
            new MappingFromConfigurationSchemaColumn([
                'name' => 'col1',
                'data_type' => [
                    'base' => [
                        'type' => 'STRING',
                        'length' => '255',
                    ],
                    'snowflake' => [
                        'type' => 'VARCHAR',
                        'length' => '255',
                    ],
                ],
                'primary_key' => true,
            ]),
            new MappingFromConfigurationSchemaColumn([
                'name' => 'col2',
                'data_type' => [
                    'base' => [
                        'type' => 'NUMERIC',
                    ],
                ],
            ]),
            new MappingFromConfigurationSchemaColumn([
                'name' => 'col3',
                'data_type' => [
                    'base' => [
                        'type' => 'NUMERIC',
                        'length' => '123',
                    ],
                    'snowflake' => [
                        'type' => 'VARCHAR',
                        'length' => '123',
                    ],
                ],
            ]),
        ];

        $validator = new TypedTableStructureValidator(
            new NullLogger(),
            $this->getClientMock()->getTable('in.c-main.table'),
        );
        $this->expectException(InvalidTableStructureException::class);
        $this->expectExceptionMessage(
            'Table "in.c-main.table" column "col3" has different type than the schema. '.
            'Table type: "NUMBER", schema type: "VARCHAR".',
        );
        $validator->validate($schema);
    }

    public function testErrorWrongColumnDataTypeBaseType(): void
    {
        $schema = [
            new MappingFromConfigurationSchemaColumn([
                'name' => 'col1',
                'data_type' => [
                    'base' => [
                        'type' => 'STRING',
                        'length' => '255',
                    ],
                    'snowflake' => [
                        'type' => 'VARCHAR',
                        'length' => '255',
                    ],
                ],
                'primary_key' => true,
            ]),
            new MappingFromConfigurationSchemaColumn([
                'name' => 'col2',
                'data_type' => [
                    'base' => [
                        'type' => 'STRING',
                    ],
                ],
            ]),
            new MappingFromConfigurationSchemaColumn([
                'name' => 'col3',
                'data_type' => [
                    'base' => [
                        'type' => 'NUMERIC',
                        'length' => '123',
                    ],
                    'snowflake' => [
                        'type' => 'INT',
                        'length' => '123',
                    ],
                ],
            ]),
        ];

        $validator = new TypedTableStructureValidator(
            new NullLogger(),
            $this->getClientMock()->getTable('in.c-main.table'),
        );
        $this->expectException(InvalidTableStructureException::class);
        $this->expectExceptionMessage(
            'Table "in.c-main.table" column "col2" has different type than the schema. '.
            'Table type: "NUMERIC", schema type: "STRING".',
        );
        $validator->validate($schema);
    }

    public function testWrongColumnDataTypeBackendLength(): void
    {
        $col3 = new MappingFromConfigurationSchemaColumn([
            'name' => 'col3',
            'data_type' => [
                'base' => [
                    'type' => 'NUMERIC',
                    'length' => '123',
                ],
                'snowflake' => [
                    'type' => 'INT',
                    'length' => '543',
                ],
            ],
        ]);
        $schema = [
            new MappingFromConfigurationSchemaColumn([
                'name' => 'col1',
                'data_type' => [
                    'base' => [
                        'type' => 'STRING',
                        'length' => '255',
                    ],
                    'snowflake' => [
                        'type' => 'VARCHAR',
                        'length' => '255',
                    ],
                ],
                'nullable' => false,
                'primary_key' => true,
            ]),
            new MappingFromConfigurationSchemaColumn([
                'name' => 'col2',
                'data_type' => [
                    'base' => [
                        'type' => 'NUMERIC',
                    ],
                ],
                'nullable' => false,
            ]),
            $col3,
        ];

        $validator = new TypedTableStructureValidator(
            new NullLogger(),
            $this->getClientMock()->getTable('in.c-main.table'),
        );
        $tableChangesStore = $validator->validate($schema);

        self::assertEquals(
            [$col3],
            $tableChangesStore->getDifferentColumnAttributes(),
        );
    }

    public function testWrongColumnDataTypeBaseLength(): void
    {
        $col3 = new MappingFromConfigurationSchemaColumn([
            'name' => 'col3',
            'data_type' => [
                'base' => [
                    'type' => 'NUMERIC',
                    'length' => '987',
                ],
            ],
        ]);
        $schema = [
            new MappingFromConfigurationSchemaColumn([
                'name' => 'col1',
                'data_type' => [
                    'base' => [
                        'type' => 'STRING',
                        'length' => '255',
                    ],
                    'snowflake' => [
                        'type' => 'VARCHAR',
                        'length' => '255',
                    ],
                ],
                'nullable' => false,
                'primary_key' => true,
            ]),
            new MappingFromConfigurationSchemaColumn([
                'name' => 'col2',
                'data_type' => [
                    'base' => [
                        'type' => 'NUMERIC',
                    ],
                ],
                'nullable' => false,
            ]),
            $col3,
        ];

        $validator = new TypedTableStructureValidator(
            new NullLogger(),
            $this->getClientMock()->getTable('in.c-main.table'),
        );
        $tableChangesStore = $validator->validate($schema);

        self::assertEquals(
            [$col3],
            $tableChangesStore->getDifferentColumnAttributes(),
        );
    }

    public function testErrorWrongColumnMultipleErrors(): void
    {
        $schema = [
            new MappingFromConfigurationSchemaColumn([
                'name' => 'col1',
                'data_type' => [
                    'base' => [
                        'type' => 'NUMERIC',
                        'length' => '254',
                    ],
                ],
                'primary_key' => true,
                'nullable' => false,
            ]),
            new MappingFromConfigurationSchemaColumn([
                'name' => 'col2',
                'data_type' => [
                    'base' => [
                        'type' => 'STRING',
                    ],
                ],
                'nullable' => false,
            ]),
            new MappingFromConfigurationSchemaColumn([
                'name' => 'col3',
                'data_type' => [
                    'base' => [
                        'type' => 'NUMERIC',
                        'length' => '123',
                    ],
                ],
                'nullable' => false,
            ]),
        ];

        $validator = new TypedTableStructureValidator(
            new NullLogger(),
            $this->getClientMock()->getTable('in.c-main.table'),
        );
        $this->expectException(InvalidTableStructureException::class);
        $expectedMessage = 'Table "in.c-main.table" column "col1" has different type than the schema. ';
        $expectedMessage .= 'Table type: "STRING", schema type: "NUMERIC". ';
        $expectedMessage .= 'Table "in.c-main.table" column "col2" has different type than the schema. ';
        $expectedMessage .= 'Table type: "NUMERIC", schema type: "STRING".';
        $this->expectExceptionMessage($expectedMessage);
        $validator->validate($schema);
    }

    public function testValidateColumnAliases(): void
    {
        $clientMock = $this->createMock(Client::class);
        $clientMock
            ->expects(self::once())
            ->method('getTable')->willReturn([
                'id' => 'in.c-main.table',
                'isTyped' => true,
                'definition' => [
                    'primaryKeysNames' => [],
                    'columns' => [
                        [
                            'name' => 'nvarchar2',
                            'definition' => [
                                'type' => Snowflake::TYPE_VARCHAR,
                                'nullable' => false,
                            ],
                            'basetype' => BaseType::STRING,
                        ],
                        [
                            'name' => 'integer',
                            'definition' => [
                                'type' => Snowflake::TYPE_NUMBER,
                                'nullable' => false,
                            ],
                            'basetype' => BaseType::INTEGER,
                        ],
                        [
                            'name' => 'double',
                            'definition' => [
                                'type' => Snowflake::TYPE_FLOAT,
                                'nullable' => true,
                            ],
                            'basetype' => BaseType::FLOAT,
                        ],
                    ],
                ],
                'bucket' => [
                    'backend' => 'snowflake',
                ],
            ]);

        $schema = [
            new MappingFromConfigurationSchemaColumn([
                'name' => 'nvarchar2',
                'data_type' => [
                    'base' => [
                        'type' => BaseType::STRING,
                    ],
                    'snowflake' => [
                        'type' => Snowflake::TYPE_NVARCHAR2,
                    ],
                ],
                'nullable' => false,
            ]),
            new MappingFromConfigurationSchemaColumn([
                'name' => 'integer',
                'data_type' => [
                    'base' => [
                        'type' => BaseType::INTEGER,
                    ],
                    'snowflake' => [
                        'type' => Snowflake::TYPE_INTEGER,
                    ],
                ],
                'nullable' => false,
            ]),
            new MappingFromConfigurationSchemaColumn([
                'name' => 'double',
                'data_type' => [
                    'base' => [
                        'type' => BaseType::FLOAT,
                    ],
                    'snowflake' => [
                        'type' => Snowflake::TYPE_DOUBLE,
                    ],
                ],
                'nullable' => true,
            ]),
        ];

        $validator = new TypedTableStructureValidator(
            new NullLogger(),
            $clientMock->getTable('in.c-main.table'),
        );
        $validator->validate($schema);

        self::assertTrue(true);
    }

    public function testValidateColumnTimestampAliases(): void
    {
        $clientMock = $this->createMock(Client::class);
        $clientMock
            ->expects(self::once())
            ->method('getTable')->willReturn([
                'id' => 'in.c-main.table',
                'isTyped' => true,
                'definition' => [
                    'primaryKeysNames' => [],
                    'columns' => [
                        [
                            'name' => 'timestamp',
                            'definition' => [
                                'type' => Snowflake::TYPE_TIMESTAMP,
                                'nullable' => false,
                                'length' => '255',
                            ],
                            'basetype' => BaseType::TIMESTAMP,
                        ],
                        [
                            'name' => 'timestamp_tz',
                            'definition' => [
                                'type' => Snowflake::TYPE_TIMESTAMP_TZ,
                                'nullable' => false,
                            ],
                            'basetype' => BaseType::TIMESTAMP,
                        ],
                        [
                            'name' => 'timestamp_ntz',
                            'definition' => [
                                'type' => Snowflake::TYPE_TIMESTAMP_NTZ,
                                'nullable' => true,
                            ],
                            'basetype' => BaseType::TIMESTAMP,
                        ],
                        [
                            'name' => 'timestamp_ltz',
                            'definition' => [
                                'type' => Snowflake::TYPE_TIMESTAMP_LTZ,
                                'nullable' => true,
                            ],
                            'basetype' => BaseType::TIMESTAMP,
                        ],
                    ],
                ],
                'bucket' => [
                    'backend' => 'snowflake',
                ],
            ]);

        $schema = [
            new MappingFromConfigurationSchemaColumn([
                'name' => 'timestamp',
                'data_type' => [
                    'base' => [
                        'type' => Snowflake::TYPE_TIMESTAMP_TZ, // TIMESTAMP in storage
                    ],
                ],
                'nullable' => false,
            ]),
            new MappingFromConfigurationSchemaColumn([
                'name' => 'timestamp_tz',
                'data_type' => [
                    'base' => [
                        'type' => Snowflake::TYPE_TIMESTAMP_NTZ, // TIMESTAMP_TZ in storage
                    ],
                ],
                'nullable' => false,
            ]),
            new MappingFromConfigurationSchemaColumn([
                'name' => 'timestamp_ntz',
                'data_type' => [
                    'base' => [
                        'type' => Snowflake::TYPE_TIMESTAMP_LTZ, // TIMESTAMP_NTZ in storage
                    ],
                ],
                'nullable' => true,
            ]),
            new MappingFromConfigurationSchemaColumn([
                'name' => 'timestamp_ltz',
                'data_type' => [
                    'base' => [
                        'type' => Snowflake::TYPE_TIMESTAMP, // TIMESTAMP_LTZ in storage
                    ],
                ],
                'nullable' => true,
            ]),
        ];

        $validator = new TypedTableStructureValidator(
            new NullLogger(),
            $clientMock->getTable('in.c-main.table'),
        );
        $validator->validate($schema);

        self::assertTrue(true);
    }

    public function testValidateIfTableIsTypes(): void
    {
        $clientMock = $this->createMock(Client::class);
        $clientMock
            ->expects(self::once())
            ->method('getTable')->willReturn([
                'id' => 'in.c-main.table',
                'isTyped' => false,
            ]);

        $schema = [
            new MappingFromConfigurationSchemaColumn([
                'name' => 'nvarchar2',
                'data_type' => [
                    'base' => [
                        'type' => BaseType::STRING,
                    ],
                    'snowflake' => [
                        'type' => Snowflake::TYPE_NVARCHAR2,
                    ],
                ],
                'nullable' => false,
            ]),
        ];

        $validator = new TypedTableStructureValidator(
            new NullLogger(),
            $clientMock->getTable('in.c-main.table'),
        );

        $this->expectException(InvalidOutputException::class);
        $this->expectExceptionMessage('Table is not typed.');
        $validator->validate($schema);

        self::assertTrue(true);
    }

    public function typedTableWithPkDataProvider(): Generator
    {
        $primaryKeyColumn1 = new MappingFromConfigurationSchemaColumn([
            'name' => 'col1',
            'data_type' => [
                'base' => [
                    'type' => 'STRING',
                ],
                'snowflake' => [
                    'type' => 'VARCHAR',
                ],
            ],
            'nullable' => false,
            'primary_key' => true,
        ]);

        $primaryKeyColumn2 = new MappingFromConfigurationSchemaColumn([
            'name' => 'col2',
            'data_type' => [
                'base' => [
                    'type' => 'NUMERIC',
                ],
            ],
            'nullable' => false,
            'primary_key' => true,
        ]);

        yield 'primary key change' => [
            'schemaColumns' => [
                $primaryKeyColumn1,
                $primaryKeyColumn2,
                new MappingFromConfigurationSchemaColumn([
                    'name' => 'col3',
                    'data_type' => [
                        'base' => [
                            'type' => 'NUMERIC',
                        ],
                        'snowflake' => [
                            'type' => 'INT',
                        ],
                    ],
                    'nullable' => true,
                ]),
            ],
            'expectedPrimaryKeyColumns' => [$primaryKeyColumn1, $primaryKeyColumn2],
        ];

        yield 'same primary key as table' => [
            'schemaColumns' => [
                $primaryKeyColumn1,
                new MappingFromConfigurationSchemaColumn([
                    'name' => 'col2',
                    'data_type' => [
                        'base' => [
                            'type' => 'NUMERIC',
                        ],
                    ],
                    'nullable' => false,
                ]),
                new MappingFromConfigurationSchemaColumn([
                    'name' => 'col3',
                    'data_type' => [
                        'base' => [
                            'type' => 'NUMERIC',
                        ],
                        'snowflake' => [
                            'type' => 'INT',
                        ],
                    ],
                    'nullable' => true,
                ]),
            ],
            'expectedPrimaryKeyColumns' => null,
        ];

        yield 'primary key reset' => [
            'schemaColumns' => [
                new MappingFromConfigurationSchemaColumn([
                    'name' => 'col1',
                    'data_type' => [
                        'base' => [
                            'type' => 'STRING',
                        ],
                        'snowflake' => [
                            'type' => 'VARCHAR',
                        ],
                    ],
                    'nullable' => false,
                ]),
                new MappingFromConfigurationSchemaColumn([
                    'name' => 'col2',
                    'data_type' => [
                        'base' => [
                            'type' => 'NUMERIC',
                        ],
                    ],
                    'nullable' => false,
                ]),
                new MappingFromConfigurationSchemaColumn([
                    'name' => 'col3',
                    'data_type' => [
                        'base' => [
                            'type' => 'NUMERIC',
                        ],
                        'snowflake' => [
                            'type' => 'INT',
                        ],
                    ],
                    'nullable' => true,
                ]),
            ],
            'expectedPrimaryKeyColumns' => [],
        ];
    }

    /**
     * @dataProvider typedTableWithPkDataProvider
     */
    public function testWithPk(array $schemaColumns, ?array $expectedPrimaryKeyColumns): void
    {
        $validator = new TypedTableStructureValidator(
            new NullLogger(),
            $this->getClientMock()->getTable('in.c-main.table'),
        );
        $tableChangesStore = $validator->validate($schemaColumns);

        if ($expectedPrimaryKeyColumns === null) {
            self::assertNull($tableChangesStore->getPrimaryKey());
        } else {
            self::assertNotNull($tableChangesStore->getPrimaryKey());
            self::assertSame(
                $expectedPrimaryKeyColumns,
                $tableChangesStore->getPrimaryKey()->getColumns(),
            );
        }
    }

    private function getClientMock(): Client
    {
        $clientMock = $this->createMock(Client::class);
        $clientMock
            ->expects(self::once())
            ->method('getTable')->willReturn([
                'id' => 'in.c-main.table',
                'isTyped' => true,
                'definition' => [
                    'primaryKeysNames' => [
                        'col1',
                    ],
                    'columns' => [
                        [
                            'name' => 'col1',
                            'definition' => [
                                'type' => Snowflake::TYPE_VARCHAR,
                                'nullable' => false,
                                'length' => '255',
                            ],
                            'basetype' => BaseType::STRING,
                        ],
                        [
                            'name' => 'col2',
                            'definition' => [
                                'type' => Snowflake::TYPE_NUMBER,
                                'nullable' => false,
                                'length' => '20',
                            ],
                            'basetype' => BaseType::NUMERIC,
                        ],
                        [
                            'name' => 'col3',
                            'definition' => [
                                'type' => Snowflake::TYPE_NUMBER,
                                'nullable' => true,
                                'length' => '123',
                            ],
                            'basetype' => BaseType::NUMERIC,
                        ],
                    ],
                ],
                'bucket' => [
                    'backend' => 'snowflake',
                ],
            ]);

        return $clientMock;
    }
}
