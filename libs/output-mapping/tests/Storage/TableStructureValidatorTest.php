<?php

declare(strict_types=1);

namespace Keboola\OutputMapping\Tests\Storage;

use Keboola\OutputMapping\Exception\InvalidOutputException;
use Keboola\OutputMapping\Exception\InvalidTableStructureException;
use Keboola\OutputMapping\Mapping\MappingFromConfigurationSchemaColumn;
use Keboola\OutputMapping\Storage\TableStructureValidator;
use Keboola\OutputMapping\Tests\AbstractTestCase;
use Keboola\StorageApi\Client;
use Keboola\StorageApi\ClientException;
use Psr\Log\NullLogger;
use Psr\Log\Test\TestLogger;

class TableStructureValidatorTest extends AbstractTestCase
{
    public function testValidateTypedTableStructure(): void
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

        $validator = new TableStructureValidator(true, new NullLogger(), $this->getClientMockTypedTable());
        $validator->validateTable('in.c-main.table', $schema);

        self::assertTrue(true);
    }

    public function testSkipValidationWithoutFeature(): void
    {
        $clientMock = $this->createMock(Client::class);

        $schema = [];
        $validator = new TableStructureValidator(false, new NullLogger(), $clientMock);
        $validator->validateTable('in.c-main.table', $schema);

        self::assertTrue(true);
    }

    public function testSkipValidationWithoutConfigSchema(): void
    {
        $clientMock = $this->createMock(Client::class);
        $schema = [];

        $validator = new TableStructureValidator(false, new NullLogger(), $clientMock);
        $validator->validateTable('in.c-main.table', $schema);

        self::assertTrue(true);
    }

    public function testSkipValidationIfTableNotExists(): void
    {
        $clientMock = $this->createMock(Client::class);
        $clientMock
            ->method('getTable')
            ->willThrowException(new ClientException('Table not found', 404));

        $schema = [];

        $validator = new TableStructureValidator(true, new NullLogger(), $clientMock);
        $validator->validateTable('in.c-main.table', $schema);

        self::assertTrue(true);
    }

    public function testErrorCatchStorageApiException(): void
    {
        $clientMock = $this->createMock(Client::class);
        $clientMock
            ->method('getTable')
            ->willThrowException(new ClientException('Bad request', 400));

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
        ];
        $validator = new TableStructureValidator(true, new NullLogger(), $clientMock);

        $this->expectException(InvalidOutputException::class);
        $this->expectExceptionMessage('Bad request');
        $validator->validateTable('in.c-main.table', $schema);
    }

    public function testTypedTableMissingColumnInStorage(): void
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

        $validator = new TableStructureValidator(true, new NullLogger(), $this->getClientMockTypedTable());
        $tableChangesStore = $validator->validateTable('in.c-main.table', $schema);

        self::assertTrue($tableChangesStore->hasMissingColumns());
        self::assertEquals([$newColumn], $tableChangesStore->getMissingColumns());
    }

    public function testErrorTypedTableWrongCountColumns(): void
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

        $validator = new TableStructureValidator(true, new NullLogger(), $this->getClientMockTypedTable());
        $this->expectException(InvalidTableStructureException::class);
        $this->expectExceptionMessage(
            'Table "in.c-main.table" does not contain the same number of columns as the schema.'.
            ' Table columns: 3, schema columns: 2.',
        );
        $validator->validateTable('in.c-main.table', $schema);
    }

    public function testErrorTypedTableWrongSchemaColumnName(): void
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

        $validator = new TableStructureValidator(true, new NullLogger(), $this->getClientMockTypedTable());
        $this->expectException(InvalidTableStructureException::class);
        $this->expectExceptionMessage('Table "in.c-main.table" does not contain columns: "col4".');
        $validator->validateTable('in.c-main.table', $schema);
    }

    public function testErrorTypedTableWrongColumnDataTypeBackendType(): void
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
                        'type' => 'BIGINT',
                        'length' => '123',
                    ],
                ],
            ]),
        ];

        $validator = new TableStructureValidator(true, new NullLogger(), $this->getClientMockTypedTable());
        $this->expectException(InvalidTableStructureException::class);
        $this->expectExceptionMessage(
            'Table "in.c-main.table" column "col3" has different type than the schema. '.
            'Table type: "INT", schema type: "BIGINT".',
        );
        $validator->validateTable('in.c-main.table', $schema);
    }

    public function testErrorTypedTableWrongColumnDataTypeBaseType(): void
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

        $validator = new TableStructureValidator(true, new NullLogger(), $this->getClientMockTypedTable());
        $this->expectException(InvalidTableStructureException::class);
        $this->expectExceptionMessage(
            'Table "in.c-main.table" column "col2" has different type than the schema. '.
            'Table type: "NUMERIC", schema type: "STRING".',
        );
        $validator->validateTable('in.c-main.table', $schema);
    }

    public function testErrorTypedTableWrongColumnDataTypeBackendLength(): void
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
                        'type' => 'INT',
                        'length' => '543',
                    ],
                ],
            ]),
        ];

        $validator = new TableStructureValidator(true, new NullLogger(), $this->getClientMockTypedTable());
        $this->expectException(InvalidTableStructureException::class);
        $this->expectExceptionMessage(
            'Table "in.c-main.table" column "col3" has different length than the schema. '.
            'Table length: "123", schema length: "543".',
        );
        $validator->validateTable('in.c-main.table', $schema);
    }

    public function testErrorTypedTableWrongColumnDataTypeBaseLength(): void
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
                        'length' => '987',
                    ],
                ],
            ]),
        ];

        $validator = new TableStructureValidator(true, new NullLogger(), $this->getClientMockTypedTable());
        $this->expectException(InvalidTableStructureException::class);
        $this->expectExceptionMessage(
            'Table "in.c-main.table" column "col3" has different length than the schema. '.
            'Table length: "123", schema length: "987".',
        );
        $validator->validateTable('in.c-main.table', $schema);
    }

    public function testErrorTypedTableWrongColumnNullable(): void
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
                        'length' => '123',
                    ],
                ],
                'nullable' => false,
            ]),
        ];

        $validator = new TableStructureValidator(true, new NullLogger(), $this->getClientMockTypedTable());
        $this->expectException(InvalidTableStructureException::class);
        $this->expectExceptionMessage(
            'Table "in.c-main.table" column "col3" has different nullable than the schema. '.
            'Table nullable: "true", schema nullable: "false".',
        );
        $validator->validateTable('in.c-main.table', $schema);
    }

    public function testErrorTypedTableWrongColumnMultipleErrors(): void
    {
        $schema = [
            new MappingFromConfigurationSchemaColumn([
                'name' => 'col1',
                'data_type' => [
                    'base' => [
                        'type' => 'STRING',
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

        $validator = new TableStructureValidator(true, new NullLogger(), $this->getClientMockTypedTable());
        $this->expectException(InvalidTableStructureException::class);
        $expectedMessage = 'Table "in.c-main.table" column "col1" has different length than the schema. ';
        $expectedMessage .= 'Table length: "255", schema length: "254". ';
        $expectedMessage .= 'Table "in.c-main.table" column "col2" has different type than the schema. ';
        $expectedMessage .= 'Table type: "NUMERIC", schema type: "STRING". ';
        $expectedMessage .= 'Table "in.c-main.table" column "col3" has different nullable than the schema. ';
        $expectedMessage .= 'Table nullable: "true", schema nullable: "false".';
        $this->expectExceptionMessage($expectedMessage);
        $validator->validateTable('in.c-main.table', $schema);
    }

    public function testErrorTypedTableCountPrimaryKeys(): void
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
                'primary_key' => true,
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

        $validator = new TableStructureValidator(true, new NullLogger(), $this->getClientMockTypedTable());

        $this->expectException(InvalidTableStructureException::class);
        $this->expectExceptionMessage(
            'Table primary keys does not contain the same number of columns as the schema. '.
            'Table primary keys: "col1", schema primary keys: "col1, col2".',
        );
        $validator->validateTable('in.c-main.table', $schema);
    }

    public function testErrorTypedTableWrongPrimaryKey(): void
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
            ]),
            new MappingFromConfigurationSchemaColumn([
                'name' => 'col2',
                'data_type' => [
                    'base' => [
                        'type' => 'NUMERIC',
                    ],
                ],
                'nullable' => false,
                'primary_key' => true,
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

        $validator = new TableStructureValidator(true, new NullLogger(), $this->getClientMockTypedTable());

        $this->expectException(InvalidTableStructureException::class);
        $this->expectExceptionMessage(
            'Table primary keys does not contain the same number of columns as the schema. '.
            'Table primary keys: "col1", schema primary keys: "col2".',
        );
        $validator->validateTable('in.c-main.table', $schema);
    }

    public function testWebalizeTypedTableColumns(): void
    {
        $clientMock = $this->createMock(Client::class);
        $clientMock
            ->expects(self::once())
            ->method('getTable')->willReturn([
                'id' => 'in.c-main.table',
                'isTyped' => true,
                'definition' => [
                    'primaryKeysNames' => [
                        'AbcdefGHij_k_lm',
                    ],
                    'columns' => [
                        [
                            'name' => 'AbcdefGHij_k_lm',
                            'definition' => [
                                'type' => 'VARCHAR',
                                'nullable' => false,
                                'length' => '255',
                            ],
                            'basetype' => 'STRING',
                        ],
                    ],
                ],
                'bucket' => [
                    'backend' => 'snowflake',
                ],
            ]);

        $schema = [
            new MappingFromConfigurationSchemaColumn([
                'name' => '_-AbčďěfGHíj-k_lm_',
                'data_type' => [
                    'base' => [
                        'type' => 'STRING',
                        'length' => '255',
                    ],
                ],
                'nullable' => false,
                'primary_key' => true,
            ]),
        ];

        $validator = new TableStructureValidator(true, new NullLogger(), $clientMock);
        $validator->validateTable('in.c-main.table', $schema);

        self::assertTrue(true);
    }

    public function testValidateNonTypedTableStructure(): void
    {
        $schema = [
            new MappingFromConfigurationSchemaColumn([
                'name' => 'col1',
                'data_type' => [
                    'base' => [
                        'type' => 'STRING',
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
                        'type' => 'STRING',
                        'length' => '123',
                    ],
                ],
            ]),
        ];

        $validator = new TableStructureValidator(true, new NullLogger(), $this->getClientMockNonTypedTable());
        $validator->validateTable('in.c-main.table', $schema);

        self::assertTrue(true);
    }

    public function testNonTypedTableMissingColumnInStorage(): void
    {
        $schema = [
            new MappingFromConfigurationSchemaColumn([
                'name' => 'col1',
                'data_type' => [
                    'base' => [
                        'type' => 'STRING',
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
                'name' => 'col3',
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

        $validator = new TableStructureValidator(true, new NullLogger(), $this->getClientMockNonTypedTable());
        $this->expectException(InvalidTableStructureException::class);
        $this->expectExceptionMessage('Cannot add columns to untyped table "in.c-main.table". Columns: "col4".');
        $validator->validateTable('in.c-main.table', $schema);
    }

    public function testErrorNonTypedTableWrongCountColumns(): void
    {
        $schema = [
            new MappingFromConfigurationSchemaColumn([
                'name' => 'col1',
                'data_type' => [
                    'base' => [
                        'type' => 'STRING',
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

        $validator = new TableStructureValidator(true, new NullLogger(), $this->getClientMockNonTypedTable());
        $this->expectException(InvalidTableStructureException::class);
        $this->expectExceptionMessage(
            'Table "in.c-main.table" does not contain the same number of columns as the schema.'.
            ' Table columns: 3, schema columns: 2.',
        );
        $validator->validateTable('in.c-main.table', $schema);
    }

    public function testErrorNonTypedTableWrongSchemaColumnName(): void
    {
        $schema = [
            new MappingFromConfigurationSchemaColumn([
                'name' => 'col1',
                'data_type' => [
                    'base' => [
                        'type' => 'STRING',
                    ],
                ],
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
                'name' => 'col4',
                'data_type' => [
                    'base' => [
                        'type' => 'STRING',
                    ],
                ],
            ]),
        ];

        $validator = new TableStructureValidator(true, new NullLogger(), $this->getClientMockNonTypedTable());
        $this->expectException(InvalidTableStructureException::class);
        $this->expectExceptionMessage('Table "in.c-main.table" does not contain columns: "col4".');
        $validator->validateTable('in.c-main.table', $schema);
    }

    public function testErrorNonTypedTableWrongColumnDataTypeBackendType(): void
    {
        $schema = [
            new MappingFromConfigurationSchemaColumn([
                'name' => 'col1',
                'data_type' => [
                    'base' => [
                        'type' => 'STRING',
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
                        'type' => 'STRING',
                    ],
                ],
            ]),
        ];

        $validator = new TableStructureValidator(true, new NullLogger(), $this->getClientMockNonTypedTable());
        $this->expectException(InvalidTableStructureException::class);
        $this->expectExceptionMessage(
            'Table "in.c-main.table" is untyped, but schema column "col2" has unsupported type "NUMERIC".',
        );
        $validator->validateTable('in.c-main.table', $schema);
    }

    public function testErrorNonTypedTableHasSetSpecificBackendColumnType(): void
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
                        'type' => 'STRING',
                        'length' => '123',
                    ],
                ],
            ]),
        ];

        $logger = new TestLogger();
        $validator = new TableStructureValidator(true, $logger, $this->getClientMockNonTypedTable());
        $validator->validateTable('in.c-main.table', $schema);

        self::assertTrue($logger->hasWarning(
            'Table "in.c-main.table" is untyped, but schema has set specific backend column "col1".',
        ));
    }

    public function testErrorNonTypedTableWrongColumnMultipleErrors(): void
    {
        $schema = [
            new MappingFromConfigurationSchemaColumn([
                'name' => 'col1',
                'data_type' => [
                    'base' => [
                        'type' => 'DATE',
                    ],
                    'snowflake' => [
                        'type' => 'VARCHAR',
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
                    ],
                ],
            ]),
        ];

        $validator = new TableStructureValidator(true, new NullLogger(), $this->getClientMockNonTypedTable());
        $this->expectException(InvalidTableStructureException::class);
        $expectedMessage = 'Table "in.c-main.table" is untyped, but schema column "col1" has unsupported type "DATE". ';
        $expectedMessage .= 'Table "in.c-main.table" is untyped, ';
        $expectedMessage .= 'but schema column "col3" has unsupported type "NUMERIC".';
        $this->expectExceptionMessage($expectedMessage);
        $validator->validateTable('in.c-main.table', $schema);
    }

    public function testErrorNonTypedTableCountPrimaryKeys(): void
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
                'primary_key' => true,
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

        $validator = new TableStructureValidator(true, new NullLogger(), $this->getClientMockNonTypedTable());

        $this->expectException(InvalidTableStructureException::class);
        $this->expectExceptionMessage(
            'Table primary keys does not contain the same number of columns as the schema. '.
            'Table primary keys: "col1", schema primary keys: "col1, col2".',
        );
        $validator->validateTable('in.c-main.table', $schema);
    }

    public function testErrorNonTypedTableWrongPrimaryKey(): void
    {
        $schema = [
            new MappingFromConfigurationSchemaColumn([
                'name' => 'col1',
                'data_type' => [
                    'base' => [
                        'type' => 'STRING',
                        'length' => '255',
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
                'primary_key' => true,
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

        $validator = new TableStructureValidator(true, new NullLogger(), $this->getClientMockNonTypedTable());

        $this->expectException(InvalidTableStructureException::class);
        $this->expectExceptionMessage(
            'Table primary keys does not contain the same number of columns as the schema. '.
            'Table primary keys: "col1", schema primary keys: "col2".',
        );
        $validator->validateTable('in.c-main.table', $schema);
    }

    public function testWebalizeNonTypedTableColumns(): void
    {
        $clientMock = $this->createMock(Client::class);
        $clientMock
            ->expects(self::once())
            ->method('getTable')->willReturn([
                'id' => 'in.c-main.table',
                'isTyped' => false,
                'primaryKey' => [
                    'AbcdefGHij_k_lm',
                ],
                'columns' => [
                    'AbcdefGHij_k_lm',
                ],
                'bucket' => [
                    'backend' => 'snowflake',
                ],
            ]);

        $schema = [
            new MappingFromConfigurationSchemaColumn([
                'name' => '_-AbčďěfGHíj-k_lm_',
                'data_type' => [
                    'base' => [
                        'type' => 'STRING',
                        'length' => '255',
                    ],
                ],
                'nullable' => false,
                'primary_key' => true,
            ]),
        ];

        $validator = new TableStructureValidator(true, new NullLogger(), $clientMock);
        $validator->validateTable('in.c-main.table', $schema);

        self::assertTrue(true);
    }

    private function getClientMockTypedTable(): Client
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
                                'type' => 'VARCHAR',
                                'nullable' => false,
                                'length' => '255',
                            ],
                            'basetype' => 'STRING',
                        ],
                        [
                            'name' => 'col2',
                            'definition' => [
                                'type' => 'INT',
                                'nullable' => false,
                                'length' => '20',
                            ],
                            'basetype' => 'NUMERIC',
                        ],
                        [
                            'name' => 'col3',
                            'definition' => [
                                'type' => 'INT',
                                'nullable' => true,
                                'length' => '123',
                            ],
                            'basetype' => 'NUMERIC',
                        ],
                    ],
                ],
                'bucket' => [
                    'backend' => 'snowflake',
                ],
            ]);

        return $clientMock;
    }

    private function getClientMockNonTypedTable(): Client
    {
        $clientMock = $this->createMock(Client::class);
        $clientMock
            ->expects(self::once())
            ->method('getTable')->willReturn([
                'id' => 'in.c-main.table',
                'isTyped' => false,
                'primaryKey' => [
                    'col1',
                ],
                'columns' => [
                    'col1',
                    'col2',
                    'col3',
                ],
                'bucket' => [
                    'backend' => 'snowflake',
                ],
            ]);

        return $clientMock;
    }
}
