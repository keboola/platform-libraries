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

class TableStructureValidatorTest extends AbstractTestCase
{
    public function testValidateTableStructure(): void
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

        $validator = new TableStructureValidator(true, new NullLogger(), $this->getClientMock());
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

        $schema = [];
        $validator = new TableStructureValidator(true, new NullLogger(), $clientMock);

        $this->expectException(InvalidOutputException::class);
        $this->expectExceptionMessage('Bad request');
        $validator->validateTable('in.c-main.table', $schema);
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

        $validator = new TableStructureValidator(true, new NullLogger(), $this->getClientMock());
        $this->expectException(InvalidTableStructureException::class);
        $this->expectExceptionMessage(
            'Table "in.c-main.table" does not contain the same number of columns as the schema.'.
            ' Table columns: 3, schema columns: 2.',
        );
        $validator->validateTable('in.c-main.table', $schema);
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

        $validator = new TableStructureValidator(true, new NullLogger(), $this->getClientMock());
        $this->expectException(InvalidTableStructureException::class);
        $this->expectExceptionMessage('Table "in.c-main.table" does not contain columns: "col4".');
        $validator->validateTable('in.c-main.table', $schema);
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
                        'type' => 'BIGINT',
                        'length' => '123',
                    ],
                ],
            ]),
        ];

        $validator = new TableStructureValidator(true, new NullLogger(), $this->getClientMock());
        $this->expectException(InvalidTableStructureException::class);
        $this->expectExceptionMessage(
            'Table "in.c-main.table" column "col3" has different type than the schema. '.
            'Table type: "INT", schema type: "BIGINT".',
        );
        $validator->validateTable('in.c-main.table', $schema);
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

        $validator = new TableStructureValidator(true, new NullLogger(), $this->getClientMock());
        $this->expectException(InvalidTableStructureException::class);
        $this->expectExceptionMessage(
            'Table "in.c-main.table" column "col2" has different type than the schema. '.
            'Table type: "NUMERIC", schema type: "STRING".',
        );
        $validator->validateTable('in.c-main.table', $schema);
    }

    public function testErrorWrongColumnDataTypeBackendLength(): void
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

        $validator = new TableStructureValidator(true, new NullLogger(), $this->getClientMock());
        $this->expectException(InvalidTableStructureException::class);
        $this->expectExceptionMessage(
            'Table "in.c-main.table" column "col3" has different length than the schema. '.
            'Table length: "123", schema length: "543".',
        );
        $validator->validateTable('in.c-main.table', $schema);
    }

    public function testErrorWrongColumnDataTypeBaseLength(): void
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

        $validator = new TableStructureValidator(true, new NullLogger(), $this->getClientMock());
        $this->expectException(InvalidTableStructureException::class);
        $this->expectExceptionMessage(
            'Table "in.c-main.table" column "col3" has different length than the schema. '.
            'Table length: "123", schema length: "987".',
        );
        $validator->validateTable('in.c-main.table', $schema);
    }

    public function testErrorWrongColumnNullable(): void
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

        $validator = new TableStructureValidator(true, new NullLogger(), $this->getClientMock());
        $this->expectException(InvalidTableStructureException::class);
        $this->expectExceptionMessage(
            'Table "in.c-main.table" column "col3" has different nullable than the schema. '.
            'Table nullable: "true", schema nullable: "false".',
        );
        $validator->validateTable('in.c-main.table', $schema);
    }

    public function testErrorWrongColumnMultipleErrors(): void
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

        $validator = new TableStructureValidator(true, new NullLogger(), $this->getClientMock());
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

    public function testErrorCountPrimaryKeys(): void
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

        $validator = new TableStructureValidator(true, new NullLogger(), $this->getClientMock());

        $this->expectException(InvalidTableStructureException::class);
        $this->expectExceptionMessage(
            'Table primary keys does not contain the same number of columns as the schema. '.
            'Table primary keys: "col1", schema primary keys: "col1, col2".',
        );
        $validator->validateTable('in.c-main.table', $schema);
    }

    public function testErrorWrongPrimaryKey(): void
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

        $validator = new TableStructureValidator(true, new NullLogger(), $this->getClientMock());

        $this->expectException(InvalidTableStructureException::class);
        $this->expectExceptionMessage(
            'Table primary keys does not contain the same number of columns as the schema. '.
            'Table primary keys: "col1", schema primary keys: "col2".',
        );
        $validator->validateTable('in.c-main.table', $schema);
    }

    public function testWebalizeColumns(): void
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
}
