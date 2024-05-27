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
                        'length' => '123',
                    ],
                ],
            ]),
        ];

        $validator = new TableStructureValidator(true, $this->getClientMock());
        $validator->validateTable('in.c-main.table', $schema);

        self::assertTrue(true);
    }

    public function testSkipValidationWithoutFeature(): void
    {
        $schema = [];
        $validator = new TableStructureValidator(false, $this->getClientMock());
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

        $validator = new TableStructureValidator(true, $clientMock);
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
        $validator = new TableStructureValidator(true, $clientMock);

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

        $validator = new TableStructureValidator(true, $this->getClientMock());
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

        $validator = new TableStructureValidator(true, $this->getClientMock());
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

        $validator = new TableStructureValidator(true, $this->getClientMock());
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

        $validator = new TableStructureValidator(true, $this->getClientMock());
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

        $validator = new TableStructureValidator(true, $this->getClientMock());
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

        $validator = new TableStructureValidator(true, $this->getClientMock());
        $this->expectException(InvalidTableStructureException::class);
        $this->expectExceptionMessage(
            'Table "in.c-main.table" column "col3" has different length than the schema. '.
            'Table length: "123", schema length: "987".',
        );
        $validator->validateTable('in.c-main.table', $schema);
    }

    public function testErrorWrongColumnDataTypeTableLengthNotSet(): void
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
                        'length' => '255',
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
                ],
            ]),
        ];

        $validator = new TableStructureValidator(true, $this->getClientMock());
        $this->expectException(InvalidTableStructureException::class);
        $this->expectExceptionMessage('Table "in.c-main.table" column "col2" has not set length.');
        $validator->validateTable('in.c-main.table', $schema);
    }

    private function getClientMock(): Client
    {
        $clientMock = $this->createMock(Client::class);
        $clientMock->method('getTable')->willReturn([
            'id' => 'in.c-main.table',
            'isTyped' => true,
            'columns' => [
                'col1',
                'col2',
                'col3',
            ],
            'columnMetadata' => [
                'col1' => [
                    [
                        'key' => 'KBC.datatype.type',
                        'value' => 'VARCHAR',
                    ],
                    [
                        'key' => 'KBC.datatype.basetype',
                        'value' => 'STRING',
                    ],
                    [
                        'key' => 'KBC.datatype.length',
                        'value' => '255',
                    ],
                ],
                'col2' => [
                    [
                        'key' => 'KBC.datatype.type',
                        'value' => 'INT',
                    ],
                    [
                        'key' => 'KBC.datatype.basetype',
                        'value' => 'NUMERIC',
                    ],
                ],
                'col3' => [
                    [
                        'key' => 'KBC.datatype.type',
                        'value' => 'INT',
                    ],
                    [
                        'key' => 'KBC.datatype.basetype',
                        'value' => 'NUMERIC',
                    ],
                    [
                        'key' => 'KBC.datatype.length',
                        'value' => '123',
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
