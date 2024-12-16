<?php

declare(strict_types=1);

namespace Keboola\OutputMapping\Tests\Storage;

use Keboola\Datatype\Definition\BaseType;
use Keboola\Datatype\Definition\Snowflake;
use Keboola\OutputMapping\Exception\InvalidOutputException;
use Keboola\OutputMapping\Exception\InvalidTableStructureException;
use Keboola\OutputMapping\Mapping\MappingFromConfigurationSchemaColumn;
use Keboola\OutputMapping\Storage\TableStructureValidator;
use Keboola\StorageApi\Client;
use PHPUnit\Framework\TestCase;
use Psr\Log\NullLogger;

class TableStructureValidatorTest extends TestCase
{
    public function testNonTypedTableMissingColumnInStorage(): void
    {
        $newColumn = new MappingFromConfigurationSchemaColumn([
            'name' => 'col4',
            'data_type' => [
                'base' => [
                    'type' => 'STRING',
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
                    ],
                ],
            ]),
            $newColumn,
        ];

        $validator = new TableStructureValidator(
            new NullLogger(),
            $this->getClientMock()->getTable('in.c-main.table'),
        );
        $tableChangesStore = $validator->validate($schema);

        self::assertTrue($tableChangesStore->hasMissingColumns());
        self::assertEquals([$newColumn], $tableChangesStore->getMissingColumns());
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

        $validator = new TableStructureValidator(
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

        $validator = new TableStructureValidator(
            new NullLogger(),
            $this->getClientMock()->getTable('in.c-main.table'),
        );
        $this->expectException(InvalidTableStructureException::class);
        $this->expectExceptionMessage('Table "in.c-main.table" does not contain columns: "col4".');
        $validator->validate($schema);
    }

    public function testErrorNonTypedTableCountPrimaryKeys(): void
    {
        $col1Pk = new MappingFromConfigurationSchemaColumn([
            'name' => 'col1',
            'data_type' => [
                'base' => [
                    'type' => 'STRING',
                ],
            ],
            'nullable' => false,
            'primary_key' => true,
        ]);

        $col2Pk = new MappingFromConfigurationSchemaColumn([
            'name' => 'col2',
            'data_type' => [
                'base' => [
                    'type' => 'STRING',
                ],
            ],
            'nullable' => false,
            'primary_key' => true,
        ]);

        $schema = [
            $col1Pk,
            $col2Pk,
            new MappingFromConfigurationSchemaColumn([
                'name' => 'col3',
                'data_type' => [
                    'base' => [
                        'type' => 'STRING',
                    ],
                ],
                'nullable' => true,
            ]),
        ];

        $validator = new TableStructureValidator(
            new NullLogger(),
            $this->getClientMock()->getTable('in.c-main.table'),
        );
        $tableChangesStore = $validator->validate($schema);

        self::assertNotNull($tableChangesStore->getPrimaryKey());
        self::assertEquals([$col1Pk, $col2Pk], $tableChangesStore->getPrimaryKey()->getColumns());
    }

    public function testErrorNonTypedTableWrongPrimaryKey(): void
    {
        $col2Pk = new MappingFromConfigurationSchemaColumn([
            'name' => 'col2',
            'data_type' => [
                'base' => [
                    'type' => 'STRING',
                ],
            ],
            'nullable' => false,
            'primary_key' => true,
        ]);

        $schema = [
            new MappingFromConfigurationSchemaColumn([
                'name' => 'col1',
                'data_type' => [
                    'base' => [
                        'type' => 'STRING',
                    ],
                ],
                'nullable' => false,
            ]),
            $col2Pk,
            new MappingFromConfigurationSchemaColumn([
                'name' => 'col3',
                'data_type' => [
                    'base' => [
                        'type' => 'STRING',
                    ],
                ],
                'nullable' => true,
            ]),
        ];

        $validator = new TableStructureValidator(
            new NullLogger(),
            $this->getClientMock()->getTable('in.c-main.table'),
        );
        $tableChangesStore = $validator->validate($schema);

        self::assertNotNull($tableChangesStore->getPrimaryKey());
        self::assertEquals([$col2Pk], $tableChangesStore->getPrimaryKey()->getColumns());
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

        $validator = new TableStructureValidator(
            new NullLogger(),
            $this->getClientMock()->getTable('in.c-main.table'),
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
                'isTyped' => true,
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

        $validator = new TableStructureValidator(
            new NullLogger(),
            $clientMock->getTable('in.c-main.table'),
        );

        $this->expectException(InvalidOutputException::class);
        $this->expectExceptionMessage('Table is typed.');
        $validator->validate($schema);

        self::assertTrue(true);
    }

    private function getClientMock(): Client
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
