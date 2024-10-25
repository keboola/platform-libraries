<?php

declare(strict_types=1);

namespace Keboola\OutputMapping\Tests\Writer\Table\TableDefinition;

use Generator;
use Keboola\Datatype\Definition\GenericStorage;
use Keboola\Datatype\Definition\Snowflake;
use Keboola\OutputMapping\Writer\Table\TableDefinition\TableDefinitionFactory;
use PHPUnit\Framework\TestCase;

class TableDefinitionFactoryTest extends TestCase
{
    /** @dataProvider createTableDefinitionProvider */
    public function testFactoryCreateTableDefinition(
        array $tableMetadata,
        string $backendType,
        string $tableName,
        array $primaryKeyNames,
        array $columnMetadata,
        bool $enforceBaseTypes,
        array $expectedSerialization,
    ): void {
        $tableDefinitionFactory = new TableDefinitionFactory($tableMetadata, $backendType, $enforceBaseTypes);
        $tableDefinition = $tableDefinitionFactory->createTableDefinition(
            $tableName,
            $primaryKeyNames,
            $columnMetadata,
        );
        self::assertSame($expectedSerialization, $tableDefinition->getRequestData());
    }

    public function createTableDefinitionProvider(): Generator
    {
        yield 'base type test' => [
            'tableMetadata' => [],
            'backendType' => 'snowflake',
            'tableName' => 'basicTable',
            'primaryKeyNames' => [
                'one', 'two',
            ],
            'columnMetadata' => [
                'Id' => (new GenericStorage('int', ['nullable' => false]))->toMetadata(),
                'Name' => (new GenericStorage('varchar', ['length' => '17', 'nullable' => false]))->toMetadata(),
                'birthday' => (new GenericStorage('date'))->toMetadata(),
                'created' => (new GenericStorage('timestamp'))->toMetadata(),
                '123' => (new GenericStorage('date'))->toMetadata(),
            ],
            'enforceBaseTypes' => false,
            'expectedSerialisation' => [
                'name' => 'basicTable',
                'primaryKeysNames' => ['one', 'two'],
                'columns' => [
                    [
                        'name' => 'Id',
                        'basetype' => 'INTEGER',
                    ],
                    [
                        'name' => 'Name',
                        'basetype' => 'STRING',
                    ],
                    [
                        'name' => 'birthday',
                        'basetype' => 'DATE',
                    ],
                    [
                        'name' => 'created',
                        'basetype' => 'TIMESTAMP',
                    ],
                    [
                        'name' => '123',
                        'basetype' => 'DATE',
                    ],
                ],
            ],
        ];

        yield 'test using native types' => [
            'tableMetadata' => [
                [
                    'key' => 'KBC.datatype.backend',
                    'value' => 'snowflake',
                ],
            ],
            'backendType' => 'snowflake',
            'tableName' => 'snowflakeNativeTable',
            'primaryKeyNames' => [
                'one', 'two',
            ],
            'columnMetadata' => [
                'Id' => (new Snowflake(Snowflake::TYPE_INTEGER, ['nullable' => false]))->toMetadata(),
                'Name' => (new Snowflake(Snowflake::TYPE_TEXT, ['length' => '127']))->toMetadata(),
                'birthtime' => (new Snowflake(Snowflake::TYPE_TIME))->toMetadata(),
                'created' => (new Snowflake(Snowflake::TYPE_TIMESTAMP_TZ))->toMetadata(),
                '123' => (new Snowflake(Snowflake::TYPE_TIME))->toMetadata(),
            ],
            'enforceBaseTypes' => false,
            'expectedSerialisation' => [
                'name' => 'snowflakeNativeTable',
                'primaryKeysNames' => ['one', 'two'],
                'columns' => [
                    [
                        'name' => 'Id',
                        'definition' => [
                            'type' => 'INTEGER',
                            'length' => null,
                            'nullable' => false,
                        ],
                    ],
                    [
                        'name' => 'Name',
                        'definition' => [
                            'type' => 'TEXT',
                            'length' => '127',
                            'nullable' => true,
                        ],
                    ],
                    [
                        'name' => 'birthtime',
                        'definition' => [
                            'type' => 'TIME',
                            'length' => null,
                            'nullable' => true,
                        ],
                    ],
                    [
                        'name' => 'created',
                        'definition' => [
                            'type' => 'TIMESTAMP_TZ',
                            'length' => null,
                            'nullable' => true,
                        ],
                    ],
                    [
                        'name' => '123',
                        'definition' => [
                            'type' => 'TIME',
                            'length' => null,
                            'nullable' => true,
                        ],
                    ],
                ],
            ],
        ];

        yield 'test enforce base types' => [
            'tableMetadata' => [
                [
                    'key' => 'KBC.datatype.backend',
                    'value' => 'snowflake',
                ],
            ],
            'backendType' => 'snowflake',
            'tableName' => 'snowflakeNativeTable',
            'primaryKeyNames' => [
                'one', 'two',
            ],
            'columnMetadata' => [
                'Id' => (new Snowflake(Snowflake::TYPE_INTEGER, ['nullable' => false]))->toMetadata(),
                'Name' => (new Snowflake(Snowflake::TYPE_TEXT, ['length' => '127']))->toMetadata(),
                'birthtime' => (new Snowflake(Snowflake::TYPE_TIME))->toMetadata(),
                'created' => (new Snowflake(Snowflake::TYPE_TIMESTAMP_TZ))->toMetadata(),
                '123' => (new Snowflake(Snowflake::TYPE_TIME))->toMetadata(),
            ],
            'enforceBaseTypes' => true,
            'expectedSerialisation' => [
                'name' => 'snowflakeNativeTable',
                'primaryKeysNames' => ['one', 'two'],
                'columns' => [
                    [
                        'name' => 'Id',
                        'basetype' => 'INTEGER',
                    ],
                    [
                        'name' => 'Name',
                        'basetype' => 'STRING',
                    ],
                    [
                        'name' => 'birthtime',
                        'basetype' => 'STRING',
                    ],
                    [
                        'name' => 'created',
                        'basetype' => 'TIMESTAMP',
                    ],
                    [
                        'name' => '123',
                        'basetype' => 'STRING',
                    ],
                ],
            ],
        ];
    }
}
