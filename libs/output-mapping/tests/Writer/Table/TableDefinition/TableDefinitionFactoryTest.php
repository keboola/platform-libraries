<?php

declare(strict_types=1);

namespace Keboola\OutputMapping\Tests\Writer\Table\TableDefinition;

use Keboola\Datatype\Definition\GenericStorage;
use Keboola\Datatype\Definition\Snowflake;
use Keboola\OutputMapping\Writer\Table\TableDefinition\TableDefinitionColumnFactory;
use Keboola\OutputMapping\Writer\Table\TableDefinition\TableDefinitionFactory;
use PhpUnit\Framework\TestCase;

class TableDefinitionFactoryTest extends TestCase
{
    /** @dataProvider createTableDefinitionProvider */
    public function testFactoryCreateTableDefinition(
        array $tableMetadata,
        string $backendType,
        string $tableName,
        array $primaryKeyNames,
        array $columnMetadata,
        array $expectedSerialization
    ): void {
        $tableDefinitionFactory = new TableDefinitionFactory($tableMetadata, $backendType);
        $tableDefinition = $tableDefinitionFactory->createTableDefinition($tableName, $primaryKeyNames, $columnMetadata);
        $this->assertSame($expectedSerialization, $tableDefinition->getRequestData());
    }

    public function createTableDefinitionProvider(): \Generator
    {
        yield [
            [],
            'snowflake',
            'basicTable',
            [
                'one', 'two',
            ],
            [
                'Id' => (new GenericStorage('int', ['nullable' => false]))->toMetadata(),
                'Name' => (new GenericStorage('varchar', ['length' => '17', 'nullable' => false]))->toMetadata(),
                'birthday' => (new GenericStorage('date'))->toMetadata(),
                'created' => (new GenericStorage('timestamp'))->toMetadata(),
            ],
            [
                'name' => 'basicTable',
                'primaryKeysNames' => ['one', 'two'],
                'columns' => [
                    [
                        'name' => 'Id',
                        'basetype' => 'INTEGER'
                    ],
                    [
                        'name' => 'Name',
                        'basetype' => 'STRING'
                    ],
                    [
                        'name' => 'birthday',
                        'basetype' => 'DATE'
                    ],
                    [
                        'name' => 'created',
                        'basetype' => 'TIMESTAMP'
                    ],
                ],
            ],
        ];
        // test using native types
        yield [
            [
                [
                    'key' => TableDefinitionColumnFactory::NATIVE_TYPE_METADATA_KEY,
                    'value' => 'snowflake',
                ],
            ],
            'snowflake',
            'snowflakeNativeTable',
            [
                'one', 'two',
            ],
            [
                'Id' => (new Snowflake(Snowflake::TYPE_INTEGER, ['nullable' => false]))->toMetadata(),
                'Name' => (new Snowflake(Snowflake::TYPE_TEXT, ['length' => '127']))->toMetadata(),
                'birthtime' => (new Snowflake(Snowflake::TYPE_TIME))->toMetadata(),
                'created' => (new Snowflake(Snowflake::TYPE_TIMESTAMP_TZ))->toMetadata(),
            ],
            [
                'name' => 'snowflakeNativeTable',
                'primaryKeysNames' => ['one', 'two'],
                'columns' => [
                    [
                        'name' => 'Id',
                        'definition' => [
                            'type' => 'INTEGER',
                            'length' => null,
                            'nullable' => false,
                        ]
                    ],
                    [
                        'name' => 'Name',
                        'definition' => [
                            'type' => 'TEXT',
                            'length' => '127',
                            'nullable' => true,
                        ]
                    ],
                    [
                        'name' => 'birthtime',
                        'definition' => [
                            'type' => 'TIME',
                            'length' => null,
                            'nullable' => true,
                        ]
                    ],
                    [
                        'name' => 'created',
                        'definition' => [
                            'type' => 'TIMESTAMP_TZ',
                            'length' => null,
                            'nullable' => true,
                        ]
                    ],
                ],
            ],
        ];
    }
}
