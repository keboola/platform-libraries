<?php

declare(strict_types=1);

namespace Keboola\OutputMapping\Tests\Writer\Table\TableDefinitionFromSchema;

use Generator;
use Keboola\OutputMapping\Mapping\MappingFromConfigurationSchemaColumn;
use Keboola\OutputMapping\Writer\Table\TableDefinitionFromSchema\TableDefinitionFromSchema;
use PHPUnit\Framework\TestCase;

class TableDefinitionFromSchemaTest extends TestCase
{
    /** @dataProvider convertSchemaDataProvider */
    public function testConvertSchemaToTableDefinitionStructure(
        array $columns,
        string $backend,
        array $expectedOutput,
    ): void {
        $tableDefinition = new TableDefinitionFromSchema('testTableName', $columns, 'snowflake');
        self::assertEquals($expectedOutput, $tableDefinition->getRequestData());
    }

    public function convertSchemaDataProvider(): Generator
    {
        yield 'empty schema' => [
            'columns' => [],
            'backend' => 'snowflake',
            'expectedOutput' => [
                'name' => 'testTableName',
                'primaryKeysNames' => [],
                'columns' => [],
            ],
        ];

        yield 'simple schema' => [
            'columns' => [
                new MappingFromConfigurationSchemaColumn([
                    'name' => 'testNoDefinitionUseBaseType',
                ]),
            ],
            'backend' => 'snowflake',
            'expectedOutput' => [
                'name' => 'testTableName',
                'primaryKeysNames' => [],
                'columns' => [
                    [
                        'name' => 'testNoDefinitionUseBaseType',
                    ],
                ],
            ],
        ];

        yield 'simple schema with primary key' => [
            'columns' => [
                new MappingFromConfigurationSchemaColumn([
                    'name' => 'col1',
                    'primary_key' => true,
                ]),
                new MappingFromConfigurationSchemaColumn([
                    'name' => 'col2',
                    'primary_key' => true,
                ]),
                new MappingFromConfigurationSchemaColumn([
                    'name' => 'col3',
                    'primary_key' => false,
                ]),
            ],
            'backend' => 'snowflake',
            'expectedOutput' => [
                'name' => 'testTableName',
                'primaryKeysNames' => ['col1', 'col2'],
                'columns' => [
                    [
                        'name' => 'col1',
                    ],
                    [
                        'name' => 'col2',
                    ],
                    [
                        'name' => 'col3',
                    ],
                ],
            ],
        ];
    }

    public function testGetTableName(): void
    {
        $tableDefinition = new TableDefinitionFromSchema('testTableName', [], 'snowflake');
        self::assertEquals('testTableName', $tableDefinition->getTableName());
    }
}
