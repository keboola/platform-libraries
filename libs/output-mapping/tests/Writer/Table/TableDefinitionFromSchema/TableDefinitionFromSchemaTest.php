<?php

declare(strict_types=1);

namespace Keboola\OutputMapping\Tests\Writer\Table\TableDefinitionFromSchema;

use Generator;
use Keboola\OutputMapping\Mapping\MappingFromConfigurationSchemaColumn;
use Keboola\OutputMapping\Mapping\MappingTableLayout;
use Keboola\OutputMapping\Storage\TableDescription;
use Keboola\OutputMapping\Writer\Table\TableDefinitionFromSchema\TableDefinitionFromSchema;
use PHPUnit\Framework\TestCase;
use Psr\Log\NullLogger;

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

    public function testRequestDataWithLayout(): void
    {
        $tableDefinition = new TableDefinitionFromSchema(
            'testTable',
            [
                new MappingFromConfigurationSchemaColumn(['name' => 'Id', 'primary_key' => true]),
                new MappingFromConfigurationSchemaColumn(['name' => 'Name']),
            ],
            'bigquery',
        );

        self::assertArrayNotHasKey('timePartitioning', $tableDefinition->getRequestData());
        self::assertArrayNotHasKey('rangePartitioning', $tableDefinition->getRequestData());
        self::assertArrayNotHasKey('clustering', $tableDefinition->getRequestData());

        $tableDefinition->setDescriptions(
            new TableDescription('in.c-main.testTable', 'table description', ['Id' => 'id description']),
            new NullLogger(),
        );
        $tableDefinition->setLayout(new MappingTableLayout(
            ['type' => 'DAY', 'field' => 'Id', 'expiration_ms' => 1000],
            null,
            ['fields' => ['Name']],
        ));

        self::assertSame(
            [
                'name' => 'testTable',
                'primaryKeysNames' => ['Id'],
                'columns' => [
                    ['name' => 'Id', 'definition' => ['description' => 'id description']],
                    ['name' => 'Name'],
                ],
                'description' => 'table description',
                'timePartitioning' => ['type' => 'DAY', 'field' => 'Id', 'expirationMs' => 1000],
                'clustering' => ['fields' => ['Name']],
            ],
            $tableDefinition->getRequestData(),
        );
    }
}
