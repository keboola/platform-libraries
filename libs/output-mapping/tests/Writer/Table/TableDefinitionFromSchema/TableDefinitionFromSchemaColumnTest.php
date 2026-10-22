<?php

declare(strict_types=1);

namespace Keboola\OutputMapping\Tests\Writer\Table\TableDefinitionFromSchema;

use Generator;
use Keboola\OutputMapping\Mapping\MappingFromConfigurationSchemaColumn;
use Keboola\OutputMapping\Writer\Table\TableDefinitionFromSchema\TableDefinitionFromSchemaColumn;
use PHPUnit\Framework\TestCase;

class TableDefinitionFromSchemaColumnTest extends TestCase
{
    /** @dataProvider columnDataProvider */
    public function testConvertColumnToTableDefinitionStructure(
        MappingFromConfigurationSchemaColumn $column,
        string $backend,
        array $expectedStructure,
    ): void {
        $tableDefinitionColumn = new TableDefinitionFromSchemaColumn($column, $backend);
        self::assertEquals($expectedStructure, $tableDefinitionColumn->getRequestData());
    }

    public function columnDataProvider(): Generator
    {
        yield 'simple basetype' => [
            'column' => new MappingFromConfigurationSchemaColumn([
                'name' => 'test_table_name',
            ]),
            'backend' => 'snowflake',
            'expectedStructure' => [
                'name' => 'test_table_name',
            ],
        ];

        yield 'simple with basetype' => [
            'column' => new MappingFromConfigurationSchemaColumn([
                'name' => 'test_table_name',
                'data_type' => [
                    'base' => [
                        'type' => 'STRING',
                    ],
                ],
            ]),
            'backend' => 'snowflake',
            'expectedStructure' => [
                'name' => 'test_table_name',
                'basetype' => 'STRING',
            ],
        ];

        yield 'full definition on backend' => [
            'column' => new MappingFromConfigurationSchemaColumn([
                'name' => 'test_table_name',
                'data_type' => [
                    'base' => [
                        'type' => 'STRING',
                    ],
                    'snowflake' => [
                        'type' => 'VARCHAR',
                        'length' => '1024',
                        'default' => 'defaultValue',
                    ],
                ],
                'nullable' => false,
            ]),
            'backend' => 'snowflake',
            'expectedStructure' => [
                'name' => 'test_table_name',
                'basetype' => 'STRING',
                'definition' => [
                    'type' => 'VARCHAR',
                    'length' => '1024',
                    // 'default' => 'defaultValue', // default value is not works correctly
                    'nullable' => false,
                ],
            ],
        ];

        yield 'type definition on backend' => [
            'column' => new MappingFromConfigurationSchemaColumn([
                'name' => 'test_table_name',
                'data_type' => [
                    'base' => [
                        'type' => 'STRING',
                    ],
                    'snowflake' => [
                        'type' => 'VARCHAR',
                    ],
                ],
            ]),
            'backend' => 'snowflake',
            'expectedStructure' => [
                'name' => 'test_table_name',
                'basetype' => 'STRING',
                'definition' => [
                    'type' => 'VARCHAR',
                ],
            ],
        ];

        yield 'length definition on backend' => [
            'column' => new MappingFromConfigurationSchemaColumn([
                'name' => 'test_table_name',
                'data_type' => [
                    'base' => [
                        'type' => 'STRING',
                    ],
                    'snowflake' => [
                        'type' => 'VARCHAR',
                        'length' => '1024',
                    ],
                ],
            ]),
            'backend' => 'snowflake',
            'expectedStructure' => [
                'name' => 'test_table_name',
                'basetype' => 'STRING',
                'definition' => [
                    'type' => 'VARCHAR',
                    'length' => '1024',
                ],
            ],
        ];

        // default value is not works correctly
        //
        // yield 'default definition on backend' => [
        //     'column' => new MappingFromConfigurationSchemaColumn([
        //         'name' => 'test_table_name',
        //         'data_type' => [
        //             'base' => [
        //                 'type' => 'STRING',
        //             ],
        //             'snowflake' => [
        //                 'type' => 'VARCHAR',
        //                 'default' => 'defaultValue',
        //             ],
        //         ],
        //     ]),
        //     'backend' => 'snowflake',
        //     'expectedStructure' => [
        //         'name' => 'test_table_name',
        //         'basetype' => 'STRING',
        //         'definition' => [
        //             'type' => 'VARCHAR',
        //             'default' => 'defaultValue',
        //         ],
        //     ],
        // ];

        yield 'full definition and wrong backend' => [
            'column' => new MappingFromConfigurationSchemaColumn([
                'name' => 'test_table_name',
                'data_type' => [
                    'base' => [
                        'type' => 'STRING',
                    ],
                    'snowflake' => [
                        'type' => 'VARCHAR',
                        'length' => '1024',
                        'default' => 'defaultValue',
                    ],
                ],
            ]),
            'backend' => 'bigquery',
            'expectedStructure' => [
                'name' => 'test_table_name',
                'basetype' => 'STRING',
            ],
        ];

        yield 'backend definition without type' => [
            'column' => new MappingFromConfigurationSchemaColumn([
                'name' => 'test_table_name',
                'data_type' => [
                    'base' => [
                        'type' => 'STRING',
                    ],
                    'snowflake' => [
                        'length' => '1024',
                    ],
                ],
            ]),
            'backend' => 'snowflake',
            'expectedStructure' => [
                'name' => 'test_table_name',
                'basetype' => 'STRING',
            ],
        ];
    }
}
