<?php

declare(strict_types=1);

namespace Keboola\OutputMapping\Tests\Writer\Helper;

use Keboola\OutputMapping\Exception\InvalidOutputException;
use Keboola\OutputMapping\Writer\Helper\ConfigurationMerger;
use PHPUnit\Framework\TestCase;

class ConfigurationMergerTest extends TestCase
{
    /**
     * @dataProvider provideConfigurations
     * @param array $mapping
     * @param array $manifest
     * @param array $expected
     */
    public function testMergeConfigurations(array $mapping, array $manifest, array $expected): void
    {
        $result = ConfigurationMerger::mergeConfigurations($manifest, $mapping);
        self::assertEquals($expected, $result);
    }

    public function testMergePrimaryKeysConfigurationHasUnexistsPK(): void
    {
        $mapping = [
            'primary_key' => ['Name2'],
        ];

        $manifest = [
            'schema' => [
                [
                    'name' => 'Id',
                    'data_type' => [
                        'base' => [
                            'type' => 'STRING',
                        ],
                    ],
                    'nullable' => false,
                ],
                [
                    'name' => 'Name',
                    'data_type' => [
                        'base' => [
                            'type' => 'STRING',
                        ],
                    ],
                    'nullable' => false,
                ],
            ],
        ];

        $this->expectException(InvalidOutputException::class);
        $this->expectExceptionMessage('Primary key column "Name2" not found in manifest file.');
        ConfigurationMerger::mergeConfigurations($manifest, $mapping);
    }

    public function provideConfigurations(): array
    {
        return [
            'schema-empty-mapping' => [
                'mapping' => [],
                'manifest' => [
                    'schema' => [
                        [
                            'name' => 'col1',
                            'nullable' => false,
                        ],
                    ],
                ],
                'expected' => [
                    'schema' => [
                        [
                            'name' => 'col1',
                            'nullable' => false,
                        ],
                    ],
                ],
            ],
            'schema-empty-manifest' => [
                'mapping' => [
                    'schema' => [
                        [
                            'name' => 'col1',
                            'nullable' => false,
                        ],
                    ],
                ],
                'manifest' => [],
                'expected' => [
                    'schema' => [
                        [
                            'name' => 'col1',
                            'nullable' => false,
                        ],
                    ],
                ],
            ],
            'override-schema-manifest' => [
                'mapping' => [
                    'schema' => [
                        [
                            'name' => 'col1',
                            'nullable' => true,
                            'data_type' => [
                                'base' => [
                                    'type' => 'STRING',
                                ],
                            ],
                        ],
                    ],
                ],
                'manifest' => [
                    'schema' => [
                        [
                            'name' => 'col1',
                            'nullable' => false,
                        ],
                    ],
                ],
                'expected' => [
                    'schema' => [
                        [
                            'name' => 'col1',
                            'nullable' => true,
                            'data_type' => [
                                'base' => [
                                    'type' => 'STRING',
                                ],
                            ],
                        ],
                    ],
                ],
            ],
            'override-full-schema' => [
                'mapping' => [
                    'schema' => [
                        [
                            'name' => 'col1',
                            'data_type' => [
                                'base' => [
                                    'type' => 'STRING',
                                ],
                                'snowflake' => [
                                    'type' => 'DATE',
                                ],
                                'exasol' => [
                                    'type' => 'INT',
                                    'length' => 35,
                                ],
                            ],
                            'nullable' => false,
                            'primary_key' => false,
                            'description' => 'mappingDesc',
                            'metadata' => [
                                'a' => 'c',
                                'd' => 'e',
                            ],
                        ],
                    ],
                ],
                'manifest' => [
                    'schema' => [
                        [
                            'name' => 'col1',
                            'data_type' => [
                                'base' => [
                                    'type' => 'STRING',
                                ],
                                'snowflake' => [
                                    'type' => 'VARCHAR',
                                    'length' => 255,
                                ],
                                'synapse' => [
                                    'type' => 'NVARCHAR',
                                    'length' => 35,
                                ],
                            ],
                            'nullable' => true,
                            'primary_key' => true,
                            'description' => 'desc',
                            'metadata' => [
                                'a' => 'b',
                            ],
                        ],
                    ],
                ],
                'expected' => [
                    'schema' => [
                        [
                            'name' => 'col1',
                            'data_type' => [
                                'base' => [
                                    'type' => 'STRING',
                                ],
                                'snowflake' => [
                                    'type' => 'DATE',
                                ],
                                'exasol' => [
                                    'type' => 'INT',
                                    'length' => 35,
                                ],
                                'synapse' => [
                                    'type' => 'NVARCHAR',
                                    'length' => 35,
                                ],
                            ],
                            'nullable' => false,
                            'primary_key' => false,
                            'description' => 'mappingDesc',
                            'metadata' => [
                                'a' => 'c',
                                'd' => 'e',
                            ],
                        ],
                    ],
                ],
            ],
            'schema-add-new-column-in-mapping' => [
                'mapping' => [
                    'schema' => [
                        [
                            'name' => 'col1',
                        ],
                        [
                            'name' => 'col2',
                            'primary_key' => true,
                        ],
                    ],
                ],
                'manifest' => [
                    'schema' => [
                        [
                            'name' => 'col1',
                        ],
                    ],
                ],
                'expected' => [
                    'schema' => [
                        [
                            'name' => 'col1',
                        ],
                        [
                            'name' => 'col2',
                            'primary_key' => true,
                        ],
                    ],
                ],
            ],
            'override-table-metadata' => [
                'mapping' => [
                    'table_metadata' => [
                        'a' => 'a1',
                        'c' => 'c1',
                    ],
                ],
                'manifest' => [
                    'table_metadata' => [
                        'a' => 'a2',
                        'b' => 'b2',
                    ],
                ],
                'expected' => [
                    'table_metadata' => [
                        'a' => 'a1',
                        'b' => 'b2',
                        'c' => 'c1',
                    ],
                ],
            ],
            'override-table-metadata-empty-manifest' => [
                'mapping' => [
                    'table_metadata' => [
                        'a' => 'a1',
                        'c' => 'c1',
                    ],
                ],
                'manifest' => [],
                'expected' => [
                    'table_metadata' => [
                        'a' => 'a1',
                        'c' => 'c1',
                    ],
                ],
            ],
            'override-schema-metadata' => [
                'mapping' => [
                    'schema' => [
                        [
                            'name' => 'col1',
                            'metadata' => [
                                'a' => 'a1',
                                'c' => 'c1',
                            ],
                        ],
                    ],
                ],
                'manifest' => [
                    'schema' => [
                        [
                            'name' => 'col1',
                            'metadata' => [
                                'a' => 'a2',
                                'b' => 'b2',
                            ],
                        ],
                    ],
                ],
                'expected' => [
                    'schema' => [
                        [
                            'name' => 'col1',
                            'metadata' => [
                                'a' => 'a1',
                                'b' => 'b2',
                                'c' => 'c1',
                            ],
                        ],
                    ],
                ],
            ],
            'empty-manifest' => [
                'mapping' => [
                    'destination' => 'in.c-main.test',
                    'primary_key' => [],
                    'columns' => [],
                    'incremental' => false,
                    'delete_where_values' => [],
                    'delete_where_operator' => 'eq',
                    'delimiter' => ',',
                    'enclosure' => '"',
                ],
                'manifest' => [],
                'expected' => [
                    'destination' => 'in.c-main.test',
                    'primary_key' => [],
                    'columns' => [],
                    'incremental' => false,
                    'delete_where_values' => [],
                    'delete_where_operator' => 'eq',
                    'delimiter' => ',',
                    'enclosure' => '"',
                ],
            ],
            'empty-mapping' => [
                'mapping' => [],
                'manifest' => [
                    'destination' => 'in.c-main.test',
                    'primary_key' => [],
                    'columns' => [],
                    'incremental' => false,
                    'delete_where_values' => [],
                    'delete_where_operator' => 'eq',
                    'delimiter' => ',',
                    'enclosure' => '"',
                ],
                'expected' => [
                    'destination' => 'in.c-main.test',
                    'primary_key' => [],
                    'columns' => [],
                    'incremental' => false,
                    'delete_where_values' => [],
                    'delete_where_operator' => 'eq',
                    'delimiter' => ',',
                    'enclosure' => '"',
                ],
            ],
            'manifest-overwrite' => [
                'mapping' => [
                    'destination' => 'in.c-main.test2',
                    'primary_key' => ['a', 'b'],
                    'columns' => ['c', 'd'],
                    'incremental' => true,
                    'delete_where_values' => ['e', 'f'],
                    'delete_where_operator' => 'neq',
                    'delimiter' => ':',
                    'enclosure' => '|',
                ],
                'manifest' => [
                    'destination' => 'in.c-main.test',
                    'primary_key' => [],
                    'columns' => [],
                    'incremental' => false,
                    'delete_where_values' => [],
                    'delete_where_operator' => 'eq',
                    'delimiter' => ',',
                    'enclosure' => '"',
                ],
                'expected' => [
                    'destination' => 'in.c-main.test2',
                    'primary_key' => ['a', 'b'],
                    'columns' => ['c', 'd'],
                    'incremental' => true,
                    'delete_where_values' => ['e', 'f'],
                    'delete_where_operator' => 'neq',
                    'delimiter' => ':',
                    'enclosure' => '|',
                ],
            ],
            'defaults-overwrite' => [
                'mapping' => [
                    'destination' => 'in.c-main.test',
                    'primary_key' => ['g'],
                    'columns' => ['h'],
                    'delete_where_values' => ['i'],
                ],
                'manifest' => [
                    'destination' => 'in.c-main.test2',
                    'primary_key' => ['a', 'b'],
                    'columns' => ['c', 'd'],
                    'incremental' => true,
                    'delete_where_values' => ['e', 'f'],
                    'delete_where_operator' => 'neq',
                    'delimiter' => ':',
                    'enclosure' => '|',
                ],
                'expected' => [
                    'destination' => 'in.c-main.test',
                    'primary_key' => ['g'],
                    'columns' => ['h'],
                    'incremental' => true,
                    'delete_where_values' => ['i'],
                    'delete_where_operator' => 'neq',
                    'delimiter' => ':',
                    'enclosure' => '|',
                ],
            ],
            'defaults-no-overwrite' => [
                // this is a very weird case that needs to be fixed https://keboola.atlassian.net/browse/PS-364
                'mapping' => [
                    'incremental' => false,
                    'delete_where_operator' => 'eq',
                    'delimiter' => ',',
                    'enclosure' => '"',
                ],
                'manifest' => [
                    'destination' => 'in.c-main.test2',
                    'primary_key' => ['a', 'b'],
                    'columns' => ['c', 'd'],
                    'incremental' => true,
                    'delete_where_values' => ['e', 'f'],
                    'delete_where_operator' => 'neq',
                    'delimiter' => ':',
                    'enclosure' => '|',
                ],
                'expected' => [
                    'destination' => 'in.c-main.test2',
                    'primary_key' => ['a', 'b'],
                    'columns' => ['c', 'd'],
                    'incremental' => true,
                    'delete_where_values' => ['e', 'f'],
                    'delete_where_operator' => 'neq',
                    'delimiter' => ':',
                    'enclosure' => '|',
                ],
            ],
            'metadata-empty-manifest' => [
                'mapping' => [
                    'metadata' => [
                        [
                            'key' => 'a',
                            'value' => 'b',
                        ],
                        [
                            'key' => 'c',
                            'value' => 'd',
                        ],
                    ],
                ],
                'manifest' => [],
                'expected' => [
                    'metadata' => [
                        [
                            'key' => 'a',
                            'value' => 'b',
                        ],
                        [
                            'key' => 'c',
                            'value' => 'd',
                        ],
                    ],
                ],
            ],
            'metadata-empty-mapping' => [
                'mapping' => [],
                'manifest' => [
                    'metadata' => [
                        [
                            'key' => 'a',
                            'value' => 'b',
                        ],
                        [
                            'key' => 'c',
                            'value' => 'd',
                        ],
                    ],
                ],
                'expected' => [
                    'metadata' => [
                        [
                            'key' => 'a',
                            'value' => 'b',
                        ],
                        [
                            'key' => 'c',
                            'value' => 'd',
                        ],
                    ],
                ],
            ],
            'metadata-merge' => [
                'mapping' => [
                    'metadata' => [
                        [
                            'key' => 'a',
                            'value' => 'b',
                        ],
                        [
                            'key' => 'c',
                            'value' => 'd',
                        ],
                    ],
                ],
                'manifest' => [
                    'metadata' => [
                        [
                            'key' => 'b',
                            'value' => 'b2',
                        ],
                        [
                            'key' => 'c',
                            'value' => 'c2',
                        ],
                    ],
                ],
                'expected' => [
                    'metadata' => [
                        [
                            'key' => 'b',
                            'value' => 'b2',
                        ],
                        [
                            'key' => 'c',
                            'value' => 'd',
                        ],
                        [
                            'key' => 'a',
                            'value' => 'b',
                        ],
                    ],
                ],
            ],
            'column-metadata-empty-manifest' => [
                'mapping' => [
                    'column_metadata' => [
                        'column1' => [
                            [
                                'key' => 'a',
                                'value' => 'b',
                            ],
                            [
                                'key' => 'c',
                                'value' => 'd',
                            ],
                        ],
                        'column2' => [
                            [
                                'key' => 'a2',
                                'value' => 'b2',
                            ],
                            [
                                'key' => 'c2',
                                'value' => 'd2',
                            ],
                        ],
                    ],
                ],
                'manifest' => [],
                'expected' => [
                    'column_metadata' => [
                        'column1' => [
                            [
                                'key' => 'a',
                                'value' => 'b',
                            ],
                            [
                                'key' => 'c',
                                'value' => 'd',
                            ],
                        ],
                        'column2' => [
                            [
                                'key' => 'a2',
                                'value' => 'b2',
                            ],
                            [
                                'key' => 'c2',
                                'value' => 'd2',
                            ],
                        ],
                    ],
                ],
            ],
            'column-metadata-empty-mapping' => [
                'mapping' => [],
                'manifest' => [
                    'column_metadata' => [
                        'column1' => [
                            [
                                'key' => 'a',
                                'value' => 'b',
                            ],
                            [
                                'key' => 'c',
                                'value' => 'd',
                            ],
                        ],
                        'column2' => [
                            [
                                'key' => 'a2',
                                'value' => 'b2',
                            ],
                            [
                                'key' => 'c2',
                                'value' => 'd2',
                            ],
                        ],
                    ],
                ],
                'expected' => [
                    'column_metadata' => [
                        'column1' => [
                            [
                                'key' => 'a',
                                'value' => 'b',
                            ],
                            [
                                'key' => 'c',
                                'value' => 'd',
                            ],
                        ],
                        'column2' => [
                            [
                                'key' => 'a2',
                                'value' => 'b2',
                            ],
                            [
                                'key' => 'c2',
                                'value' => 'd2',
                            ],
                        ],
                    ],
                ],
            ],
            'column-metadata-merge' => [
                'mapping' => [
                    'column_metadata' => [
                        'column1' => [
                            [
                                'key' => 'a',
                                'value' => 'b',
                            ],
                            [
                                'key' => 'c',
                                'value' => 'd',
                            ],
                        ],
                        'column2' => [
                            [
                                'key' => 'a2',
                                'value' => 'b2',
                            ],
                            [
                                'key' => 'c2',
                                'value' => 'd2',
                            ],
                        ],
                    ],
                ],
                'manifest' => [
                    'column_metadata' => [
                        'column2' => [
                            [
                                'key' => 'a3',
                                'value' => 'b3',
                            ],
                            [
                                'key' => 'c2',
                                'value' => 'd3',
                            ],
                        ],
                        'column3' => [
                            [
                                'key' => 'a4',
                                'value' => 'b4',
                            ],
                            [
                                'key' => 'c4',
                                'value' => 'd4',
                            ],
                        ],
                    ],
                ],
                'expected' => [
                    'column_metadata' => [
                        'column2' => [
                            [
                                'key' => 'a3',
                                'value' => 'b3',
                            ],
                            [
                                'key' => 'c2',
                                'value' => 'd2',
                            ],
                            [
                                'key' => 'a2',
                                'value' => 'b2',
                            ],
                        ],
                        'column3' => [
                            [
                                'key' => 'a4',
                                'value' => 'b4',
                            ],
                            [
                                'key' => 'c4',
                                'value' => 'd4',
                            ],
                        ],
                        'column1' => [
                            [
                                'key' => 'a',
                                'value' => 'b',
                            ],
                            [
                                'key' => 'c',
                                'value' => 'd',
                            ],
                        ],
                    ],
                ],
            ],
            'metadata-with-multiple-matching-keys' => [
                'mapping' => [
                    'metadata' => [
                        [
                            'key' => 'table.key.one',
                            'value' => 'table value three',
                        ],
                        [
                            'key' => 'table.key.two',
                            'value' => 'table value four',
                        ],
                    ],
                ],
                'manifest' => [
                    'metadata' => [
                        [
                            'key' => 'table.key.one',
                            'value' => 'table value one',
                        ],
                        [
                            'key' => 'table.key.two',
                            'value' => 'table value two',
                        ],
                    ],
                ],
                'expected' => [
                    'metadata' => [
                        [
                            'key' => 'table.key.one',
                            'value' => 'table value three',
                        ],
                        [
                            'key' => 'table.key.two',
                            'value' => 'table value four',
                        ],
                    ],
                ],
            ],
            'schema-manifest-legacy-configuration' => [
                'mapping' => [
                    'primary_key' => ['Id'],
                ],
                'manifest' => [
                   'schema' => [
                       [
                           'name' => 'Id',
                           'data_type' => [
                               'base' => [
                                   'type' => 'STRING',
                               ],
                           ],
                           'nullable' => false,
                       ],
                       [
                           'name' => 'Name',
                           'data_type' => [
                               'base' => [
                                   'type' => 'STRING',
                               ],
                           ],
                           'nullable' => false,
                       ],
                       [
                           'name' => 'Name2',
                           'data_type' => [
                               'base' => [
                                   'type' => 'STRING',
                               ],
                           ],
                           'nullable' => false,
                           'primary_key' => true,
                       ],
                   ],
                ],
                'expected' => [
                    'schema' => [
                        [
                            'name' => 'Id',
                            'data_type' => [
                                'base' => [
                                    'type' => 'STRING',
                                ],
                            ],
                            'nullable' => false,
                            'primary_key' => true,
                        ],
                        [
                            'name' => 'Name',
                            'data_type' => [
                                'base' => [
                                    'type' => 'STRING',
                                ],
                            ],
                            'nullable' => false,
                        ],
                        [
                            'name' => 'Name2',
                            'data_type' => [
                                'base' => [
                                    'type' => 'STRING',
                                ],
                            ],
                            'nullable' => false,
                            'primary_key' => true,
                        ],
                    ],
                ],
            ],
            'primary-key-reset' => [
                'mapping' => [
                    'primary_key' => [],
                ],
                'manifest' => [
                    'primary_key' => ['a', 'b'],
                ],
                'expected' => [
                    'primary_key' => [],
                ],
            ],
        ];
    }
}
