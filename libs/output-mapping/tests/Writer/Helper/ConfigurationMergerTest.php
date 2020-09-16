<?php

namespace Keboola\OutputMapping\Tests\Writer\Helper;

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
    public function testMergeConfigurations(array $mapping, array $manifest, array $expected)
    {
        $result = ConfigurationMerger::mergeConfigurations($manifest, $mapping);
        self::assertEquals($expected, $result);
    }

    public function provideConfigurations()
    {
        return [
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
        ];
    }
}
