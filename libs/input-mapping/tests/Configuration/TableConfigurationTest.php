<?php

declare(strict_types=1);

namespace Keboola\InputMapping\Tests\Configuration;

use Keboola\InputMapping\Configuration\Table;
use PHPUnit\Framework\TestCase;
use Symfony\Component\Config\Definition\Exception\InvalidConfigurationException;
use Throwable;

class TableConfigurationTest extends TestCase
{
    public function provideValidConfigs(): array
    {
        return [
            'ComplexConfiguration' => [
                'config' => [
                    'source' => 'in.c-main.test',
                    'destination' => 'test',
                    'changed_since' => '-1 days',
                    'columns' => ['Id', 'Name'],
                    'where_column' => 'status',
                    'where_values' => ['val1', 'val2'],
                    'where_operator' => 'ne',
                ],
                'expected' => [
                    'source' => 'in.c-main.test',
                    'destination' => 'test',
                    'changed_since' => '-1 days',
                    'columns' => ['Id', 'Name'],
                    'where_column' => 'status',
                    'where_values' => ['val1', 'val2'],
                    'where_operator' => 'ne',
                    'column_types' => [],
                    'overwrite' => false,
                    'use_view' => false,
                ],
            ],
            'DaysNullConfiguration' => [
                'config' => [
                    'source' => 'in.c-main.test',
                    'destination' => 'test',
                    'days' => null,
                    'columns' => ['Id', 'Name'],
                    'where_column' => 'status',
                    'where_values' => ['val1', 'val2'],
                    'where_operator' => 'ne',
                ],
                'expected' => [
                    'source' => 'in.c-main.test',
                    'destination' => 'test',
                    'days' => null,
                    'columns' => ['Id', 'Name'],
                    'where_column' => 'status',
                    'where_values' => ['val1', 'val2'],
                    'where_operator' => 'ne',
                    'column_types' => [],
                    'overwrite' => false,
                    'use_view' => false,
                ],
            ],
            'DaysConfiguration' => [
                'config' => [
                    'source' => 'in.c-main.test',
                    'destination' => 'test',
                    'days' => 1,
                    'columns' => ['Id', 'Name'],
                    'where_column' => 'status',
                    'where_values' => ['val1', 'val2'],
                    'where_operator' => 'ne',
                ],
                'expected' => [
                    'source' => 'in.c-main.test',
                    'destination' => 'test',
                    'days' => 1,
                    'columns' => ['Id', 'Name'],
                    'where_column' => 'status',
                    'where_values' => ['val1', 'val2'],
                    'where_operator' => 'ne',
                    'column_types' => [],
                    'overwrite' => false,
                    'use_view' => false,
                ],
            ],
            'ChangedSinceNullConfiguration' => [
                'config' => [
                    'source' => 'in.c-main.test',
                    'destination' => 'test',
                    'changed_since' => null,
                    'columns' => ['Id', 'Name'],
                    'where_column' => 'status',
                    'where_values' => ['val1', 'val2'],
                    'where_operator' => 'ne',
                ],
                'expected' => [
                    'source' => 'in.c-main.test',
                    'destination' => 'test',
                    'changed_since' => null,
                    'columns' => ['Id', 'Name'],
                    'where_column' => 'status',
                    'where_values' => ['val1', 'val2'],
                    'where_operator' => 'ne',
                    'column_types' => [],
                    'overwrite' => false,
                    'use_view' => false,
                ],
            ],
            'ChangedSinceConfiguration' => [
                'config' => [
                    'source' => 'in.c-main.test',
                    'destination' => 'test',
                    'changed_since' => '-1 days',
                    'columns' => ['Id', 'Name'],
                    'where_column' => 'status',
                    'where_values' => ['val1', 'val2'],
                    'where_operator' => 'ne',
                ],
                'expected' => [
                    'source' => 'in.c-main.test',
                    'destination' => 'test',
                    'changed_since' => '-1 days',
                    'columns' => ['Id', 'Name'],
                    'where_column' => 'status',
                    'where_values' => ['val1', 'val2'],
                    'where_operator' => 'ne',
                    'column_types' => [],
                    'overwrite' => false,
                    'use_view' => false,
                ],
            ],
            'SearchSourceConfiguration' => [
                'config' => [
                    'source_search' => [
                        'key' => 'bdm.scaffold.tag',
                        'value' => 'test_table',
                    ],
                    'destination' => 'test',
                    'changed_since' => '-1 days',
                    'columns' => ['Id', 'Name'],
                    'where_column' => 'status',
                    'where_values' => ['val1', 'val2'],
                    'where_operator' => 'ne',
                ],
                'expected' => [
                    'source_search' => [
                        'key' => 'bdm.scaffold.tag',
                        'value' => 'test_table',
                    ],
                    'destination' => 'test',
                    'changed_since' => '-1 days',
                    'columns' => ['Id', 'Name'],
                    'where_column' => 'status',
                    'where_values' => ['val1', 'val2'],
                    'where_operator' => 'ne',
                    'column_types' => [],
                    'overwrite' => false,
                    'use_view' => false,
                ],
            ],
            'DataTypesConfiguration' => [
                'config' => [
                    'source' => 'foo',
                    'destination' => 'bar',
                    'column_types' => [
                        [
                            'source' => 'Id',
                            'type' => 'VARCHAR',
                        ],
                        [
                            'source' => 'Name',
                            'type' => 'VARCHAR',
                        ],
                    ],
                    'where_column' => 'status',
                    'where_values' => ['val1', 'val2'],
                    'where_operator' => 'ne',
                ],
                'expected' => [
                    'source' => 'foo',
                    'destination' => 'bar',
                    'columns' => [],
                    'column_types' => [
                        [
                            'source' => 'Id',
                            'type' => 'VARCHAR',
                        ],
                        [
                            'source' => 'Name',
                            'type' => 'VARCHAR',
                        ],
                    ],
                    'where_column' => 'status',
                    'where_values' => ['val1', 'val2'],
                    'where_operator' => 'ne',
                    'overwrite' => false,
                    'use_view' => false,
                ],
            ],
            'FullDataTypesConfiguration' => [
                'config' => [
                    'source' => 'foo',
                    'destination' => 'bar',
                    'columns' => ['Id'],
                    'column_types' => [
                        [
                            'source' => 'Id',
                            'type' => 'VARCHAR',
                            'destination' => 'MyId',
                            'length' => '10,2',
                            'nullable' => true,
                            'convert_empty_values_to_null' => true,
                            'compression' => 'DELTA32K',
                        ],
                    ],
                ],
                'expected' => [
                    'source' => 'foo',
                    'destination' => 'bar',
                    'columns' => ['Id'],
                    'where_values' => [],
                    'where_operator' => 'eq',
                    'column_types' => [
                        [
                            'source' => 'Id',
                            'type' => 'VARCHAR',
                            'destination' => 'MyId',
                            'length' => '10,2',
                            'nullable' => true,
                            'convert_empty_values_to_null' => true,
                            'compression' => 'DELTA32K',
                        ],
                    ],
                    'overwrite' => false,
                    'use_view' => false,
                ],
            ],
            'FullDataTypesConfigurationEmptyLength' => [
                'config' => [
                    'source' => 'foo',
                    'destination' => 'bar',
                    'columns' => ['Id'],
                    'column_types' => [
                        [
                            'source' => 'Id',
                            'type' => 'VARCHAR',
                            'destination' => 'MyId',
                            'length' => '',
                            'nullable' => true,
                            'convert_empty_values_to_null' => true,
                            'compression' => 'DELTA32K',
                        ],
                    ],
                ],
                'expected' => [
                    'source' => 'foo',
                    'destination' => 'bar',
                    'columns' => ['Id'],
                    'where_values' => [],
                    'where_operator' => 'eq',
                    'column_types' => [
                        [
                            'source' => 'Id',
                            'type' => 'VARCHAR',
                            'destination' => 'MyId',
                            'length' => '',
                            'nullable' => true,
                            'convert_empty_values_to_null' => true,
                            'compression' => 'DELTA32K',
                        ],
                    ],
                    'overwrite' => false,
                    'use_view' => false,
                ],
            ],
            'OverwriteConfiguration' => [
                [
                    'source' => 'in.c-main.test',
                    'overwrite' => true,
                ],
                'expected' => [
                    'source' => 'in.c-main.test',
                    'columns' => [],
                    'where_values' => [],
                    'where_operator' => 'eq',
                    'column_types' => [],
                    'overwrite' => true,
                    'use_view' => false,
                ],
            ],
            'BasicConfiguration' => [
                [
                    'source' => 'in.c-main.test',
                ],
                [
                    'source' => 'in.c-main.test',
                    'columns' => [],
                    'where_values' => [],
                    'where_operator' => 'eq',
                    'column_types' => [],
                    'overwrite' => false,
                    'use_view' => false,
                ],
            ],
        ];
    }

    /**
     * @dataProvider provideValidConfigs
     */
    public function testValidConfigDefinition(
        array $config,
        array $expected
    ): void {
        $processedConfiguration = (new Table())->parse(['config' => $config]);
        self::assertEquals($expected, $processedConfiguration);
    }

    public function provideInvalidConfigs(): array
    {
        return [
            'InvalidWhereOperator' => [
                [
                    'source' => 'in.c-main.test',
                    'where_operator' => 'abc',
                ],
                InvalidConfigurationException::class,
                'Invalid configuration for path "table.where_operator": Invalid operator in where_operator "abc"',
            ],
            'testEmptyConfiguration' => [
                [],
                InvalidConfigurationException::class,
                'Either "source" or "source_search" must be configured.',
            ],
            'EmptySourceConfiguration' => [
                ['source' => ''],
                InvalidConfigurationException::class,
                'The path "table.source" cannot contain an empty value, but got "".',
            ],
            'InvalidSearchSourceEmptyKey' => [
                [
                    'source_search' => [
                        'key' => '',
                        'value' => 'test_table',
                    ],
                ],
                InvalidConfigurationException::class,
                'The path "table.source_search.key" cannot contain an empty value, but got "".',
            ],
            'InvalidSearchSourceEmptyValue' => [
                [
                    'source_search' => [
                        'key' => 'bdm.scaffold.tag',
                        'value' => '',
                    ],
                ],
                InvalidConfigurationException::class,
                'The path "table.source_search.value" cannot contain an empty value, but got "".',
            ],
        ];
    }

    /**
     * @dataProvider provideInvalidConfigs
     * @param class-string<Throwable> $exception
     */
    public function testInvalidConfigDefinition(
        array $config,
        string $exception,
        string $exceptionMessage
    ): void {
        $this->expectException($exception);
        $this->expectExceptionMessage($exceptionMessage);
        (new Table())->parse(['config' => $config]);
    }

    public function testEmptyWhereOperator(): void
    {
        $config = [
            'source' => 'in.c-main.test',
            'where_operator' => '',
        ];

        $expectedArray = $config;
        $expectedArray['where_operator'] = 'eq';
        $expectedArray['columns'] = [];
        $expectedArray['where_values'] = [];
        $expectedArray['column_types'] = [];
        $expectedArray['overwrite'] = false;
        $expectedArray['use_view'] = false;
        $processedConfiguration = (new Table())->parse(['config' => $config]);
        self::assertEquals($expectedArray, $processedConfiguration);
    }
}
