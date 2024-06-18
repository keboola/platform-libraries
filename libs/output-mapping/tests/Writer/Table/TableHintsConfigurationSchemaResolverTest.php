<?php

declare(strict_types=1);

namespace Keboola\OutputMapping\Tests\Writer\Table;

use Generator;
use Keboola\OutputMapping\Writer\Table\TableHintsConfigurationSchemaResolver;
use PHPUnit\Framework\TestCase;

class TableHintsConfigurationSchemaResolverTest extends TestCase
{
    /** @dataProvider configDataProvider */
    public function testResolveSchemaConfiguration(array $config, array $expectedConfig): void
    {
        $resolver = new TableHintsConfigurationSchemaResolver();
        self::assertEquals($expectedConfig, $resolver->resolveColumnsConfiguration($config));
    }

    public function configDataProvider(): Generator
    {
        yield 'empty config' => [
            [],
            [],
        ];

        yield 'config with data type' => [
            [
                'schema' => [
                    [
                        'name' => 'col1',
                        'data_type' => [
                            'base' => [
                                'type' => 'VARCHAR',
                                'length' => '255',
                                'default' => 'default',
                            ],
                        ],
                    ],
                ],
            ],
            [
                'schema' => [
                    [
                        'name' => 'col1',
                        'metadata' => [
                            'KBC.datatype.basetype' => 'VARCHAR',
                            'KBC.datatype.length' => '255',
                            'KBC.datatype.default' => 'default',
                        ],
                    ],
                ],
            ],
        ];

        yield 'config with nullable' => [
            [
                'schema' => [
                    [
                        'name' => 'col1',
                        'nullable' => true,
                    ],
                ],
            ],
            [
                'schema' => [
                    [
                        'name' => 'col1',
                        'metadata' => [
                            'KBC.datatype.nullable' => 1,
                        ],
                    ],
                ],
            ],
        ];

        yield 'config with primary key' => [
            [
                'schema' => [
                    [
                        'name' => 'col1',
                        'primary_key' => true,
                    ],
                ],
            ],
            [
                'schema' => [
                    [
                        'name' => 'col1',
                        'primary_key' => true,
                    ],
                ],
            ],
        ];

        yield 'full config' => [
            [
                'schema' => [
                    [
                        'name' => 'col1',
                        'data_type' => [
                            'base' => [
                                'type' => 'VARCHAR',
                                'length' => '255',
                                'default' => 'default',
                            ],
                        ],
                        'nullable' => true,
                        'primary_key' => true,
                        'distribution_key' => true,
                        'description' => 'description value',
                        'metadata' => [
                            'KBC.metadata1' => 'metadata1 value',
                        ],
                    ],
                ],
            ],
            [
                'schema' => [
                    [
                        'name' => 'col1',
                        'metadata' => [
                            'KBC.datatype.basetype' => 'VARCHAR',
                            'KBC.datatype.length' => '255',
                            'KBC.datatype.default' => 'default',
                            'KBC.datatype.nullable' => 1,
                            'KBC.metadata1' => 'metadata1 value',
                            'KBC.datatype.distribution_key' => 1,
                        ],
                        'primary_key' => true,
                        'description' => 'description value',
                    ],
                ],
            ],
        ];
    }
}
