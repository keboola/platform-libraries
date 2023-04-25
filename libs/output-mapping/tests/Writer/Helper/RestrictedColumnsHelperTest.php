<?php

declare(strict_types=1);

namespace Keboola\OutputMapping\Tests\Writer\Helper;

use Generator;
use Keboola\OutputMapping\Writer\Helper\RestrictedColumnsHelper;
use PHPUnit\Framework\TestCase;

class RestrictedColumnsHelperTest extends TestCase
{
    public function removeRestrictedColumnsFromConfigProvider(): Generator
    {
        yield 'empty config' => [
            'config' => [],
            'expectedResult' => [],
        ];

        yield 'non-empty config' => [
            'config' => [
                'destination' => 'in.c-myBucket.myTable',
            ],
            'expectedResult' => [
                'destination' => 'in.c-myBucket.myTable',
            ],
        ];

        yield 'config with columns' => [
            'config' => [
                'destination' => 'in.c-myBucket.myTable',
                'columns' => [
                    'id',
                    'name',
                ],
            ],
            'expectedResult' => [
                'destination' => 'in.c-myBucket.myTable',
                'columns' => [
                    'id',
                    'name',
                ],
            ],
        ];

        yield 'config with restricted columns' => [
            'config' => [
                'destination' => 'in.c-myBucket.myTable',
                'columns' => [
                    'id',
                    'name',
                    '_timestamp',
                    '_TIMESTAMP', // to be sure that filter is not case-sensitive
                ],
            ],
            'expectedResult' => [
                'destination' => 'in.c-myBucket.myTable',
                'columns' => [
                    'id',
                    'name',
                ],
            ],
        ];

        yield 'config with columns in metadata' => [
            'config' => [
                'destination' => 'in.c-myBucket.myTable',
                'column_metadata' => [
                    'id' => [
                        [
                            'key' => 'KBC.datatype.type',
                            'value' => 'INT',
                        ],
                        [
                            'key' => 'KBC.datatype.basetype',
                            'value' => 'INTEGER',
                        ],
                    ],
                    'name' => [
                        [
                            'key' => 'KBC.datatype.type',
                            'value' => 'VARCHAR',
                        ],
                        [
                            'key' => 'KBC.datatype.basetype',
                            'value' => 'STRING',
                        ],
                    ],
                ],
            ],
            'expectedResult' => [
                'destination' => 'in.c-myBucket.myTable',
                'column_metadata' => [
                    'id' => [
                        [
                            'key' => 'KBC.datatype.type',
                            'value' => 'INT',
                        ],
                        [
                            'key' => 'KBC.datatype.basetype',
                            'value' => 'INTEGER',
                        ],
                    ],
                    'name' => [
                        [
                            'key' => 'KBC.datatype.type',
                            'value' => 'VARCHAR',
                        ],
                        [
                            'key' => 'KBC.datatype.basetype',
                            'value' => 'STRING',
                        ],
                    ],
                ],
            ],
        ];
        yield 'config with restricted columns in metadata' => [
            'config' => [
                'destination' => 'in.c-myBucket.myTable',
                'column_metadata' => [
                    'id' => [
                        [
                            'key' => 'KBC.datatype.type',
                            'value' => 'INT',
                        ],
                        [
                            'key' => 'KBC.datatype.basetype',
                            'value' => 'INTEGER',
                        ],
                    ],
                    'name' => [
                        [
                            'key' => 'KBC.datatype.type',
                            'value' => 'VARCHAR',
                        ],
                        [
                            'key' => 'KBC.datatype.basetype',
                            'value' => 'STRING',
                        ],
                    ],
                    '_timestamp' => [
                        [
                            'key' => 'KBC.datatype.type',
                            'value' => 'TIMESTAMP_NTZ',
                        ],
                        [
                            'key' => 'KBC.datatype.basetype',
                            'value' => 'TIMESTAMP',
                        ],
                    ],
                    // to be sure that filter is not case-sensitive
                    '_TIMESTAMP' => [
                        [
                            'key' => 'KBC.datatype.type',
                            'value' => 'TIMESTAMP_NTZ',
                        ],
                        [
                            'key' => 'KBC.datatype.basetype',
                            'value' => 'TIMESTAMP',
                        ],
                    ],
                ],
            ],
            'expectedResult' => [
                'destination' => 'in.c-myBucket.myTable',
                'column_metadata' => [
                    'id' => [
                        [
                            'key' => 'KBC.datatype.type',
                            'value' => 'INT',
                        ],
                        [
                            'key' => 'KBC.datatype.basetype',
                            'value' => 'INTEGER',
                        ],
                    ],
                    'name' => [
                        [
                            'key' => 'KBC.datatype.type',
                            'value' => 'VARCHAR',
                        ],
                        [
                            'key' => 'KBC.datatype.basetype',
                            'value' => 'STRING',
                        ],
                    ],
                ],
            ],
        ];
    }

    /**
     * @dataProvider removeRestrictedColumnsFromConfigProvider
     */
    public function testRemoveRestrictedColumnsFromConfig(array $config, array $expectedResult): void
    {
        self::assertSame($expectedResult, RestrictedColumnsHelper::removeRestrictedColumnsFromConfig($config));
    }
}
