<?php

declare(strict_types=1);

namespace Keboola\OutputMapping\Tests\Writer\Helper;

use Generator;
use Keboola\OutputMapping\Exception\InvalidOutputException;
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

        yield 'config with restricted columns and restricted columns metadata' => [
            'config' => [
                'destination' => 'in.c-myBucket.myTable',
                'columns' => [
                    'id',
                    'name',
                    '_timestamp',
                    '_TIMESTAMP', // to be sure that filter is not case-sensitive
                ],
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
                'schema' => [
                    [
                        'name' => 'id',
                    ],
                    [
                        'name' => 'name',
                    ],
                    [
                        'name' => '_timestamp',
                    ],
                    [
                        'name' => '_TIMESTAMP',
                    ],
                ],
            ],
            'expectedResult' => [
                'destination' => 'in.c-myBucket.myTable',
                'columns' => [
                    'id',
                    'name',
                ],
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
                'schema' => [
                    [
                        'name' => 'id',
                    ],
                    [
                        'name' => 'name',
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

    public function validateRestrictedColumnsInConfigProvider(): Generator
    {
        yield 'empty config' => [
            [],
        ];

        yield 'non-empty config' => [
            [
                'destination' => 'in.c-myBucket.myTable',
            ],
        ];

        yield 'config with columns' => [
            [
                'destination' => 'in.c-myBucket.myTable',
                'columns' => [
                    'id',
                    'name',
                ],
            ],
        ];

        yield 'config with columns in metadata' => [
            [
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

        yield 'config with columns in schema' => [
            [
                'destination' => 'in.c-myBucket.myTable',
                'schema' => [
                    [
                        'name' => 'id',
                    ],
                    [
                        'name' => 'name',
                    ],
                ],
            ],
        ];
    }

    /**
     * @dataProvider validateRestrictedColumnsInConfigProvider
     */
    public function testValidateRestrictedColumnsInConfig(array $config): void
    {
        $this->expectNotToPerformAssertions();
        RestrictedColumnsHelper::validateRestrictedColumnsInConfig(
            $config['columns'] ?? [],
            $config['column_metadata'] ?? [],
            $config['schema'] ?? [],
        );
    }

    public function validateRestrictedColumnsInConfigThrowsErrorProvider(): Generator
    {
        yield 'config with restricted columns' => [
            'config' => [
                'columns' => [
                    'id',
                    'name',
                    '_timestamp',
                    '_TIMESTAMP', // to be sure that validation is not case-sensitive
                ],
            ],
            'expectedErrorMessage' => 'System columns "_timestamp, _TIMESTAMP" cannot be imported to the table.',
        ];

        yield 'config with restricted columns in metadata' => [
            'config' => [
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
                    // to be sure that validation is not case-sensitive
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
            'expectedErrorMessage' => 'Metadata for system columns "_timestamp, _TIMESTAMP" '
                . 'cannot be imported to the table.',
        ];

        yield 'config with restricted columns and restricted columns metadata' => [
            'config' => [
                'columns' => [
                    'id',
                    'name',
                    '_timestamp',
                    '_TIMESTAMP', // to be sure that validation is not case-sensitive
                ],
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
                    // to be sure that validation is not case-sensitive
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
            'expectedErrorMessage' => 'System columns "_timestamp, _TIMESTAMP" cannot be imported to the table. '
                . 'Metadata for system columns "_timestamp, _TIMESTAMP" cannot be imported to the table.',
        ];
    }

    /**
     * @dataProvider validateRestrictedColumnsInConfigThrowsErrorProvider
     */
    public function testValidateRestrictedColumnsInConfigThrowsError(array $config, string $expectedErrorMessage): void
    {
        $this->expectExceptionCode(0);
        $this->expectException(InvalidOutputException::class);
        $this->expectExceptionMessage($expectedErrorMessage);
        RestrictedColumnsHelper::validateRestrictedColumnsInConfig(
            $config['columns'] ?? [],
            $config['column_metadata'] ?? [],
            $config['schema'] ?? [],
        );
    }

    public function isRestrictedColumnProvider(): Generator
    {
        yield 'id' => [
            'columnName' => 'id',
            'expectedResult' => false,
        ];
        yield 'timestamp' => [
            'columnName' => 'timestamp',
            'expectedResult' => false,
        ];
        yield 'restricted timestamp with udnerscore' => [
            'columnName' => '_timestamp',
            'expectedResult' => true,
        ];
        yield 'restricted timestamp with udnerscore - case insensitive' => [
            'columnName' => '_TimeStamp',
            'expectedResult' => true,
        ];
    }

    /**
     * @dataProvider isRestrictedColumnProvider
     */
    public function testIsRestrictedColumn(string $columName, bool $expectedResult): void
    {
        self::assertSame($expectedResult, RestrictedColumnsHelper::isRestrictedColumn($columName));
    }
}
