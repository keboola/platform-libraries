<?php

declare(strict_types=1);

namespace Keboola\OutputMapping\Tests\Storage;

use Generator;
use Keboola\OutputMapping\Mapping\MappingFromConfigurationDeleteWhere;
use Keboola\OutputMapping\Storage\DeleteTableRowsOptionsFactory;
use PHPUnit\Framework\TestCase;

class DeleteTableRowsOptionsFactoryTest extends TestCase
{
    public function testCreateFromLegacyDeleteWhereColumn(): void
    {
        self::assertSame(
            [
                'whereColumn' => 'test_column',
                'whereOperator' => 'eq',
                'whereValues' => ['value1', 'value2'],
            ],
            DeleteTableRowsOptionsFactory::createFromLegacyDeleteWhereColumn(
                'test_column',
                'eq',
                ['value1', 'value2'],
            ),
        );
    }

    /**
     * @dataProvider provideCreateFromDeleteWhereData
     */
    public function testCreateFromDeleteWhere(
        array $mapping,
        ?array $expectedResult,
    ): void {
        $deleteWhere = new MappingFromConfigurationDeleteWhere($mapping);
        $result = DeleteTableRowsOptionsFactory::createFromDeleteWhere($deleteWhere);

        $this->assertEquals($expectedResult, $result);
    }

    public static function provideCreateFromDeleteWhereData(): Generator
    {
        yield 'empty mapping returns null' => [
            'mapping' => [],
            'expectedResult' => null,
        ];

        yield 'mapping with changed_since only' => [
            'mapping' => [
                'changed_since' => '2024-01-01',
            ],
            'expectedResult' => [
                'changedSince' => '2024-01-01',
            ],
        ];

        yield 'mapping with changed_until only' => [
            'mapping' => [
                'changed_until' => '2024-01-02',
            ],
            'expectedResult' => [
                'changedUntil' => '2024-01-02',
            ],
        ];

        yield 'mapping with values_from_set filter' => [
            'mapping' => [
                'where_filters' => [
                    [
                        'column' => 'test_column',
                        'operator' => 'eq',
                        'values_from_set' => ['value1', 'value2'],
                    ],
                ],
            ],
            'expectedResult' => [
                'whereFilters' => [
                    [
                        'column' => 'test_column',
                        'operator' => 'eq',
                        'values' => ['value1', 'value2'],
                    ],
                ],
            ],
        ];

        yield 'mapping with values_from_workspace filter' => [
            'mapping' => [
                'where_filters' => [
                    [
                        'column' => 'test_column',
                        'operator' => 'ne',
                        'values_from_workspace' => [
                            'workspace_id' => 'workspace123',
                            'table' => 'table1',
                            'column' => 'column1',
                        ],
                    ],
                ],
            ],
            'expectedResult' => [
                'whereFilters' => [
                    [
                        'column' => 'test_column',
                        'operator' => 'ne',
                        'valuesByTableInWorkspace' => [
                            'workspaceId' => 'workspace123',
                            'table' => 'table1',
                            'column' => 'column1',
                        ],
                    ],
                ],
            ],
        ];

        yield 'mapping with values_from_workspace filter - use column from filter' => [
            'mapping' => [
                'where_filters' => [
                    [
                        'column' => 'test_column',
                        'operator' => 'ne',
                        'values_from_workspace' => [
                            'workspace_id' => 'workspace123',
                            'table' => 'table1',
                        ],
                    ],
                ],
            ],
            'expectedResult' => [
                'whereFilters' => [
                    [
                        'column' => 'test_column',
                        'operator' => 'ne',
                        'valuesByTableInWorkspace' => [
                            'workspaceId' => 'workspace123',
                            'table' => 'table1',
                            'column' => 'test_column',
                        ],
                    ],
                ],
            ],
        ];

        yield 'mapping with multiple filters and dates' => [
            'mapping' => [
                'changed_since' => '2024-01-01',
                'changed_until' => '2024-01-02',
                'where_filters' => [
                    [
                        'column' => 'test_column',
                        'operator' => 'eq',
                        'values_from_set' => ['value1', 'value2'],
                    ],
                    [
                        'column' => 'another_column',
                        'operator' => 'ne',
                        'values_from_workspace' => [
                            'workspace_id' => 'workspace123',
                            'table' => 'table1',
                            'column' => 'column1',
                        ],
                    ],
                ],
            ],
            'expectedResult' => [
                'changedSince' => '2024-01-01',
                'changedUntil' => '2024-01-02',
                'whereFilters' => [
                    [
                        'column' => 'test_column',
                        'operator' => 'eq',
                        'values' => ['value1', 'value2'],
                    ],
                    [
                        'column' => 'another_column',
                        'operator' => 'ne',
                        'valuesByTableInWorkspace' => [
                            'workspaceId' => 'workspace123',
                            'table' => 'table1',
                            'column' => 'column1',
                        ],
                    ],
                ],
            ],
        ];
    }
}
