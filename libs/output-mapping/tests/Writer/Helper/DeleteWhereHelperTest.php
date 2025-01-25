<?php

declare(strict_types=1);

namespace Keboola\OutputMapping\Tests\Writer\Helper;

use Generator;
use Keboola\OutputMapping\Writer\Helper\DeleteWhereHelper;
use PHPUnit\Framework\TestCase;

class DeleteWhereHelperTest extends TestCase
{
    public static function provideDeleteWhereMappings(): Generator
    {
        yield 'empty' => [
            'deleteWhere' => [],
            'expectedResult' => [],
        ];

        yield 'no where_filters' => [
            'deleteWhere' => [
                'changed_since' => '2021-01-01',
            ],
            'expectedResult' => [
                'changed_since' => '2021-01-01',
            ],
        ];

        yield 'invalid where_filters' => [
            'deleteWhere' => [
                'where_filters' => 'invalid',
            ],
            'expectedResult' => [
                'where_filters' => 'invalid',
            ],
        ];

        yield 'empty where_filters' => [
            'deleteWhere' => [
                'where_filters' => [],
            ],
            'expectedResult' => [
                'where_filters' => [],
            ],
        ];

        yield 'where_filters with values_from_set' => [
            'deleteWhere' => [
                'where_filters' => [
                    [
                        'column' => 'id',
                        'operator' => 'eq',
                        'values_from_set' => ['123'],
                    ],
                ],
            ],
            'expectedResult' => [
                'where_filters' => [
                    [
                        'column' => 'id',
                        'operator' => 'eq',
                        'values_from_set' => ['123'],
                    ],
                ],
            ],
        ];

        yield 'values_from_workspace with existing workspace' => [
            'deleteWhere' => [
                'where_filters' => [
                    [
                        'column' => 'id',
                        'operator' => 'eq',
                        'values_from_workspace' => [
                            'workspace_id' => 'existing-workspace',
                            'table' => 'tableName',
                        ],
                    ],
                ],
            ],
            'expectedResult' => [
                'where_filters' => [
                    [
                        'column' => 'id',
                        'operator' => 'eq',
                        'values_from_workspace' => [
                            'workspace_id' => 'existing-workspace',
                            'table' => 'tableName',
                        ],
                    ],
                ],
            ],
        ];

        yield 'values_from_workspace without workspace_id' => [
            'deleteWhere' => [
                'where_filters' => [
                    [
                        'column' => 'id',
                        'operator' => 'eq',
                        'values_from_workspace' => [
                            'table' => 'tableName',
                        ],
                    ],
                ],
            ],
            'expectedResult' => [
                'where_filters' => [
                    [
                        'column' => 'id',
                        'operator' => 'eq',
                        'values_from_workspace' => [
                            'table' => 'tableName',
                            'workspace_id' => '123',
                        ],
                    ],
                ],
            ],
        ];

        yield 'mixed where_filters' => [
            'deleteWhere' => [
                'where_filters' => [
                    [
                        'column' => 'id',
                        'operator' => 'eq',
                        'values_from_workspace' => [
                            'workspace_id' => 'existing-workspace',
                            'table' => 'tableName',
                        ],
                    ],
                    [
                        'column' => 'id',
                        'operator' => 'eq',
                        'values_from_workspace' => [
                            'table' => 'tableName',
                        ],
                    ],
                ],
            ],
            'expectedResult' => [
                'where_filters' => [
                    [
                        'column' => 'id',
                        'operator' => 'eq',
                        'values_from_workspace' => [
                            'workspace_id' => 'existing-workspace',
                            'table' => 'tableName',
                        ],
                    ],
                    [
                        'column' => 'id',
                        'operator' => 'eq',
                        'values_from_workspace' => [
                            'table' => 'tableName',
                            'workspace_id' => '123',
                        ],
                    ],
                ],
            ],
        ];
    }

    /**
     * @dataProvider provideDeleteWhereMappings
     */
    public function testAddWorkspaceIdToValuesFromWorkspaceIfMissing(array $deleteWhere, array $expectedResult): void
    {
        $result = DeleteWhereHelper::addWorkspaceIdToValuesFromWorkspaceIfMissing($deleteWhere, '123');

        self::assertSame($expectedResult, $result);
    }
}
