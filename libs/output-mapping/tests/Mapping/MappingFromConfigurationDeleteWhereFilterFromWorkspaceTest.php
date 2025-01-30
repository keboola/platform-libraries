<?php

declare(strict_types=1);

namespace Keboola\OutputMapping\Tests\Mapping;

use Generator;
use Keboola\OutputMapping\Mapping\MappingFromConfigurationDeleteWhereFilterFromWorkspace;
use PHPUnit\Framework\TestCase;

class MappingFromConfigurationDeleteWhereFilterFromWorkspaceTest extends TestCase
{
    public static function configurationProvider(): Generator
    {
        yield 'minimal configuration' => [
            [
                'column' => 'columnName',
                'operator' => 'ne',
                'values_from_workspace' => [
                    'workspace_id' => '123',
                    'table' => 'workspaceTable',
                ],
            ],
            [
                'column' => 'columnName',
                'operator' => 'ne',
                'workspace_id' => '123',
                'table' => 'workspaceTable',
                'workspace_column' => null,
            ],
        ];

        yield 'full configuration' => [
            [
                'column' => 'columnName',
                'operator' => 'ne',
                'values_from_workspace' => [
                    'workspace_id' => '123',
                    'table' => 'workspaceTable',
                    'column' => 'workspaceColumn',
                ],
            ],
            [
                'column' => 'columnName',
                'operator' => 'ne',
                'workspace_id' => '123',
                'table' => 'workspaceTable',
                'workspace_column' => 'workspaceColumn',
            ],
        ];
    }

    /**
     * @dataProvider configurationProvider
     */
    public function testGetters(array $config, array $expected): void
    {
        $whereFilterFromSet = new MappingFromConfigurationDeleteWhereFilterFromWorkspace($config);

        self::assertSame($expected['column'], $whereFilterFromSet->getColumn());
        self::assertSame($expected['operator'], $whereFilterFromSet->getOperator());
        self::assertSame($expected['workspace_id'], $whereFilterFromSet->getWorkspaceId());
        self::assertSame($expected['table'], $whereFilterFromSet->getWorkspaceTable());
        self::assertSame($expected['workspace_column'], $whereFilterFromSet->getWorkspaceColumn());
    }
}
