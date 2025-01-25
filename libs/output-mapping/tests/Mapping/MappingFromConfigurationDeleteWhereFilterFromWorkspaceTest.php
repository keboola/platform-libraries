<?php

declare(strict_types=1);

namespace Keboola\OutputMapping\Tests\Mapping;

use Keboola\OutputMapping\Mapping\MappingFromConfigurationDeleteWhereFilterFromWorkspace;
use PHPUnit\Framework\TestCase;

class MappingFromConfigurationDeleteWhereFilterFromWorkspaceTest extends TestCase
{
    public function testGetters(): void
    {
        $whereFilterFromSet = new MappingFromConfigurationDeleteWhereFilterFromWorkspace(
            [
                'column' => 'columnName',
                'operator' => 'ne',
                'values_from_workspace' => [
                    'workspace_id' => '123',
                    'table' => 'workspaceTable',
                    'column' => 'workspaceColumn',
                ],
            ],
        );

        self::assertSame('columnName', $whereFilterFromSet->getColumn());
        self::assertSame('ne', $whereFilterFromSet->getOperator());
        self::assertSame('123', $whereFilterFromSet->getWorkspaceId());
        self::assertSame('workspaceTable', $whereFilterFromSet->getWorkspaceTable());
        self::assertSame('workspaceColumn', $whereFilterFromSet->getWorkspaceColumn());
    }
}
