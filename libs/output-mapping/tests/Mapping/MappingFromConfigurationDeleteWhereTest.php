<?php

declare(strict_types=1);

namespace Keboola\OutputMapping\Tests\Mapping;

use Keboola\OutputMapping\Exception\InvalidOutputException;
use Keboola\OutputMapping\Mapping\MappingFromConfigurationDeleteWhere;
use Keboola\OutputMapping\Mapping\MappingFromConfigurationDeleteWhereFilterFromSet;
use Keboola\OutputMapping\Mapping\MappingFromConfigurationDeleteWhereFilterFromWorkspace;
use PHPUnit\Framework\TestCase;

class MappingFromConfigurationDeleteWhereTest extends TestCase
{
    public function testEmptyMapping(): void
    {
        $mapping = new MappingFromConfigurationDeleteWhere([]);

        self::assertNull($mapping->getChangedSince());
        self::assertNull($mapping->getChangedUntil());
        self::assertNull($mapping->getWhereFilters());
    }

    public function testMappingWithDates(): void
    {
        $mapping = new MappingFromConfigurationDeleteWhere([
            'changed_since' => '-7 days',
            'changed_until' => '-2 days',
        ]);

        self::assertSame('-7 days', $mapping->getChangedSince());
        self::assertSame('-2 days', $mapping->getChangedUntil());
        self::assertNull($mapping->getWhereFilters());
    }

    public function testMappingWithValuesFromSet(): void
    {
        $mapping = new MappingFromConfigurationDeleteWhere([
            'where_filters' => [
                [
                    'column' => 'columnName',
                    'operator' => 'eq',
                    'values_from_set' => ['value1', 'value2'],
                ],
            ],
        ]);

        $filters = $mapping->getWhereFilters();
        self::assertNotNull($filters);
        self::assertCount(1, $filters);

        $filter = $filters[0];
        self::assertInstanceOf(MappingFromConfigurationDeleteWhereFilterFromSet::class, $filter);
        self::assertWhereFilterFromSet($filter);
    }

    public function testMappingWithValuesFromWorkspace(): void
    {
        $mapping = new MappingFromConfigurationDeleteWhere([
            'where_filters' => [
                [
                    'column' => 'columnName',
                    'operator' => 'ne',
                    'values_from_workspace' => [
                        'workspace_id' => '123',
                        'table' => 'tableInWorkspace',
                        'column' => 'columnInWorkspace',
                    ],
                ],
            ],
        ]);

        $filters = $mapping->getWhereFilters();
        self::assertNotNull($filters);
        self::assertCount(1, $filters);

        $filter = $filters[0];
        self::assertInstanceOf(MappingFromConfigurationDeleteWhereFilterFromWorkspace::class, $filter);
        self::assertWhereFilterFromWorkspace($filter);
    }

    public function testMappingWithMultipleFilters(): void
    {
        $mapping = new MappingFromConfigurationDeleteWhere([
            'where_filters' => [
                [
                    'column' => 'columnName',
                    'operator' => 'eq',
                    'values_from_set' => ['value1', 'value2'],
                ],
                [
                    'column' => 'columnName',
                    'operator' => 'ne',
                    'values_from_workspace' => [
                        'workspace_id' => '123',
                        'table' => 'tableInWorkspace',
                        'column' => 'columnInWorkspace',
                    ],
                ],
            ],
        ]);

        $filters = $mapping->getWhereFilters();
        self::assertNotNull($filters);
        self::assertCount(2, $filters);
        self::assertInstanceOf(MappingFromConfigurationDeleteWhereFilterFromSet::class, $filters[0]);
        self::assertWhereFilterFromSet($filters[0]);
        self::assertInstanceOf(MappingFromConfigurationDeleteWhereFilterFromWorkspace::class, $filters[1]);
        self::assertWhereFilterFromWorkspace($filters[1]);
    }

    public function testInvalidFilterType(): void
    {
        $mapping = new MappingFromConfigurationDeleteWhere([
            'where_filters' => [
                [
                    'column' => 'columnName',
                    'operator' => 'eq',
                ],
            ],
        ]);

        $this->expectException(InvalidOutputException::class);
        $this->expectExceptionMessage('Invalid filter type specified');
        $mapping->getWhereFilters();
    }

    public function testUnsupportedValuesFromStorage(): void
    {
        $mapping = new MappingFromConfigurationDeleteWhere([
            'where_filters' => [
                [
                    'column' => 'columnName',
                    'operator' => 'eq',
                    'values_from_storage' => [
                        'bucket_id' => 'in.c-bucket',
                        'table' => 'tableName',
                        'column' => 'columnName',
                    ],
                ],
            ],
        ]);

        $this->expectException(InvalidOutputException::class);
        $this->expectExceptionMessage('Where filter "values_from_storage" is not yet supported');
        $mapping->getWhereFilters();
    }

    private static function assertWhereFilterFromSet(MappingFromConfigurationDeleteWhereFilterFromSet $filter): void
    {
        self::assertSame('columnName', $filter->getColumn());
        self::assertSame('eq', $filter->getOperator());
        self::assertSame(['value1', 'value2'], $filter->getValues());
    }

    private static function assertWhereFilterFromWorkspace(
        MappingFromConfigurationDeleteWhereFilterFromWorkspace $filter,
    ): void {
        self::assertSame('columnName', $filter->getColumn());
        self::assertSame('ne', $filter->getOperator());
        self::assertSame('123', $filter->getWorkspaceId());
        self::assertSame('tableInWorkspace', $filter->getWorkspaceTable());
        self::assertSame('columnInWorkspace', $filter->getWorkspaceColumn());
    }
}
