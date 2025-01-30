<?php

declare(strict_types=1);

namespace Keboola\OutputMapping\Storage;

use Keboola\OutputMapping\Mapping\MappingFromConfigurationDeleteWhere;
use Keboola\OutputMapping\Mapping\MappingFromConfigurationDeleteWhereFilterFromSet;
use Keboola\OutputMapping\Mapping\MappingFromConfigurationDeleteWhereFilterFromWorkspace;

class DeleteTableRowsOptionsFactory
{
    public static function createFromLegacyDeleteWhereColumn(
        string $column,
        string $operator,
        array $values,
    ): array {
        return [
            'whereColumn' => $column,
            'whereOperator' => $operator,
            'whereValues' => $values,
        ];
    }

    public static function createFromDeleteWhere(
        MappingFromConfigurationDeleteWhere $deleteWhere,
    ): ?array {
        $options = [];

        if ($deleteWhere->getChangedSince()) {
            $options['changedSince'] = $deleteWhere->getChangedSince();
        }

        if ($deleteWhere->getChangedUntil()) {
            $options['changedUntil'] = $deleteWhere->getChangedUntil();
        }

        $whereFilters = self::createWhereFilters($deleteWhere);
        if ($whereFilters !== []) {
            $options['whereFilters'] = $whereFilters;
        }

        return $options === [] ? null : $options;
    }

    private static function createWhereFilters(MappingFromConfigurationDeleteWhere $deleteWhere): array
    {
        if (!$deleteWhere->getWhereFilters()) {
            return [];
        }

        $whereFilters = [];
        foreach ($deleteWhere->getWhereFilters() as $deleteFilter) {
            $whereFilter = [
                'column' => $deleteFilter->getColumn(),
                'operator' => $deleteFilter->getOperator(),
            ];

            if ($deleteFilter instanceof MappingFromConfigurationDeleteWhereFilterFromSet) {
                $whereFilter['values'] = $deleteFilter->getValues();
                $whereFilters[] = $whereFilter;
            }
            if ($deleteFilter instanceof MappingFromConfigurationDeleteWhereFilterFromWorkspace) {
                $whereFilter['valuesByTableInWorkspace'] = [
                    'workspaceId' => $deleteFilter->getWorkspaceId(),
                    'table' => $deleteFilter->getWorkspaceTable(),
                    'column' => $deleteFilter->getWorkspaceColumn() ?: $deleteFilter->getColumn(),
                ];
                $whereFilters[] = $whereFilter;
            }
        }

        return $whereFilters;
    }
}
