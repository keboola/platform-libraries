<?php

declare(strict_types=1);

namespace Keboola\OutputMapping\Writer\Helper;

class DeleteWhereHelper
{
    private static function hasWhereFilters(array $deleteWhere): bool
    {
        return isset($deleteWhere['where_filters']) && is_array($deleteWhere['where_filters']);
    }

    private static function isSourceWorkspaceIdNeeded(array $whereFilter): bool
    {
        return isset($whereFilter['values_from_workspace'])
            && empty($whereFilter['values_from_workspace']['workspace_id']);
    }

    public static function addWorkspaceIdToValuesFromWorkspaceIfMissing(array $deleteWhere, string $workspaceId): array
    {
        if (self::hasWhereFilters($deleteWhere)) {
            $deleteWhere['where_filters'] = array_map(
                function (array $whereFilter) use ($workspaceId): array {
                    if (self::isSourceWorkspaceIdNeeded($whereFilter)) {
                        $whereFilter['values_from_workspace']['workspace_id'] = $workspaceId;
                    }
                    return $whereFilter;
                },
                $deleteWhere['where_filters'],
            );
        }

        return $deleteWhere;
    }
}
