<?php

declare(strict_types=1);

namespace Keboola\InputMapping\Helper;

class LoadTypeDecider
{
    public static function canClone(array $tableInfo, string $workspaceType, array $exportOptions): bool
    {
        if ($tableInfo['isAlias'] && (empty($tableInfo['aliasColumnsAutoSync']) || !empty($tableInfo['aliasFilter']))) {
            return false;
        }

        if (array_keys($exportOptions) !== ['overwrite'] ||
            ($tableInfo['bucket']['backend'] !== $workspaceType) ||
            ($workspaceType !== 'snowflake')
        ) {
            return false;
        }
        return true;
    }
}
