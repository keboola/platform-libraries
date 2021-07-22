<?php

namespace Keboola\InputMapping\Helper;

class LoadTypeDecider
{
    public static function canClone(array $tableInfo, $workspaceType, array $exportOptions)
    {
        if ($tableInfo['isAlias'] && (empty($tableInfo['aliasColumnsAutoSync']) || !empty($tableInfo['aliasFilter']))) {
            return false;
        }

        if (array_keys($exportOptions) !== ['overwrite'] || ($tableInfo['bucket']['backend'] !== $workspaceType) || ($workspaceType !== 'snowflake')) {
            return false;
        }
        return true;
    }
}
