<?php

namespace Keboola\InputMapping\Helper;

class LoadTypeDecider
{
    public static function canClone(array $tableInfo, $workspaceType, array $exportOptions)
    {
        if (array_keys($exportOptions) !== ['overwrite'] || ($tableInfo['bucket']['backend'] !== $workspaceType) || ($workspaceType !== 'snowflake')) {
            return false;
        }
        return true;
    }
}
