<?php

namespace Keboola\InputMapping\Reader;

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
