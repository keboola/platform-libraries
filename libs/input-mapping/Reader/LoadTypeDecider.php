<?php

namespace Keboola\InputMapping\Reader;

class LoadTypeDecider
{
    public static function canClone(array $tableInfo, $workspaceType, array $exportOptions)
    {
        if ($exportOptions || ($tableInfo['bucket']['backend'] !== $workspaceType)) {
            return false;
        }
        return true;
    }
}
