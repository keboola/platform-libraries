<?php

declare(strict_types=1);

namespace Keboola\OutputMapping\Writer\Helper;

use Keboola\Utils\Sanitizer\ColumnNameSanitizer;

abstract class AbstractColumnsHelper
{
    protected static function getMissingColumns(array $currentTableInfo, array $newTableConfiguration): array
    {
        $tableColumns = $currentTableInfo['columns'];
        $configColumns = array_map(function ($columnName): string {
            return ColumnNameSanitizer::sanitize($columnName);
        }, array_keys($newTableConfiguration['column_metadata']));

        return array_diff($configColumns, $tableColumns);
    }
}
