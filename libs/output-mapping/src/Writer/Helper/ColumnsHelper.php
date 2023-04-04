<?php

declare(strict_types=1);

namespace Keboola\OutputMapping\Writer\Helper;

use Keboola\OutputMapping\Writer\Table\TableDefinition\TableDefinitionColumnFactory;
use Keboola\StorageApi\Client;
use Keboola\Utils\Sanitizer\ColumnNameSanitizer;

class ColumnsHelper extends AbstractColumnsHelper
{
    public static function addMissingColumns(
        Client $client,
        array $currentTableInfo,
        array $newTableConfiguration
    ): void {
        if ($currentTableInfo['isTyped'] !== false) {
            return;
        }

        $missingColumns = self::getMissingColumns($currentTableInfo, $newTableConfiguration);

        foreach ($missingColumns as $columnName) {
            $client->addTableColumn($currentTableInfo['id'], $columnName);
        }
    }

    protected static function getMissingColumns(array $currentTableInfo, array $newTableConfiguration): array
    {
        $tableColumns = $currentTableInfo['columns'];
        $configColumns = [];

        if (!empty($newTableConfiguration['column_metadata'])) {
            $configColumns = array_map(function ($columnName): string {
                return ColumnNameSanitizer::sanitize($columnName);
            }, array_keys($newTableConfiguration['column_metadata']));
        } elseif (!empty($newTableConfiguration['columns'])) {
            $configColumns = array_map(function ($columnName): string {
                return ColumnNameSanitizer::sanitize($columnName);
            }, $newTableConfiguration['columns']);
        }

        return array_diff($configColumns, $tableColumns);
    }
}
