<?php

declare(strict_types=1);

namespace Keboola\OutputMapping\Writer\Helper;

use Keboola\OutputMapping\Writer\Table\TableDefinition\TableDefinitionColumnFactory;
use Keboola\StorageApi\Client;
use Keboola\Utils\Sanitizer\ColumnNameSanitizer;

class TableColumnsHelper
{
    public static function addMissingColumns(
        Client $client,
        array $currentTableInfo,
        array $newTableConfiguration,
        string $backendType,
    ): void {
        $missingColumnsData = [];
        $missingColumns = self::getMissingColumnsFromColumnMetadata($currentTableInfo, $newTableConfiguration);

        if ($currentTableInfo['isTyped'] === true) {
            foreach ($newTableConfiguration['column_metadata'] as $columnName => $columnMetadata) {
                $columnName = ColumnNameSanitizer::sanitize($columnName);

                if (!in_array($columnName, $missingColumns)) {
                    continue;
                }

                $tableMetadata = $newTableConfiguration['metadata'] ?? [];
                $column = (new TableDefinitionColumnFactory($tableMetadata, $backendType))
                    ->createTableDefinitionColumn($columnName, $columnMetadata);

                $columnData = $column->toArray();
                $missingColumnsData[] = [
                    $column->getName(),
                    $columnData['definition'] ?? null,
                    $columnData['basetype'] ?? null,
                ];
            }
        } else {
            $missingColumns = !$missingColumns
                ? self::getMissingColumnsFromColumns($currentTableInfo, $newTableConfiguration)
                : $missingColumns;

            foreach ($missingColumns as $columnName) {
                $missingColumnsData[] = [
                    $columnName,
                    null,
                    null,
                ];
            }
        }

        foreach ($missingColumnsData as $missingColumnData) {
            [$columnName, $columnDefinition, $columnBasetype] = $missingColumnData;
            $client->addTableColumn(
                $currentTableInfo['id'],
                $columnName,
                $columnDefinition,
                $columnBasetype
            );
        }
    }

    private static function getMissingColumnsFromColumnMetadata(
        array $currentTableInfo,
        array $newTableConfiguration
    ): array {
        $tableColumns = $currentTableInfo['columns'];
        $configColumns = [];

        if (!empty($newTableConfiguration['column_metadata'])) {
            $configColumns = array_map(function ($columnName): string {
                return ColumnNameSanitizer::sanitize($columnName);
            }, array_keys($newTableConfiguration['column_metadata']));
        }

        return array_diff($configColumns, $tableColumns);
    }

    private static function getMissingColumnsFromColumns(array $currentTableInfo, array $newTableConfiguration): array
    {
        $tableColumns = $currentTableInfo['columns'];
        $configColumns = [];

        if (!empty($newTableConfiguration['columns'])) {
            $configColumns = array_map(function ($columnName): string {
                return ColumnNameSanitizer::sanitize($columnName);
            }, $newTableConfiguration['columns']);
        }

        return array_diff($configColumns, $tableColumns);
    }
}
