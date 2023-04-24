<?php

declare(strict_types=1);

namespace Keboola\OutputMapping\Writer\Helper;

use Keboola\Datatype\Definition\BaseType;
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
        $missingColumns = array_unique(
            array_merge(
                self::getMissingColumnsFromColumnMetadata($currentTableInfo, $newTableConfiguration),
                self::getMissingColumnsFromColumns($currentTableInfo, $newTableConfiguration)
            )
        );

        if (!$missingColumns) {
            return;
        }

        $defaultBaseTypeValue = $currentTableInfo['isTyped'] === true ? BaseType::STRING : null;
        $missingColumnsData = [];
        if (!empty($newTableConfiguration['column_metadata']) && $currentTableInfo['isTyped'] === true) {
            foreach ($newTableConfiguration['column_metadata'] as $columnName => $columnMetadata) {
                $columnName = ColumnNameSanitizer::sanitize($columnName);

                if (!in_array($columnName, $missingColumns, true)) {
                    continue;
                }

                $tableMetadata = $newTableConfiguration['metadata'] ?? [];
                $column = (new TableDefinitionColumnFactory($tableMetadata, $backendType))
                    ->createTableDefinitionColumn($columnName, $columnMetadata);

                $columnData = $column->toArray();
                $missingColumnsData[] = [
                    $column->getName(),
                    $columnData['definition'] ?? null,
                    $columnData['basetype'] ?? ($columnData['definition'] ? null : $defaultBaseTypeValue),
                ];

                $missingColumns = array_diff($missingColumns, [$column->getName()]);
            }
        }

        foreach ($missingColumns as $columnName) {
            $missingColumnsData[] = [
                $columnName,
                null,
                $defaultBaseTypeValue,
            ];
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
            $configColumns = array_map(
                function ($columnName): string {
                    return ColumnNameSanitizer::sanitize($columnName);
                },
                self::filterUnsupportedColumns(
                    array_keys($newTableConfiguration['column_metadata'])
                )
            );
        }

        return array_diff($configColumns, $tableColumns);
    }

    private static function filterUnsupportedColumns(array $columns): array
    {
        return array_filter($columns, function (string $column) {
            return mb_strtolower($column) !== '_timestamp';
        });
    }

    private static function getMissingColumnsFromColumns(array $currentTableInfo, array $newTableConfiguration): array
    {
        $tableColumns = $currentTableInfo['columns'];
        $configColumns = [];

        if (!empty($newTableConfiguration['columns'])) {
            $configColumns = array_map(
                function ($columnName): string {
                    return ColumnNameSanitizer::sanitize($columnName);
                },
                self::filterUnsupportedColumns($newTableConfiguration['columns'])
            );
        }

        return array_diff($configColumns, $tableColumns);
    }
}
