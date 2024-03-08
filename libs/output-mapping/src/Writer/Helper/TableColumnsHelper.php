<?php

declare(strict_types=1);

namespace Keboola\OutputMapping\Writer\Helper;

use Keboola\Datatype\Definition\BaseType;
use Keboola\OutputMapping\Mapping\MappingFromProcessedConfiguration;
use Keboola\OutputMapping\Writer\Table\TableDefinition\TableDefinitionColumnFactory;
use Keboola\StorageApi\Client;
use Keboola\Utils\Sanitizer\ColumnNameSanitizer;

class TableColumnsHelper
{
    public static function addMissingColumns(
        Client $client,
        array $currentTableInfo,
        MappingFromProcessedConfiguration $newTableConfiguration,
        string $backendType,
    ): void {
        $missingColumns = array_unique(
            array_merge(
                self::getMissingColumnsFromColumnMetadata($currentTableInfo, $newTableConfiguration->getColumnMetadata()),
                self::getMissingColumnsFromColumns($currentTableInfo, $newTableConfiguration->getColumns()),
            ),
        );

        if (!$missingColumns) {
            return;
        }

        $defaultBaseTypeValue = $currentTableInfo['isTyped'] === true ? BaseType::STRING : null;
        $missingColumnsData = [];
        if ($currentTableInfo['isTyped'] === true) {
            foreach ($newTableConfiguration->getColumnMetadata() as $columnName => $columnMetadata) {
                $columnName = ColumnNameSanitizer::sanitize($columnName);

                if (!in_array($columnName, $missingColumns, true)) {
                    continue;
                }

                $tableMetadata = $newTableConfiguration->getMetadata();
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
                $columnBasetype,
            );
        }
    }

    private static function getMissingColumnsFromColumnMetadata(
        array $currentTableInfo,
        array $newTableConfigurationColumnMetadata,
    ): array {
        $tableColumns = $currentTableInfo['columns'];

        $configColumns = array_map(function ($columnName): string {
            return ColumnNameSanitizer::sanitize($columnName);
        }, array_keys($newTableConfigurationColumnMetadata));

        return array_udiff($configColumns, $tableColumns, 'strcasecmp');
    }

    private static function getMissingColumnsFromColumns(array $currentTableInfo, array $newTableConfigurationColumns): array
    {
        $tableColumns = $currentTableInfo['columns'];
        $configColumns = array_map(function ($columnName): string {
            return ColumnNameSanitizer::sanitize($columnName);
        }, $newTableConfigurationColumns);

        return array_udiff($configColumns, $tableColumns, 'strcasecmp');
    }
}
