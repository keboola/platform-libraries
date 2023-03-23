<?php

declare(strict_types=1);

namespace Keboola\OutputMapping\Writer\Helper;

use Keboola\OutputMapping\Writer\Table\TableDefinition\TableDefinitionColumnFactory;
use Keboola\StorageApi\Client;
use Keboola\Utils\Sanitizer\ColumnNameSanitizer;

class TypedColumnsHelper
{
    private static function getMissingColumns(array $currentTableInfo, array $newTableConfiguration): array
    {
        $tableColumns = $currentTableInfo['columns'];
        $configColumns = array_map(function ($columnName): string {
            return ColumnNameSanitizer::sanitize($columnName);
        }, array_keys($newTableConfiguration['column_metadata']));

        return array_diff($configColumns, $tableColumns);
    }

    public static function addMissingColumns(
        Client $client,
        array $currentTableInfo,
        array $newTableConfiguration,
        string $backendType,
    ): void {
        if ($currentTableInfo['isTyped'] !== true) {
            return;
        }

        $tableMetadata = $newTableConfiguration['metadata'] ?? [];

        $missingColumns = self::getMissingColumns($currentTableInfo, $newTableConfiguration);

        foreach ($newTableConfiguration['column_metadata'] as $columnName => $columnMetadata) {
            $columnName = ColumnNameSanitizer::sanitize($columnName);

            if (!in_array($columnName, $missingColumns)) {
                continue;
            }

            $tableDefinitionColumnFactory = new TableDefinitionColumnFactory($tableMetadata, $backendType);
            $column = $tableDefinitionColumnFactory->createTableDefinitionColumn($columnName, $columnMetadata);

            $columnData = $column->toArray();
            $client->addTableColumn(
                $currentTableInfo['id'],
                $column->getName(),
                $columnData['definition'] ?? null,
                $columnData['basetype'] ?? null
            );
        }
    }
}
