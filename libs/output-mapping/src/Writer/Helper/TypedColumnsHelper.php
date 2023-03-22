<?php

declare(strict_types=1);

namespace Keboola\OutputMapping\Writer\Helper;

use Keboola\OutputMapping\Writer\Table\TableDefinition\TableDefinitionColumnFactory;
use Keboola\StorageApi\Client;
use Keboola\Utils\Sanitizer\ColumnNameSanitizer;

class TypedColumnsHelper
{
    private static function getMissingColumns(array $currentTableInfo, array $config): array
    {
        $tableColumns = $currentTableInfo['columns'];
        $configColumns = array_map(function ($columnName): string {
            return ColumnNameSanitizer::sanitize($columnName);
        }, array_keys($config['column_metadata']));

        return array_diff($configColumns, $tableColumns);
    }

    public static function addMissingColumnsDecider(array $currentTableInfo, array $config): bool
    {
        if ($currentTableInfo['isTyped'] !== true) {
            return false;
        }

        if (self::getMissingColumns($currentTableInfo, $config)) {
            return true;
        }

        return false;
    }

    public static function addMissingColumns(
        Client $client,
        array  $currentTableInfo,
        array  $config,
        string $backendType,
    ): void {

        $tableMetadata = $config['metadata'] ?? [];

        $missingColumns = self::getMissingColumns($currentTableInfo, $config);

        foreach ($config['column_metadata'] as $columnName => $columnMetadata) {
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
