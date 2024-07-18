<?php

declare(strict_types=1);

namespace Keboola\OutputMapping\Storage;

use Keboola\Datatype\Definition\BaseType;
use Keboola\OutputMapping\Exception\PrimaryKeyNotChangedException;
use Keboola\OutputMapping\Mapping\MappingFromProcessedConfiguration;
use Keboola\OutputMapping\Writer\Table\MappingDestination;
use Keboola\OutputMapping\Writer\Table\TableDefinition\TableDefinitionColumnFactory;
use Keboola\Utils\Sanitizer\ColumnNameSanitizer;

class TableStructureModifier extends AbstractTableStructureModifier
{
    public function updateTableStructure(
        BucketInfo $destinationBucket,
        TableInfo $destinationTableInfo,
        MappingFromProcessedConfiguration $source,
        MappingDestination $destination,
    ): void {
        $this->addMissingColumns(
            $destinationTableInfo,
            $source,
            $destinationBucket->backend,
        );

        if ($this->modifyPrimaryKeyDecider($destinationTableInfo->getPrimaryKey(), $source->getPrimaryKey(),)) {
            try {
                $this->modifyPrimaryKey(
                    $destination->getTableId(),
                    $destinationTableInfo->getPrimaryKey(),
                    $source->getPrimaryKey(),
                );
            } catch (PrimaryKeyNotChangedException $e) {
                // ignore
            }
        }
    }

    private function addMissingColumns(
        TableInfo $currentTableInfo,
        MappingFromProcessedConfiguration $newTableConfiguration,
        string $backendType,
    ): void {
        $missingColumns = array_unique(
            array_merge(
                $this->getMissingColumnsFromColumnMetadata(
                    $currentTableInfo->getColumns(),
                    $newTableConfiguration->getColumnMetadata(),
                ),
                $this->getMissingColumnsFromColumns(
                    $currentTableInfo->getColumns(),
                    $newTableConfiguration->getColumns(),
                ),
            ),
        );

        if (!$missingColumns) {
            return;
        }

        $defaultBaseTypeValue = $currentTableInfo->isTyped() === true ? BaseType::STRING : null;
        $missingColumnsData = [];
        if ($currentTableInfo->isTyped() === true) {
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
            $this->client->addTableColumn(
                $currentTableInfo->getId(),
                $columnName,
                $columnDefinition,
                $columnBasetype,
            );
        }
    }

    private function getMissingColumnsFromColumnMetadata(
        array $currentTableColumns,
        array $newTableConfigurationColumnMetadata,
    ): array {
        $configColumns = array_map(function ($columnName): string {
            return ColumnNameSanitizer::sanitize($columnName);
        }, array_keys($newTableConfigurationColumnMetadata));

        return array_udiff($configColumns, $currentTableColumns, 'strcasecmp');
    }

    private function getMissingColumnsFromColumns(
        array $currentTableColumns,
        array $newTableConfigurationColumns,
    ): array {
        $configColumns = array_map(function ($columnName): string {
            return ColumnNameSanitizer::sanitize($columnName);
        }, $newTableConfigurationColumns);

        return array_udiff($configColumns, $currentTableColumns, 'strcasecmp');
    }
}
