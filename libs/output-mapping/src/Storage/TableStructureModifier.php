<?php

declare(strict_types=1);

namespace Keboola\OutputMapping\Storage;

use Keboola\Datatype\Definition\BaseType;
use Keboola\OutputMapping\Exception\PrimaryKeyNotChangedException;
use Keboola\OutputMapping\Mapping\MappingColumnMetadata;
use Keboola\OutputMapping\Mapping\MappingFromProcessedConfiguration;
use Keboola\OutputMapping\Writer\Helper\PrimaryKeyHelper;
use Keboola\OutputMapping\Writer\Table\MappingDestination;
use Keboola\OutputMapping\Writer\Table\TableDefinition\TableDefinitionColumnFactory;

class TableStructureModifier extends AbstractTableStructureModifier
{
    public function updateTableStructure(
        BucketInfo $destinationBucket,
        TableInfo $destinationTableInfo,
        MappingFromProcessedConfiguration $source,
        MappingDestination $destination,
        bool $enforceBaseTypes,
    ): void {
        $this->addMissingColumns(
            $destinationTableInfo,
            $source,
            $destinationBucket->backend,
            $enforceBaseTypes,
        );

        if (PrimaryKeyHelper::modifyPrimaryKeyDecider(
            $this->logger,
            $destinationTableInfo->getPrimaryKey(),
            $source->getPrimaryKey(),
        )) {
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
        bool $enforceBaseTypes,
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
            foreach ($newTableConfiguration->getColumnMetadata() as $columnMetadata) {
                if (!in_array($columnMetadata->getColumnName(), $missingColumns, true)) {
                    continue;
                }

                $tableMetadata = $newTableConfiguration->getMetadata();
                $column = (new TableDefinitionColumnFactory($tableMetadata, $backendType, $enforceBaseTypes))
                    ->createTableDefinitionColumn($columnMetadata->getColumnName(), $columnMetadata->getMetadata());

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
        return array_udiff(
            array_map(function (MappingColumnMetadata $columnMetadata): string {
                return $columnMetadata->getColumnName();
            }, $newTableConfigurationColumnMetadata),
            $currentTableColumns,
            'strcasecmp',
        );
    }

    private function getMissingColumnsFromColumns(
        array $currentTableColumns,
        array $newTableConfigurationColumns,
    ): array {
        return array_udiff($newTableConfigurationColumns, $currentTableColumns, 'strcasecmp');
    }
}
