<?php

declare(strict_types=1);

namespace Keboola\OutputMapping\Storage;

use Keboola\Datatype\Definition\BaseType;
use Keboola\OutputMapping\Mapping\MappingFromProcessedConfiguration;
use Keboola\OutputMapping\Writer\Table\MappingDestination;
use Keboola\OutputMapping\Writer\Table\TableDefinition\TableDefinitionColumnFactory;
use Keboola\StorageApi\Client;
use Keboola\StorageApiBranch\ClientWrapper;
use Keboola\Utils\Sanitizer\ColumnNameSanitizer;
use Psr\Log\LoggerInterface;
use Throwable;

class TableStructureModifier
{
    public function __construct(
        private readonly ClientWrapper $clientWrapper,
        private readonly LoggerInterface $logger,
    ) {
    }

    public function updateTableStructure(
        BucketInfo $destinationBucket,
        TableInfo $destinationTableInfo,
        MappingFromProcessedConfiguration $source,
        MappingDestination $destination,
    ): void {
        $this->addMissingColumns(
            $this->clientWrapper->getTableAndFileStorageClient(),
            $destinationTableInfo,
            $source,
            $destinationBucket->backend,
        );

        if ($this->modifyPrimaryKeyDecider($this->logger, $destinationTableInfo->getPrimaryKey(), $source->getPrimaryKey())) {
            $this->modifyPrimaryKey(
                $this->logger,
                $this->clientWrapper->getTableAndFileStorageClient(),
                $destination->getTableId(),
                $destinationTableInfo->getPrimaryKey(),
                $source->getPrimaryKey(),
            );
        }
    }

    private function addMissingColumns(
        Client $client,
        TableInfo $currentTableInfo,
        MappingFromProcessedConfiguration $newTableConfiguration,
        string $backendType,
    ): void {
        $missingColumns = array_unique(
            array_merge(
                $this->getMissingColumnsFromColumnMetadata($currentTableInfo->getColumns(), $newTableConfiguration->getColumnMetadata()),
                $this->getMissingColumnsFromColumns($currentTableInfo->getColumns(), $newTableConfiguration->getColumns()),
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
            $client->addTableColumn(
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

    private function getMissingColumnsFromColumns(array $currentTableColumns, array $newTableConfigurationColumns): array
    {
        $configColumns = array_map(function ($columnName): string {
            return ColumnNameSanitizer::sanitize($columnName);
        }, $newTableConfigurationColumns);

        return array_udiff($configColumns, $currentTableColumns, 'strcasecmp');
    }

    /**
     * @param array $keys
     * @param LoggerInterface $logger
     * @return array
     */
    private function normalizeKeyArray(LoggerInterface $logger, array $keys)
    {
        return array_map(
            function ($key) {
                return trim($key);
            },
            array_unique(
                array_filter($keys, function ($col) use ($logger) {
                    if ($col !== '') {
                        return true;
                    }
                    $logger->warning('Found empty column name in key array.');
                    return false;
                }),
            ),
        );
    }

    private function modifyPrimaryKeyDecider(
        LoggerInterface $logger,
        array $currentTablePrimaryKey,
        array $newTableConfigurationPrimaryKey,
    ): bool {
        $configPK = $this->normalizeKeyArray($logger, $newTableConfigurationPrimaryKey);
        if (count($currentTablePrimaryKey) !== count($configPK)) {
            return true;
        }
        $currentTablePkColumnsCount = count($currentTablePrimaryKey);
        if (count(array_intersect($currentTablePrimaryKey, $configPK)) !== $currentTablePkColumnsCount) {
            return true;
        }
        return false;
    }

    private function modifyPrimaryKey(
        LoggerInterface $logger,
        Client $client,
        string $tableId,
        array $tablePrimaryKey,
        array $configPrimaryKey,
    ): void {
        $logger->warning(sprintf(
            'Modifying primary key of table "%s" from "%s" to "%s".',
            $tableId,
            join(', ', $tablePrimaryKey),
            join(', ', $configPrimaryKey),
        ));
        if ($this->removePrimaryKey($logger, $client, $tableId, $tablePrimaryKey)) {
            // modify primary key
            try {
                if (count($configPrimaryKey)) {
                    $client->createTablePrimaryKey($tableId, $configPrimaryKey);
                }
            } catch (Throwable $e) {
                // warn and try to rollback to original state
                $logger->warning(
                    "Error changing primary key of table {$tableId}: " . $e->getMessage(),
                );
                if (count($tablePrimaryKey) > 0) {
                    $client->createTablePrimaryKey($tableId, $tablePrimaryKey);
                }
            }
        }
    }

    private function removePrimaryKey(LoggerInterface $logger, Client $client, string $tableId, array $tablePrimaryKey,): bool
    {
        if (count($tablePrimaryKey) > 0) {
            try {
                $client->removeTablePrimaryKey($tableId);
            } catch (Throwable $e) {
                // warn and go on
                $logger->warning(
                    "Error deleting primary key of table {$tableId}: " . $e->getMessage(),
                );
                return false;
            }
        }
        return true;
    }
}
