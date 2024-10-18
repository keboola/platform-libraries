<?php

declare(strict_types=1);

namespace Keboola\OutputMapping\Storage;

use Keboola\OutputMapping\Exception\InvalidOutputException;
use Keboola\OutputMapping\Exception\PrimaryKeyNotChangedException;
use Keboola\OutputMapping\Mapping\MappingFromConfigurationSchemaColumn;
use Keboola\OutputMapping\Writer\Table\TableDefinitionFromSchema\TableDefinitionFromSchemaColumn;
use Keboola\StorageApi\ClientException;

class TableStructureModifierFromSchema extends AbstractTableStructureModifier
{
    public function updateTableStructure(BucketInfo $bucket, TableInfo $table, TableChangesStore $changesStore): void
    {
        if ($changesStore->hasMissingColumns()) {
            $this->addColumns($table, $bucket->backend, $changesStore->getMissingColumns());
        }

        if ($changesStore->getPrimaryKey() !== null) {
            try {
                $this->modifyPrimaryKey(
                    $table->getId(),
                    $table->getPrimaryKey(),
                    $changesStore->getPrimaryKey()->getPrimaryKeyColumnNames(),
                );
            } catch (PrimaryKeyNotChangedException $e) {
                throw new InvalidOutputException($e->getMessage(), $e->getCode(), $e);
            }
        }

        if (!empty($changesStore->getDifferentColumnAttributes())) {
            $this->modifyColumnsAttributes(
                $table->getId(),
                $bucket->backend,
                $changesStore->getDifferentColumnAttributes(),
            );
        }
    }

    private function addColumns(TableInfo $table, string $backend, array $missingColumns): void
    {
        $columnsAdded = [];
        foreach ($missingColumns as $missingColumn) {
            $columnData = new TableDefinitionFromSchemaColumn($missingColumn, $backend);

            $requestData = $columnData->getRequestData();
            $definition = $requestData['definition'] ?? null;
            $baseType = $requestData['basetype'] ?? null;
            try {
                $this->client->addTableColumn(
                    $table->getId(),
                    $requestData['name'],
                    $table->isTyped() ? $definition : null,
                    $table->isTyped() ? $baseType : null,
                );
                $columnsAdded[] = $requestData['name'];
            } catch (ClientException $e) {
                // remove added columns
                foreach ($columnsAdded as $item) {
                    $this->client->deleteTableColumn($table->getId(), $item);
                }
                throw new InvalidOutputException($e->getMessage(), $e->getCode(), $e);
            }
        }
    }

    /**
     * @param MappingFromConfigurationSchemaColumn[] $columns
     */
    private function modifyColumnsAttributes(
        string $tableId,
        string $backend,
        array $columns,
    ): void {
        foreach ($columns as $column) {
            try {
                $this->client->updateTableColumnDefinition(
                    $tableId,
                    $column->getName(),
                    [
                        'length' => $column->getDataType()?->getLength($backend),
                        // Default value is not works correctly
                        // 'default' => $column->getDataType()?->getDefaultValue($backend),
                        'nullable' => $column->isNullable(),
                    ],
                );
            } catch (ClientException $e) {
                throw new InvalidOutputException($e->getMessage(), $e->getCode(), $e);
            }
        }
    }
}
