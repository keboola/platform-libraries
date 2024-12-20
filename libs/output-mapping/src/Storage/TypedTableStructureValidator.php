<?php

declare(strict_types=1);

namespace Keboola\OutputMapping\Storage;

use Keboola\OutputMapping\Exception\InvalidOutputException;
use Keboola\OutputMapping\Exception\InvalidTableStructureException;
use Keboola\OutputMapping\Mapping\MappingFromConfigurationSchemaColumn;

class TypedTableStructureValidator extends AbstractTableStructureValidator
{
    public function validate(?array $schemaColumns): TableChangesStore
    {
        if ($this->table['isTyped'] === false) {
            throw new InvalidOutputException(sprintf('Table "%s" is not typed.', $this->table['id']));
        }

        $tableChangesStore = new TableChangesStore();

        if (is_null($schemaColumns)) {
            return $tableChangesStore;
        }

        // fill misssing columns
        $tableChangesStore = $this->validateColumnsName(
            $this->table['id'],
            array_map(
                fn($column) => $column['name'],
                $this->table['definition']['columns'],
            ),
            $schemaColumns,
            $tableChangesStore,
        );

        // fill primary key if needed
        $tableChangesStore = $this->validatePrimaryKeys(
            $this->table['definition']['primaryKeysNames'],
            $schemaColumns,
            $tableChangesStore,
        );

        $tableChangesStore = $this->validateColumnsAttributes(
            $this->table,
            $schemaColumns,
            $this->table['bucket']['backend'],
            $tableChangesStore,
        );
        $this->validateTypedTable($schemaColumns);

        return $tableChangesStore;
    }

    /**
     * @param MappingFromConfigurationSchemaColumn[] $schemaColumns
     */
    private function validateTypedTable(array $schemaColumns): void
    {
        $columnDefinitionClassName = sprintf(
            'Keboola\\Datatype\\Definition\\%s',
            ucfirst(strtolower($this->table['bucket']['backend'])),
        );
        $validationErrors = [];
        foreach ($schemaColumns as $schemaColumn) {
            if (!$schemaColumn->getDataType()) {
                continue;
            }
            $findColumnInStorageTable = array_filter(
                $this->table['definition']['columns'],
                fn($column) => $column['name'] === $schemaColumn->getName(),
            );
            if (!$findColumnInStorageTable) {
                continue;
            }

            $filteresTableColumns = array_filter(
                $this->table['definition']['columns'],
                fn($column) => $column['name'] === $schemaColumn->getName(),
            );

            $tableColumn = current($filteresTableColumns);
            try {
                $schemaColumnType = $schemaColumn->getDataType()->getBackendTypeName($this->table['bucket']['backend']);
                $tableColumnType = $tableColumn['definition']['type'];
                $columnDefinition = new $columnDefinitionClassName($schemaColumnType);
                if (method_exists($columnDefinition, 'getBackendBasetype')) {
                    $schemaColumnType = $columnDefinition->getBackendBasetype();
                }
            } catch (InvalidOutputException) {
                $schemaColumnType = $schemaColumn->getDataType()->getBaseTypeName();
                $tableColumnType = $tableColumn['basetype'];
            }

            // Snowflake has different types for TIMESTAMP based on settings in Snowflake Account
            if ($this->table['bucket']['backend'] === 'snowflake') {
                if (in_array($schemaColumnType, ['TIMESTAMP', 'TIMESTAMP_LTZ', 'TIMESTAMP_NTZ', 'TIMESTAMP_TZ'])) {
                    $schemaColumnType = 'TIMESTAMP';
                }
                if (in_array($tableColumnType, ['TIMESTAMP', 'TIMESTAMP_LTZ', 'TIMESTAMP_NTZ', 'TIMESTAMP_TZ'])) {
                    $tableColumnType = 'TIMESTAMP';
                }
            }

            if (strtolower($schemaColumnType) !== strtolower($tableColumnType)) {
                $validationErrors[] = sprintf(
                    'Table "%s" column "%s" has different type than the schema.'.
                    ' Table type: "%s", schema type: "%s".',
                    $this->table['id'],
                    $schemaColumn->getName(),
                    $tableColumnType,
                    $schemaColumnType,
                );
            }
        }

        if ($validationErrors) {
            throw new InvalidTableStructureException(implode(' ', $validationErrors));
        }
    }

    private function validateColumnsAttributes(
        array $table,
        array $schemaColumns,
        string $bucketBackend,
        TableChangesStore $tableChangesStore,
    ): TableChangesStore {
        foreach ($table['definition']['columns'] ?? [] as $column) {
            /** @var MappingFromConfigurationSchemaColumn[] $schemaColumn */
            $schemaColumn = array_filter(
                $schemaColumns,
                fn(MappingFromConfigurationSchemaColumn $item) => $item->getName() === $column['name'],
            );

            // column not exists
            if (!$schemaColumn) {
                continue;
            }

            $schemaColumn = current($schemaColumn);

            // Default value is not works correctly
            // $tableColumnDefault = $column['definition']['default'] ?? null;
            // $hasDefaultValueChanged = false;
            // if ($schemaColumn->getDataType()?->getDefaultValue($bucketBackend) !== $tableColumnDefault) {
            //     $hasDefaultValueChanged = true;
            // }

            $hasNullableChanged = false;
            if ($schemaColumn->isNullable() !== $column['definition']['nullable']) {
                $hasNullableChanged = true;
            }

            $hasLengthChanged = false;
            $length = $schemaColumn->getDataType()?->getLength($bucketBackend);
            if ($length && $length !== $column['definition']['length']) {
                $hasLengthChanged = true;
            }

            if (/* $hasDefaultValueChanged || */$hasNullableChanged || $hasLengthChanged) {
                $tableChangesStore->addColumnAttributeChanges($schemaColumn);
            }
        }

        return $tableChangesStore;
    }
}
