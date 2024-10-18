<?php

declare(strict_types=1);

namespace Keboola\OutputMapping\Storage;

use Keboola\Datatype\Definition\BaseType;
use Keboola\OutputMapping\Exception\InvalidOutputException;
use Keboola\OutputMapping\Exception\InvalidTableStructureException;
use Keboola\OutputMapping\Mapping\MappingFromConfigurationSchemaColumn;
use Keboola\OutputMapping\Mapping\MappingFromConfigurationSchemaPrimaryKey;
use Keboola\OutputMapping\Writer\Helper\PrimaryKeyHelper;
use Keboola\StorageApi\Client;
use Keboola\StorageApi\ClientException;
use Psr\Log\LoggerInterface;

class TableStructureValidator
{
    public function __construct(
        readonly private bool $hasNewNativeTypeFeature,
        readonly private LoggerInterface $logger,
        readonly private Client $client,
    ) {
    }

    /**
     * @param MappingFromConfigurationSchemaColumn[] $schemaColumns
     */
    public function validateTable(string $tableId, array $schemaColumns): TableChangesStore
    {
        $tableChangesStore = new TableChangesStore();

        if (!$this->hasNewNativeTypeFeature) {
            return $tableChangesStore;
        }
        if (!$schemaColumns) {
            return $tableChangesStore;
        }

        try {
            $table = $this->client->getTable($tableId);
        } catch (ClientException $e) {
            if ($e->getCode() === 404) {
                return $tableChangesStore;
            }
            throw new InvalidOutputException($e->getMessage(), $e->getCode(), $e);
        }

        if ($table['isTyped']) {
            // fill misssing columns
            $tableChangesStore = $this->validateColumnsName(
                $table['id'],
                array_map(
                    fn($column) => $column['name'],
                    $table['definition']['columns'],
                ),
                $schemaColumns,
                $tableChangesStore,
            );

            // fill primary key if needed
            $tableChangesStore = $this->validatePrimaryKeys(
                $table['definition']['primaryKeysNames'],
                $schemaColumns,
                $tableChangesStore,
            );

            $tableChangesStore = $this->validateColumnsAttributes(
                $table,
                $schemaColumns,
                $table['bucket']['backend'],
                $tableChangesStore,
            );
            $this->validateTypedTable($table, $schemaColumns, $table['bucket']['backend']);
        } else {
            $tableChangesStore = $this->validateColumnsName(
                $table['id'],
                $table['columns'],
                $schemaColumns,
                $tableChangesStore,
            );

            $tableChangesStore = $this->validatePrimaryKeys(
                $table['primaryKey'],
                $schemaColumns,
                $tableChangesStore,
            );
        }

        return $tableChangesStore;
    }

    /**
     * @param MappingFromConfigurationSchemaColumn[] $schemaColumns
     */
    private function validateTypedTable(array $table, array $schemaColumns, string $bucketBackend): void
    {
        $columnDefinitionClassName = 'Keboola\\Datatype\\Definition\\' . ucfirst(strtolower($bucketBackend));
        $validationErrors = [];
        foreach ($schemaColumns as $schemaColumn) {
            if (!$schemaColumn->getDataType()) {
                continue;
            }
            $findColumnInStorageTable = array_filter(
                $table['definition']['columns'],
                fn($column) => $column['name'] === $schemaColumn->getName(),
            );
            if (!$findColumnInStorageTable) {
                continue;
            }

            $filteresTableColumns = array_filter(
                $table['definition']['columns'],
                fn($column) => $column['name'] === $schemaColumn->getName(),
            );

            $tableColumn = current($filteresTableColumns);
            try {
                $schemaColumnType = $schemaColumn->getDataType()->getBackendTypeName($bucketBackend);
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
            if ($bucketBackend === 'snowflake') {
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
                    $table['id'],
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

    /**
     * @param MappingFromConfigurationSchemaColumn[] $schemaColumns
     */
    private function validateColumnsName(
        string $tableId,
        array $tableColumns,
        array $schemaColumns,
        TableChangesStore $tableChangesStore,
    ): TableChangesStore {
        $schemaColumnsNames = array_map(
            fn(MappingFromConfigurationSchemaColumn $column) => $column->getName(),
            $schemaColumns,
        );

        if (count($schemaColumnsNames) !== count($tableColumns)) {
            $missingColumnsNames = array_diff($schemaColumnsNames, $tableColumns);
            if ($missingColumnsNames) {
                $missingColumns = array_filter(
                    $schemaColumns,
                    fn(MappingFromConfigurationSchemaColumn $column) => in_array(
                        $column->getName(),
                        $missingColumnsNames,
                    ),
                );
                array_walk($missingColumns, fn($column) => $tableChangesStore->addMissingColumn($column));
                return $tableChangesStore;
            }
            throw new InvalidTableStructureException(sprintf(
                'Table "%s" does not contain the same number of columns as the schema.'.
                ' Table columns: %s, schema columns: %s.',
                $tableId,
                count($tableColumns),
                count($schemaColumnsNames),
            ));
        }

        $diff = array_diff($schemaColumnsNames, $tableColumns);

        if ($diff) {
            throw new InvalidTableStructureException(sprintf(
                'Table "%s" does not contain columns: "%s".',
                $tableId,
                implode('", "', $diff),
            ));
        }

        return $tableChangesStore;
    }

    private function validatePrimaryKeys(
        array $tableKeys,
        array $schemaColumns,
        TableChangesStore $tableChangesStore,
    ): TableChangesStore {
        $primaryKeyColumns = array_filter(
            $schemaColumns,
            function (MappingFromConfigurationSchemaColumn $schemaColumn): bool {
                return $schemaColumn->isPrimaryKey();
            },
        );

        if (PrimaryKeyHelper::modifyPrimaryKeyDecider(
            $this->logger,
            $tableKeys,
            array_map(function (MappingFromConfigurationSchemaColumn $column): string {
                return $column->getName();
            }, $primaryKeyColumns),
        )) {
            $primaryKey = new MappingFromConfigurationSchemaPrimaryKey();
            foreach ($primaryKeyColumns as $primaryKeyColumn) {
                $primaryKey->addPrimaryKeyColumn($primaryKeyColumn);
            }
            $tableChangesStore->setPrimaryKey($primaryKey);
        }

        return $tableChangesStore;
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
