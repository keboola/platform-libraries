<?php

declare(strict_types=1);

namespace Keboola\OutputMapping\Storage;

use Keboola\OutputMapping\Exception\InvalidOutputException;
use Keboola\OutputMapping\Exception\InvalidTableStructureException;
use Keboola\OutputMapping\Mapping\MappingFromConfigurationSchemaColumn;
use Keboola\StorageApi\Client;
use Keboola\StorageApi\ClientException;

class TableStructureValidator
{
    public function __construct(
        readonly private bool $hasNewNativeTypeFeature,
        readonly private Client $client,
    ) {
    }

    /**
     * @param MappingFromConfigurationSchemaColumn[] $schemaColumns
     */
    public function validateTable(string $tableId, array $schemaColumns): void
    {
        if (!$this->hasNewNativeTypeFeature) {
            return;
        }

        try {
            $table = $this->client->getTable($tableId);
        } catch (ClientException $e) {
            if ($e->getCode() === 404) {
                return;
            }
            throw new InvalidOutputException($e->getMessage(), $e->getCode(), $e);
        }

        $this->validateColumnsName($table, $schemaColumns);
        if ($table['isTyped']) {
            $this->validateTypedTable($table, $schemaColumns, $table['bucket']['backend']);
        }
    }

    /**
     * @param MappingFromConfigurationSchemaColumn[] $schemaColumns
     */
    private function validateTypedTable(array $table, array $schemaColumns, string $bucketBackend): void
    {
        $validationErrors = [];
        foreach ($schemaColumns as $schemaColumn) {
            if (!$schemaColumn->getDataType()) {
                continue;
            }
            $tableColumnMetadata = $table['columnMetadata'][$schemaColumn->getName()];
            $tableColumnMetadata = array_combine(
                array_map(fn($column) => $column['key'], $tableColumnMetadata),
                array_map(fn($column) => $column['value'], $tableColumnMetadata),
            );
            try {
                $schemaColumnType = $schemaColumn->getDataType()->getTypeName($bucketBackend);
                $tableColumnType = $tableColumnMetadata['KBC.datatype.type'];
            } catch (InvalidOutputException) {
                $schemaColumnType = $schemaColumn->getDataType()->getBaseType();
                $tableColumnType = $tableColumnMetadata['KBC.datatype.basetype'];
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

            try {
                $schemaColumnLength = $schemaColumn->getDataType()->getLength($bucketBackend);
            } catch (InvalidOutputException) {
                $schemaColumnLength = $schemaColumn->getDataType()->getBaseLength();
            }

            if (!$schemaColumnLength) {
                continue;
            }

            if (!isset($tableColumnMetadata['KBC.datatype.length'])) {
                $validationErrors[] = sprintf(
                    'Table "%s" column "%s" has not set length.',
                    $table['id'],
                    $schemaColumn->getName(),
                );
            } elseif (strtolower($schemaColumnLength) !== strtolower($tableColumnMetadata['KBC.datatype.length'])) {
                $validationErrors[] = sprintf(
                    'Table "%s" column "%s" has different length than the schema.'.
                    ' Table length: "%s", schema length: "%s".',
                    $table['id'],
                    $schemaColumn->getName(),
                    $tableColumnMetadata['KBC.datatype.length'],
                    $schemaColumnLength,
                );
            }
        }

        if ($validationErrors) {
            throw new InvalidTableStructureException(implode(' ', $validationErrors));
        }
    }

    private function validateColumnsName(array $table, array $schemaColumns): void
    {
        $schemaColumnsNames = array_map(
            fn(MappingFromConfigurationSchemaColumn $column) => $column->getName(),
            $schemaColumns,
        );

        $tableColumns = $table['columns'];

        if (count($schemaColumnsNames) !== count($tableColumns)) {
            throw new InvalidTableStructureException(sprintf(
                'Table "%s" does not contain the same number of columns as the schema.'.
                ' Table columns: %s, schema columns: %s.',
                $table['id'],
                count($tableColumns),
                count($schemaColumnsNames),
            ));
        }

        $diff = array_diff($schemaColumnsNames, $table['columns']);

        if ($diff) {
            throw new InvalidTableStructureException(sprintf(
                'Table "%s" does not contain columns: "%s".',
                $table['id'],
                implode('", "', $diff),
            ));
        }
    }
}
