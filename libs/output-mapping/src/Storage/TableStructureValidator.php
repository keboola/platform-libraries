<?php

declare(strict_types=1);

namespace Keboola\OutputMapping\Storage;

use Keboola\OutputMapping\Exception\InvalidOutputException;
use Keboola\OutputMapping\Exception\InvalidTableStructureException;
use Keboola\OutputMapping\Mapping\MappingFromConfigurationSchemaColumn;
use Keboola\OutputMapping\Writer\Helper\PrimaryKeyHelper;
use Keboola\StorageApi\Client;
use Keboola\StorageApi\ClientException;
use Keboola\Utils\Sanitizer\ColumnNameSanitizer;
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

        if ($table['isTyped']) {
            $this->validateColumnsName(
                $table['id'],
                array_map(
                    fn($column) => $column['name'],
                    $table['definition']['columns'],
                ),
                $schemaColumns,
            );
            $this->validatePrimaryKeys(
                $table['definition']['primaryKeysNames'],
                array_map(
                    fn(MappingFromConfigurationSchemaColumn $column) => $column->getName(),
                    array_filter(
                        $schemaColumns,
                        fn(MappingFromConfigurationSchemaColumn $column) => $column->isPrimaryKey(),
                    ),
                ),
            );
            $this->validateTypedTable($table, $schemaColumns, $table['bucket']['backend']);
        } else {
            $this->validateColumnsName($table['id'], $table['columns'], $schemaColumns);
            $this->validatePrimaryKeys(
                $table['primaryKey'],
                array_map(
                    fn(MappingFromConfigurationSchemaColumn $column) => $column->getName(),
                    array_filter(
                        $schemaColumns,
                        fn(MappingFromConfigurationSchemaColumn $column) => $column->isPrimaryKey(),
                    ),
                ),
            );
            $this->validateUntypedTable($table, $schemaColumns);
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
            $filteresTableColumns = array_filter(
                $table['definition']['columns'],
                fn($column) => $column['name'] === ColumnNameSanitizer::sanitize($schemaColumn->getName()),
            );

            $tableColumn = current($filteresTableColumns);
            try {
                $schemaColumnType = $schemaColumn->getDataType()->getTypeName($bucketBackend);
                $tableColumnType = $tableColumn['definition']['type'];
            } catch (InvalidOutputException) {
                $schemaColumnType = $schemaColumn->getDataType()->getBaseType();
                $tableColumnType = $tableColumn['basetype'];
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

            if ($schemaColumnLength && $schemaColumnLength !== $tableColumn['definition']['length']) {
                $validationErrors[] = sprintf(
                    'Table "%s" column "%s" has different length than the schema.'.
                    ' Table length: "%s", schema length: "%s".',
                    $table['id'],
                    $schemaColumn->getName(),
                    $tableColumn['definition']['length'],
                    $schemaColumnLength,
                );
            }

            if ($schemaColumn->isNullable() !== $tableColumn['definition']['nullable']) {
                $validationErrors[] = sprintf(
                    'Table "%s" column "%s" has different nullable than the schema.'.
                    ' Table nullable: "%s", schema nullable: "%s".',
                    $table['id'],
                    $schemaColumn->getName(),
                    $tableColumn['definition']['nullable'] ? 'true' : 'false',
                    $schemaColumn->isNullable() ? 'true' : 'false',
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
    private function validateUntypedTable(array $table, array $schemaColumns): void
    {
        $validationErrors = [];
        foreach ($schemaColumns as $schemaColumn) {
            if (!$schemaColumn->getDataType()) {
                continue;
            }

            try {
                $schemaColumn->getDataType()->getTypeName($table['bucket']['backend']);
                $this->logger->warning(sprintf(
                    'Table "%s" is untyped, but schema has set specific backend column "%s".',
                    $table['id'],
                    $schemaColumn->getName(),
                ));
                throw new InvalidOutputException('Backend column type is not allowed.');
            } catch (InvalidOutputException) {
                if ($schemaColumn->getDataType()->getBaseType() !== 'STRING') {
                    $validationErrors[] = sprintf(
                        'Table "%s" is untyped, but schema column "%s" has unsupported type "%s".',
                        $table['id'],
                        $schemaColumn->getName(),
                        $schemaColumn->getDataType()->getBaseType(),
                    );
                }
            }
        }

        if ($validationErrors) {
            throw new InvalidTableStructureException(implode(' ', $validationErrors));
        }
    }

    private function validateColumnsName(string $tableId, array $tableColumns, array $schemaColumns): void
    {
        $schemaColumnsNames = array_map(
            fn(MappingFromConfigurationSchemaColumn $column) => ColumnNameSanitizer::sanitize($column->getName()),
            $schemaColumns,
        );

        if (count($schemaColumnsNames) !== count($tableColumns)) {
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
    }

    private function validatePrimaryKeys(array $tableKeys, array $schemaKeys): void
    {
        $schemaKeys = array_map(fn(string $column) => ColumnNameSanitizer::sanitize($column), $schemaKeys);
        $schemaKeys = PrimaryKeyHelper::normalizeKeyArray($this->logger, $schemaKeys);

        $invalidKeys = false;
        if (count($tableKeys) !== count($schemaKeys)) {
            $invalidKeys = true;
        }
        $currentTablePkColumnsCount = count($tableKeys);
        if (count(array_intersect($tableKeys, $schemaKeys)) !== $currentTablePkColumnsCount) {
            $invalidKeys = true;
        }

        if ($invalidKeys) {
            throw new InvalidTableStructureException(sprintf(
                'Table primary keys does not contain the same number of columns as the schema.'.
                ' Table primary keys: "%s", schema primary keys: "%s".',
                implode(', ', $tableKeys),
                implode(', ', $schemaKeys),
            ));
        }
    }
}
