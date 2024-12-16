<?php

declare(strict_types=1);

namespace Keboola\OutputMapping\Storage;

use Keboola\OutputMapping\Exception\InvalidTableStructureException;
use Keboola\OutputMapping\Mapping\MappingFromConfigurationSchemaColumn;
use Keboola\OutputMapping\Mapping\MappingFromConfigurationSchemaPrimaryKey;
use Keboola\OutputMapping\Writer\Helper\PrimaryKeyHelper;
use Psr\Log\LoggerInterface;

abstract class AbstractTableStructureValidator
{
    public function __construct(readonly LoggerInterface $logger, readonly array $table)
    {
    }

    /**
     * @param MappingFromConfigurationSchemaColumn[] $schemaColumns
     */
    abstract public function validate(?array $schemaColumns): TableChangesStore;

    /**
     * @param MappingFromConfigurationSchemaColumn[] $schemaColumns
     */
    protected function validateColumnsName(
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

    protected function validatePrimaryKeys(
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
}
