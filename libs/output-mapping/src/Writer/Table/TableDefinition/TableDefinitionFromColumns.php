<?php

declare(strict_types=1);

namespace Keboola\OutputMapping\Writer\Table\TableDefinition;

use Keboola\OutputMapping\Writer\Table\TableDefinitionInterface;

/**
 * Table definition for a non-typed table created from a plain list of column names, i.e. when the manifest
 * declares `columns` but no data types.
 *
 * No column carries a type or a basetype, which is what makes Storage create the table as non-typed -
 * Keboola\Connection\Storage\Request\Bucket\CreateTableDefinition\BaseCreateTableDefinitionRequest::isTypedTable()
 * only reports a typed table when at least one column has `definition.type` or `basetype`.
 */
class TableDefinitionFromColumns implements TableDefinitionInterface
{
    use TableDefinitionDescriptionsTrait;

    /**
     * @param string[] $columns
     * @param string[] $primaryKeysNames
     */
    public function __construct(
        private readonly string $tableName,
        private readonly array $columns,
        private readonly array $primaryKeysNames,
    ) {
    }

    public function getRequestData(): array
    {
        return $this->withDescriptions([
            'name' => $this->tableName,
            'primaryKeysNames' => array_values($this->primaryKeysNames),
            'columns' => array_map(
                static fn(string $columnName): array => ['name' => $columnName],
                array_values($this->columns),
            ),
        ]);
    }

    public function getTableName(): string
    {
        return $this->tableName;
    }
}
