<?php

declare(strict_types=1);

namespace Keboola\OutputMapping\Writer\Table\TableDefinition;

use Keboola\OutputMapping\Writer\Table\TableDefinitionInterface;

class TableDefinition implements TableDefinitionInterface
{
    public function __construct(
        private readonly TableDefinitionColumnFactory $tableDefinitionColumnFactory,
    ) {
    }

    private string $tableName;

    /** @var TableDefinitionColumnInterface[] $columns */
    private array $columns = [];

    private array $primaryKeysNames = [];

    private ?string $tableDescription = null;

    /** @var array<string, string> column name => description */
    private array $columnDescriptions = [];

    public function setTableName(string $tableName): self
    {
        $this->tableName = $tableName;
        return $this;
    }

    public function getTableName(): string
    {
        return $this->tableName;
    }

    public function setPrimaryKeysNames(array $primaryKeys): self
    {
        $this->primaryKeysNames = $primaryKeys;
        return $this;
    }

    public function setTableDescription(?string $tableDescription): self
    {
        $this->tableDescription = $tableDescription;
        return $this;
    }

    /**
     * @param array<string, string> $columnDescriptions column name => description
     */
    public function setColumnDescriptions(array $columnDescriptions): self
    {
        $this->columnDescriptions = $columnDescriptions;
        return $this;
    }

    public function getColumns(): array
    {
        return $this->columns;
    }

    public function addColumn(
        string $name,
        array $columnMetadata,
    ): self {
        $column = $this->tableDefinitionColumnFactory->createTableDefinitionColumn($name, $columnMetadata);
        $this->columns[] = $column;
        return $this;
    }

    public function getRequestData(): array
    {
        $columns = [];
        foreach ($this->columns as $column) {
            $columnData = $column->toArray();
            if (isset($this->columnDescriptions[$column->getName()])) {
                // The create-table-definition endpoint stores the column description inside the column
                // `definition` (definition.description), unlike the update endpoint which uses a top-level
                // `description`. A definition without a type may carry only a description, so this is valid
                // for typed, base-type and non-typed columns alike.
                $definition = $columnData['definition'] ?? [];
                $definition['description'] = $this->columnDescriptions[$column->getName()];
                $columnData['definition'] = $definition;
            }
            $columns[] = $columnData;
        }
        $requestData = [
            'name' => $this->tableName,
            'primaryKeysNames' => $this->primaryKeysNames,
            'columns' => $columns,
        ];
        if ($this->tableDescription !== null) {
            $requestData['description'] = $this->tableDescription;
        }
        return $requestData;
    }
}
