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
            $columns[] = $column->toArray();
        }
        return [
            'name' => $this->tableName,
            'primaryKeysNames' => $this->primaryKeysNames,
            'columns' => $columns,
        ];
    }
}
