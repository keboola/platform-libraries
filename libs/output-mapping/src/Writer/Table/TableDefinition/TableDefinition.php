<?php

declare(strict_types=1);

namespace Keboola\OutputMapping\Writer\Table\TableDefinition;

use Keboola\OutputMapping\Writer\Table\TableDefinitionInterface;

class TableDefinition implements TableDefinitionInterface
{
    private string $name;

    /** @var TableDefinitionColumnInterface[] $columns */
    private array $columns = [];

    private array $primaryKeysNames = [];

    public function setName(string $name): self
    {
        $this->name = $name;
        return $this;
    }

    public function getName(): string
    {
        return $this->name;
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
        array $tableMetadata,
        string $backendType,
    ): self {
        $tableDefinitionColumnFactory = new TableDefinitionColumnFactory($tableMetadata, $backendType);
        $column = $tableDefinitionColumnFactory->createTableDefinitionColumn($name, $columnMetadata);
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
            'name' => $this->name,
            'primaryKeysNames' => $this->primaryKeysNames,
            'columns' => $columns,
        ];
    }
}
