<?php

declare(strict_types=1);

namespace Keboola\OutputMapping\Writer\Table;


class TableDefinition
{
    private string $name;

    /** @var TableDefinitionColumn[] $columns */
    private array $columns;

    private array $primaryKeysNames;

    private ?string $nativeTypeClass;

    public function __construct(?string $nativeTypeClass)
    {
        $this->nativeTypeClass = $nativeTypeClass;
    }

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

    public function addColumn(string $name, array $metadata): self
    {
        $tableDefinitionColumnFactory = new TableDefinitionColumnFactory($this->nativeTypeClass);
        $column = $tableDefinitionColumnFactory->createTableDefinitionColumn($name, $metadata);
        $this->columns[] = $column;
        return $this;
    }

    public function getRequestData()
    {
        $columns = [];
        foreach ($this->columns as $column) {
            $columns[] = $column->toArray();
        }
        return [
            'name' => $this->name,
            'primaryKeysNames' => $this->primaryKeysNames,
            'columns' => $columns
        ];
    }
}
