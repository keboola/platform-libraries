<?php

declare(strict_types=1);

namespace Keboola\OutputMapping\Writer\Table;

class TableDefinition
{
    private string $name;
    private array $columns;
    private array $primaryKeysNames;

    public function setName(string $name): self
    {
        $this->name = $name;
        return $this;
    }

    public function getName(): string
    {
        return $this->name;
    }

    public function getPrimaryKeysNames(): array
    {
        return $this->primaryKeysNames;
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

    public function addColumn(string $name, array $metadata): void
    {

    }
}
