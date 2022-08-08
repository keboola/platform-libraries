<?php

declare(strict_types=1);

namespace Keboola\OutputMapping\Writer\Table\TableDefinition;

class TableDefinitionColumn
{

    private string $name;

    private ?string $baseType;

    public function __construct(string $name, ?string $baseType)
    {
        $this->name = $name;
        $this->baseType = $baseType;
    }

    public function getName(): string
    {
        return $this->name;
    }

    public function getBaseType(): ?string
    {
        return $this->baseType;
    }

    public function toArray()
    {
        return [
            'name' => $this->name,
            'basetype' => $this->baseType,
        ];
    }
}
