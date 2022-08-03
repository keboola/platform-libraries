<?php

declare(strict_types=1);

namespace Keboola\OutputMapping\Writer\Table;

use Keboola\Datatype\Definition\Common;
use Keboola\Datatype\Definition\DefinitionInterface;

class TableDefinitionColumn
{

    private string $name;

    private ?DefinitionInterface $dataTypeDefinition;

    private ?string $baseType;

    public function __construct(string $name, ?DefinitionInterface $dataTypeDefinition, ?string $baseType)
    {
        $this->name = $name;
        $this->dataTypeDefinition = $dataTypeDefinition;
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

    public function getDataTypeDefinition(): ?DefinitionInterface
    {
        return $this->dataTypeDefinition;
    }

    public function toArray()
    {
        return [
            'name' => $this->name,
            'basetype' => $this->baseType,
            'definition' => $this->dataTypeDefinition ? $this->dataTypeDefinition->toArray() : null
        ];
    }
}
