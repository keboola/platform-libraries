<?php

declare(strict_types=1);

namespace Keboola\OutputMapping\Writer\Table\TableDefinition;

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
        $output = [
            'name' => $this->name,
        ];
        if ($this->dataTypeDefinition) {
            $output['definition'] = $this->dataTypeDefinition->toArray();
        } elseif ($this->baseType) {
            $output['basetype'] = $this->baseType;
        }
        return $output;
    }
}
