<?php

declare(strict_types=1);

namespace Keboola\OutputMapping\Writer\Table\TableDefinition;

use Keboola\Datatype\Definition\DefinitionInterface;

class NativeTableDefinitionColumn implements TableDefinitionColumnInterface
{
    private DefinitionInterface $dataTypeDefinition;

    private string $name;

    public function __construct(string $name, DefinitionInterface $dataTypeDefinition)
    {
        $this->name = $name;
        $this->dataTypeDefinition = $dataTypeDefinition;
    }

    public function getName(): string
    {
        return $this->name;
    }

    public function getDataTypeDefinition(): DefinitionInterface
    {
        return $this->dataTypeDefinition;
    }

    public function toArray(): array
    {
        return [
            'name' => $this->name,
            'definition' => $this->dataTypeDefinition->toArray(),
        ];
    }
}
