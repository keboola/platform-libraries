<?php


namespace Keboola\OutputMapping\Writer\Table;


use Keboola\Datatype\Definition\Common;
use Keboola\Datatype\Definition\DefinitionInterface;

class TableDefinitionColumn
{

    private string $name;

    private ?DefinitionInterface $dataTypeDefinition;

    private string $baseType;

    public function __construct(string $name, array $metadata)
    {
        $this->name = $name;
        foreach ($metadata as $key => $value) {
            if ($key === Common::KBC_METADATA_KEY_BASETYPE) {
                $this->baseType = $value;
            }
        }
    }

    public function setDataTypeDefinition(DefinitionInterface $dataType): self
    {
        $this->dataTypeDefinition = $dataType;
        return $this;
    }

    public function getName(): string
    {
        return $this->name;
    }

    public function getBaseType(): string
    {
        return $this->baseType;
    }

    public function getDataTypeDefinition(): ?DefinitionInterface
    {
        return $this->dataTypeDefinition;
    }
}
