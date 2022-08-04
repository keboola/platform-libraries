<?php

declare(strict_types=1);

namespace Keboola\OutputMapping\Writer\Table;

use Keboola\Datatype\Definition\Common;
use Keboola\Datatype\Definition\DefinitionInterface;
use Keboola\Datatype\Definition\Snowflake;

class TableDefinitionColumn
{

    private string $name;

    private ?DefinitionInterface $dataTypeDefinition = null;

    private string $baseType;

    public function __construct(string $name, array $metadata, string $nativeTypeClass = null)
    {
        $this->name = $name;
        if ($nativeTypeClass) {
            $type = null;
            $options = [];
            foreach ($metadata as $metadatum) {
                switch ($metadatum['key']) {
                    case Common::KBC_METADATA_KEY_TYPE:
                        $type = $metadatum['value'];
                        break;
                    case Common::KBC_METADATA_KEY_LENGTH:
                        $options['length'] = $metadatum['value'];
                        break;
                    case Common::KBC_METADATA_KEY_DEFAULT:
                        $options['default'] = $metadatum['value'];
                        break;
                    case Common::KBC_METADATA_KEY_NULLABLE:
                        $options['nullable'] = $metadatum['value'];
                        break;
                    default:
                        break;
                }
            }
            if ($type) {
                $this->dataTypeDefinition = new $nativeTypeClass($type, $options);
            }
        }
        foreach ($metadata as $metadatum) {
            if ($metadatum['key'] === Common::KBC_METADATA_KEY_BASETYPE) {
                $this->baseType = $metadatum['value'];
            }
        }
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

    public function toArray()
    {
        return [
            'name' => $this->name,
            'basetype' => $this->baseType,
            'definition' => $this->dataTypeDefinition ? $this->dataTypeDefinition->toArray() : null
        ];
    }
}
