<?php

declare(strict_types=1);

namespace Keboola\OutputMapping\Writer\Table\TableDefinition;

use Keboola\Datatype\Definition\Common;
use Keboola\Datatype\Definition\DefinitionInterface;

class TableDefinitionColumnFactory
{
    private ?string $nativeDatatypeClass;

    public function __construct(?string $nativeDataTypeClass)
    {
        $this->nativeDatatypeClass = $nativeDataTypeClass;
    }

    public function createTableDefinitionColumn($columnName, $metadata): TableDefinitionColumn
    {
        $baseType = $this->getBaseTypeFromMetadata($metadata);
        $nativeDataType = $this->getNativeDataType($metadata);
        return new TableDefinitionColumn($columnName, $nativeDataType, $baseType);
    }

    private function getBaseTypeFromMetadata(array $metadata): ?string
    {
        foreach ($metadata as $metadatum) {
            if ($metadatum['key'] === Common::KBC_METADATA_KEY_BASETYPE) {
                return $metadatum['value'];
            }
        }
        return null;
    }

    private function getNativeDataType(array $metadata): ?DefinitionInterface
    {
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
        if ($type && $this->nativeDatatypeClass) {
            return new $this->nativeDatatypeClass($type, $options);
        }
        return null;
    }
}
