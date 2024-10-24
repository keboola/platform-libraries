<?php

declare(strict_types=1);

namespace Keboola\OutputMapping\Writer\Table\TableDefinition;

use Keboola\Datatype\Definition\Common;
use Keboola\Datatype\Definition\DefinitionInterface;

class TableDefinitionColumnFactory
{
    public const NATIVE_TYPE_METADATA_KEY = 'KBC.datatype.backend';

    /**
     * @var class-string<DefinitionInterface>|null
     */
    private ?string $nativeDatatypeClass = null;

    public function __construct(
        array $tableMetadata,
        string $backend,
        bool $enforceBaseTypes,
    ) {
        if (!$enforceBaseTypes) {
            $this->nativeDatatypeClass = $this->getNativeDatatypeClass($tableMetadata, $backend);
        }
    }

    public function createTableDefinitionColumn(string $columnName, array $metadata): TableDefinitionColumnInterface
    {
        if ($this->nativeDatatypeClass) {
            return new NativeTableDefinitionColumn(
                $columnName,
                $this->getNativeDataType($metadata),
            );
        }
        return new BaseTypeTableDefinitionColumn(
            $columnName,
            $this->getBaseTypeFromMetadata($metadata),
        );
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
        if ($type && $this->nativeDatatypeClass !== null) {
            return new $this->nativeDatatypeClass($type, $options);
        }
        return null;
    }

    /**
     * @return class-string<DefinitionInterface>|null
     */
    private function getNativeDatatypeClass(array $tableMetadata, string $backend): ?string
    {
        $columnDefinitionClassName = 'Keboola\\Datatype\\Definition\\' . ucfirst(strtolower($backend));

        $dataTypeBackend = $this->getDatatypeBackendFromMetadata($tableMetadata);
        if ($dataTypeBackend === $backend &&
            class_exists($columnDefinitionClassName) &&
                is_subclass_of($columnDefinitionClassName, DefinitionInterface::class)
        ) {
            return $columnDefinitionClassName;
        }
        return null;
    }

    private function getDatatypeBackendFromMetadata(array $metadata): ?string
    {
        foreach ($metadata as $metadatum) {
            if ($metadatum['key'] === self::NATIVE_TYPE_METADATA_KEY) {
                return $metadatum['value'];
            }
        }
        return null;
    }
}
