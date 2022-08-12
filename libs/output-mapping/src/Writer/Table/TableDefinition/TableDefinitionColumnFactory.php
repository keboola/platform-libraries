<?php

declare(strict_types=1);

namespace Keboola\OutputMapping\Writer\Table\TableDefinition;

use Keboola\Datatype\Definition\Common;
use Keboola\Datatype\Definition\DefinitionInterface;
use Keboola\Datatype\Definition\Exasol;
use Keboola\Datatype\Definition\Snowflake;
use Keboola\Datatype\Definition\Synapse;

class TableDefinitionColumnFactory
{
    public const NATIVE_TYPE_METADATA_KEY = 'KBC.datatype.backend';

    public const NATIVE_BACKEND_TYPE_CLASS_MAP = [
        'snowflake' => Snowflake::class,
        'synapse' => Synapse::class,
        'exasol' => Exasol::class,
    ];

    /**
     * @var class-string<DefinitionInterface>|null
     */
    private ?string $nativeDatatypeClass;

    public function __construct(array $tableMetadata, string $backend)
    {
        $this->nativeDatatypeClass = $this->getNativeDatatypeClass($tableMetadata, $backend);
    }

    public function createTableDefinitionColumn($columnName, $metadata): TableDefinitionColumnInterface
    {
        if ($this->nativeDatatypeClass) {
            return new NativeTableDefinitionColumn(
                $columnName,
                $this->getNativeDataType($metadata)
            );
        }
        return new BaseTypeTableDefinitionColumn(
            $columnName,
            $this->getBaseTypeFromMetadata($metadata)
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
        if ($type) {
            return new $this->nativeDatatypeClass($type, $options);
        }
        return null;
    }

    private function getNativeDatatypeClass(array $tableMetadata, string $backend): ?string
    {
        $dataTypeBackend = $this->getDatatypeBackendFromMetadata($tableMetadata);
        if ($dataTypeBackend === $backend &&
            array_key_exists($dataTypeBackend, self::NATIVE_BACKEND_TYPE_CLASS_MAP)
        ) {
            return self::NATIVE_BACKEND_TYPE_CLASS_MAP[$backend];
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
