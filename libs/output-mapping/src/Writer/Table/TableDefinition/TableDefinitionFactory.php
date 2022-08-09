<?php

declare(strict_types=1);

namespace Keboola\OutputMapping\Writer\Table\TableDefinition;

use Keboola\Datatype\Definition\Exasol;
use Keboola\Datatype\Definition\Snowflake;
use Keboola\Datatype\Definition\Synapse;

class TableDefinitionFactory
{
    public const NATIVE_TYPE_METADATA_KEY = 'kbc.datatype.source';

    public const NATIVE_BACKEND_TYPE_CLASS_MAP = [
        'snowflake' => Snowflake::class,
        'synapse' => Synapse::class,
        'exasol' => Exasol::class,
    ];

    private array $tableMetadata;

    private string $backendType;

    public function __construct(array $tableMetadata, string $backendType)
    {
        $this->tableMetadata = $tableMetadata;
        $this->backendType = $backendType;
    }

    public function createTableDefinition(string $tableName, array $primaryKeys, array $columnMetadata): TableDefinition
    {

        $tableDefinition = new TableDefinition($this->getNativeDatatypeClass());
        $tableDefinition->setName($tableName);
        $tableDefinition->setPrimaryKeysNames($primaryKeys);
        foreach ($columnMetadata as $columnName => $metadata) {
            $tableDefinition->addColumn($columnName, $metadata);
        }
        return $tableDefinition;
    }

    private function getNativeDatatypeClass(): ?string
    {
        $dataTupeSource = $this->getDatatypeSourceFromMetadata($this->tableMetadata);
        if (
            $dataTupeSource === $this->backendType &&
            array_key_exists($dataTupeSource, self::NATIVE_BACKEND_TYPE_CLASS_MAP)
        ) {
            return self::NATIVE_BACKEND_TYPE_CLASS_MAP[$this->backendType];
        }
        return null;
    }

    private function getDatatypeSourceFromMetadata(array $metadata): ?string
    {
        foreach ($metadata as $metadatum) {
            if ($metadatum['key'] === self::NATIVE_TYPE_METADATA_KEY) {
                return $metadatum['value'];
            }
        }
        return null;
    }
}
