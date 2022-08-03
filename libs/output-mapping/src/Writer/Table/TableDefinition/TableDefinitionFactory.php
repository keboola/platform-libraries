<?php


namespace Keboola\OutputMapping\Writer\Table;


use Keboola\Datatype\Definition\Exasol;
use Keboola\Datatype\Definition\Snowflake;
use Keboola\Datatype\Definition\Synapse;

class TableDefinitionFactory
{
    public const NATIVE_BACKEND_TYPE_CLASS_MAP = [
        'snowflake' => Snowflake::class,
        'synapse' => Synapse::class,
        'exasol' => Exasol::class,
    ];

    public const BACKEND_COMPONENTS_MAP = [
        'snowflake' => [
            'keboola.ex-db-snowflake',
            'keboola.snowflake-transformation-v2',
        ],
        'synapse' => [
            'keboola.ex-db-synapse',
            'keboola.synapse-transformation',
        ],
        'exasol' => [
            'keboola.ex-db-exasol',
            'keboola.exasol-transformation',
        ],
    ];

    private string $componentId;

    private string $backendType;

    public function __construct(string $componentId, string $backendType)
    {
        $this->componentId = $componentId;
        $this->backendType = $backendType;
    }

    private function getNativeDatatypeClass(): ?string
    {
        if (
            array_key_exists($this->backendType, self::BACKEND_COMPONENTS_MAP) &&
            in_array($this->componentId, self::BACKEND_COMPONENTS_MAP[$this->backendType])
        ) {
            return self::NATIVE_BACKEND_TYPE_CLASS_MAP[$this->backendType];
        }
        return null;
    }

    public function createTableDefinition(string $tableName, array $primaryKeys, array $columnMetadata): TableDefinition
    {
        $tableDefinition = new TableDefinition($this->getNativeDatatypeClass());
        $tableDefinition->setName($tableName);
        $tableDefinition->setPrimaryKeysNames($primaryKeys);
        foreach ($columnMetadata as $columnName => $metadata) {
            $tableDefinition->addColumn($columnName, $metadata);
        }
        return new TableDefinition();
    }
}
