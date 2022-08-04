<?php

declare(strict_types=1);

namespace Keboola\OutputMapping\Writer\Table;

use Keboola\Datatype\Definition\Exasol;
use Keboola\Datatype\Definition\Snowflake;
use Keboola\Datatype\Definition\Synapse;

class TableDefinition
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

    private string $name;

    /** @var TableDefinitionColumn[] $columns */
    private array $columns;

    private array $primaryKeysNames;

    private ?string $nativeTypeClass = null;

    public function __construct(string $componentId, string $bucketBackend)
    {
        if (
            array_key_exists($bucketBackend, self::BACKEND_COMPONENTS_MAP) &&
            in_array($componentId, self::BACKEND_COMPONENTS_MAP[$bucketBackend])
        ) {
            $this->nativeTypeClass = self::NATIVE_BACKEND_TYPE_CLASS_MAP[$bucketBackend];
        }
    }

    public function setName(string $name): self
    {
        $this->name = $name;
        return $this;
    }

    public function getName(): string
    {
        return $this->name;
    }

    public function setPrimaryKeysNames(array $primaryKeys): self
    {
        $this->primaryKeysNames = $primaryKeys;
        return $this;
    }

    public function getColumns(): array
    {
        return $this->columns;
    }

    public function getNativeTypeClass(): ?string
    {
        return $this->nativeTypeClass;
    }

    public function addColumn(string $name, array $metadata): self
    {
        $column = new TableDefinitionColumn($name, $metadata, $this->nativeTypeClass);
        $this->columns[] = $column;
        return $this;
    }

    public function getRequestData()
    {
        $columns = [];
        foreach ($this->columns as $column) {
            $columns[] = $column->toArray();
        }
        return [
            'name' => $this->name,
            'primaryKeysNames' => $this->primaryKeysNames,
            'columns' => $columns
        ];
    }
}
