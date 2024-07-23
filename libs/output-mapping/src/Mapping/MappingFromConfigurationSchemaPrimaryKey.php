<?php

declare(strict_types=1);

namespace Keboola\OutputMapping\Mapping;

class MappingFromConfigurationSchemaPrimaryKey
{
    /** @var MappingFromConfigurationSchemaColumn[] $columns */
    private array $columns = [];

    public function getColumns(): array
    {
        return $this->columns;
    }

    public function addPrimaryKeyColumn(MappingFromConfigurationSchemaColumn $column): void
    {
        $this->columns[] = $column;
    }

    public function getPrimaryKeyColumnNames(): array
    {
        return array_map(
            function (MappingFromConfigurationSchemaColumn $column): string {
                return $column->getName();
            },
            $this->columns,
        );
    }
}
