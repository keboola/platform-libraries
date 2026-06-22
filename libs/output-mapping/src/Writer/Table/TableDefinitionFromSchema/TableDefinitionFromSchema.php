<?php

declare(strict_types=1);

namespace Keboola\OutputMapping\Writer\Table\TableDefinitionFromSchema;

use Keboola\OutputMapping\Mapping\MappingFromConfigurationSchemaColumn;
use Keboola\OutputMapping\Writer\Table\TableDefinitionInterface;

class TableDefinitionFromSchema implements TableDefinitionInterface
{
    private array $primaryKeys = [];

    private array $columns = [];

    public function __construct(
        readonly private string $tableName,
        array $columns,
        string $backend,
        readonly private ?string $tableDescription = null,
    ) {
        foreach ($columns as $column) {
            $this->addColumn($column, $backend);
        }
    }

    public function addColumn(MappingFromConfigurationSchemaColumn $column, string $backend): void
    {
        if ($column->isPrimaryKey() && !in_array($column->getName(), $this->primaryKeys)) {
            $this->primaryKeys[] = $column->getName();
        }

        $tableDefinitionColumn = new TableDefinitionFromSchemaColumn($column, $backend);
        $this->columns[] = $tableDefinitionColumn->getRequestData();
    }

    public function getRequestData(): array
    {
        $requestData = [
            'name' => $this->tableName,
            'primaryKeysNames' => $this->primaryKeys,
            'columns' => $this->columns,
        ];
        if ($this->tableDescription !== null) {
            $requestData['description'] = $this->tableDescription;
        }
        return $requestData;
    }

    public function getTableName(): string
    {
        return $this->tableName;
    }
}
