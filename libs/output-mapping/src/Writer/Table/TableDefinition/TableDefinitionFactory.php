<?php

declare(strict_types=1);

namespace Keboola\OutputMapping\Writer\Table\TableDefinition;

class TableDefinitionFactory
{
    public function createTableDefinition(string $tableName, array $primaryKeys, array $columnMetadata): TableDefinition
    {
        $tableDefinition = new TableDefinition();
        $tableDefinition->setName($tableName);
        $tableDefinition->setPrimaryKeysNames($primaryKeys);
        foreach ($columnMetadata as $columnName => $metadata) {
            $tableDefinition->addColumn($columnName, $metadata);
        }
        return $tableDefinition;
    }
}
