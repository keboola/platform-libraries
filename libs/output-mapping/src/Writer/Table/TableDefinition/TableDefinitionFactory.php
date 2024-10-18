<?php

declare(strict_types=1);

namespace Keboola\OutputMapping\Writer\Table\TableDefinition;

class TableDefinitionFactory
{
    public function __construct(
        private readonly array $tableMetadata,
        private readonly string $backendType,
    ) {
    }

    public function createTableDefinition(string $tableName, array $primaryKeys, array $columnMetadata): TableDefinition
    {
        $tableDefinition = new TableDefinition(
            new TableDefinitionColumnFactory(
                $this->tableMetadata,
                $this->backendType,
                false,
            ),
        );
        $tableDefinition->setName($tableName);
        $tableDefinition->setPrimaryKeysNames($primaryKeys);
        foreach ($columnMetadata as $columnName => $metadata) {
            $tableDefinition->addColumn(
                (string) $columnName,
                $metadata,
            );
        }
        return $tableDefinition;
    }
}
