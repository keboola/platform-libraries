<?php

namespace Keboola\InputMapping\Reader\Definition;

class TablesDefinition
{
    private $tables = [];

    public function __construct(array $configurations)
    {
        foreach ($configurations as $item) {
            $this->tables[] = new TableDefinition($item);
        }
    }

    /**
     * @returns TableDefinition[]
     */
    public function getTables()
    {
        return $this->tables;
    }
}