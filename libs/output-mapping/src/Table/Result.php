<?php

namespace Keboola\OutputMapping\Table;

use Generator;
use Keboola\InputMapping\Table\Result\TableInfo;

class Result
{
    /** @var TableInfo[] */
    private $tables = [];

    public function addTable(TableInfo $table)
    {
        $this->tables[] = $table;
    }

    /**
     * @return Generator
     */
    public function getTables()
    {
        foreach ($this->tables as $table) {
            yield $table;
        }
    }
}
