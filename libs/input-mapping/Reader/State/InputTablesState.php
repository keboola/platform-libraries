<?php

namespace Keboola\InputMapping\Reader\State;

use Keboola\InputMapping\Reader\State\Exception\TableNotFoundException;

class InputTablesState
{
    /**
     * @var InputTableState[]
     */
    private $tables = [];

    public function __construct(array $configurations)
    {
        foreach ($configurations as $item) {
            $this->tables[] = new InputTableState($item);
        }
    }

    /**
     * @param $tableName
     * @return InputTableState
     * @throws TableNotFoundException
     */
    public function getTable($tableName)
    {
        foreach ($this->tables as $table) {
            if ($table->getSource() === $tableName) {
                return $table;
            }
        }
        throw new TableNotFoundException('State for table "' . $tableName . '" not found.');
    }
}
