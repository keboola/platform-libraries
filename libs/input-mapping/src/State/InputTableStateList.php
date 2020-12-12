<?php

namespace Keboola\InputMapping\State;

use JsonSerializable;
use Keboola\InputMapping\Exception\TableNotFoundException;

class InputTableStateList implements JsonSerializable
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

    /**
     * @return array
     */
    public function jsonSerialize()
    {
        return array_map(function (InputTableState $table) {
            return $table->jsonSerialize();
        }, $this->tables);
    }
}
