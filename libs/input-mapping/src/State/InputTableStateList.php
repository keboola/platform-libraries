<?php

declare(strict_types=1);

namespace Keboola\InputMapping\State;

use JsonSerializable;
use Keboola\InputMapping\Exception\TableNotFoundException;

class InputTableStateList implements JsonSerializable
{
    /**
     * @var InputTableState[]
     */
    private array $tables = [];

    public function __construct(array $configurations)
    {
        foreach ($configurations as $item) {
            $this->tables[] = new InputTableState($item);
        }
    }

    /**
     * @throws TableNotFoundException
     */
    public function getTable(string $tableName): InputTableState
    {
        foreach ($this->tables as $table) {
            if ($table->getSource() === $tableName) {
                return $table;
            }
        }
        throw new TableNotFoundException('State for table "' . $tableName . '" not found.');
    }

    public function jsonSerialize(): array
    {
        return array_map(function (InputTableState $table) {
            return $table->jsonSerialize();
        }, $this->tables);
    }
}
