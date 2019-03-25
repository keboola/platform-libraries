<?php

namespace Keboola\InputMapping\Reader\Options;

class InputTablesOptions
{
    /**
     * @var InputTableOptions[]
     */
    private $tables = [];

    public function __construct(array $configurations)
    {
        foreach ($configurations as $item) {
            $this->tables[] = new InputTableOptions($item);
        }
    }

    /**
     * @returns InputTableOptions[]
     */
    public function getTables()
    {
        return $this->tables;
    }
}
