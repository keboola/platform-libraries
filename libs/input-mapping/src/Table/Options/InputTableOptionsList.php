<?php

declare(strict_types=1);

namespace Keboola\InputMapping\Table\Options;

class InputTableOptionsList
{
    /**
     * @var InputTableOptions[]
     */
    private array $tables = [];

    public function __construct(array $configurations)
    {
        foreach ($configurations as $item) {
            $this->tables[] = new InputTableOptions($item);
        }
    }

    /**
     * @returns InputTableOptions[]
     */
    public function getTables(): array
    {
        return $this->tables;
    }
}
