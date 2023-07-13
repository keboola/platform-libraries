<?php

declare(strict_types=1);

namespace Keboola\InputMapping\Table\Options;

class RewrittenInputTableOptionsList
{
    /**
     * @var RewrittenInputTableOptions[]
     */
    private array $tables = [];

    /**
     * @param RewrittenInputTableOptions[] $tableOptionsList
     */
    public function __construct(array $tableOptionsList)
    {
        $this->tables = $tableOptionsList;
    }

    /**
     * @returns RewrittenInputTableOptions[]
     */
    public function getTables(): array
    {
        return $this->tables;
    }
}
