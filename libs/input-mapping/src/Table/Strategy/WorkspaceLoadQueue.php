<?php

declare(strict_types=1);

namespace Keboola\InputMapping\Table\Strategy;

use Keboola\InputMapping\Table\Options\RewrittenInputTableOptions;

final class WorkspaceLoadQueue
{
    /**
     * @param WorkspaceLoadJob[] $jobs
     */
    public function __construct(
        public readonly array $jobs,
    ) {
    }

    /**
     * @return RewrittenInputTableOptions[]
     */
    public function getAllTables(): array
    {
        $tables = [];
        foreach ($this->jobs as $job) {
            $tables = array_merge($tables, $job->tables);
        }
        return $tables;
    }
}
