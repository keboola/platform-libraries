<?php

declare(strict_types=1);

namespace Keboola\InputMapping\Table\Strategy;

use Keboola\InputMapping\Table\Options\RewrittenInputTableOptions;
use Keboola\InputMapping\Table\StrategyInterface;

final class WorkspaceLoadQueue implements TableLoadQueueInterface
{
    /**
     * @param WorkspaceLoadJob[] $jobs
     * @param class-string<StrategyInterface> $strategyClass
     */
    public function __construct(
        public readonly array $jobs,
        private readonly string $strategyClass,
        private readonly string $destination,
    ) {
    }

    public function getJobIds(): array
    {
        return array_map(
            fn(WorkspaceLoadJob $job) => $job->jobId,
            $this->jobs,
        );
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

    public function getStrategyClass(): string
    {
        return $this->strategyClass;
    }

    public function getDestination(): string
    {
        return $this->destination;
    }
}
