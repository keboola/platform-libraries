<?php

declare(strict_types=1);

namespace Keboola\OutputMapping\Table\Result;

class Metrics
{
    /** @var TableMetrics[] */
    private array $metrics = [];

    public function __construct(array $jobResults)
    {
        foreach ($jobResults as $jobResult) {
            $this->metrics[] = new TableMetrics($jobResult);
        }
    }

    /**
     * @return TableMetrics[]
     */
    public function getTableMetrics(): array
    {
        return $this->metrics;
    }
}
