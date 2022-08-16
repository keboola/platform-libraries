<?php

declare(strict_types=1);

namespace Keboola\OutputMapping\Table\Result;

use Generator;

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

    public function getTableMetrics(): Generator
    {
        foreach ($this->metrics as $metric) {
            yield $metric;
        }
    }
}
