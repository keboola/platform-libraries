<?php

namespace Keboola\InputMapping\Table\Result;

use Generator;

class Metrics
{
    /** @var TableMetrics[] */
    private $metrics;

    public function __construct(array $jobResults)
    {
        foreach ($jobResults as $jobResult) {
            $this->metrics[] = new TableMetrics($jobResult);
        }
    }

    /**
     * @return Generator
     */
    public function getTableMetrics()
    {
        foreach ($this->metrics as $metric) {
            yield $metric;
        }
    }
}
