<?php

namespace Keboola\InputMapping\Table\Result;

use Generator;

class Metrics
{
    /** @var array */
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
    public function getMetrics()
    {
        foreach ($this->metrics as $metric) {
            yield $metric;
        }
    }
}
