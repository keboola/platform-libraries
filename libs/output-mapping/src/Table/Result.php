<?php

declare(strict_types=1);

namespace Keboola\OutputMapping\Table;

use Generator;
use Keboola\InputMapping\Table\Result\TableInfo;
use Keboola\OutputMapping\Table\Result\Metrics;

class Result
{
    /** @var TableInfo[] */
    private array $tables = [];

    private ?Metrics $metrics = null;

    public function addTable(TableInfo $table): void
    {
        $this->tables[] = $table;
    }

    /**
     * @return Generator<TableInfo>
     */
    public function getTables(): Generator
    {
        foreach ($this->tables as $table) {
            yield $table;
        }
    }

    public function setMetrics(Metrics $metrics): void
    {
        $this->metrics = $metrics;
    }

    public function getMetrics(): ?Metrics
    {
        return $this->metrics;
    }
}
