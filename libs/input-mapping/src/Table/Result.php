<?php

declare(strict_types=1);

namespace Keboola\InputMapping\Table;

use Keboola\InputMapping\State\InputTableStateList;
use Keboola\InputMapping\Table\Result\Metrics;
use Keboola\InputMapping\Table\Result\TableInfo;

class Result
{
    /** @var TableInfo[] */
    private array $tables = [];

    private InputTableStateList $inputTableStateList;
    private ?Metrics $metrics = null;

    public function addTable(TableInfo $table): void
    {
        $this->tables[] = $table;
    }

    /**
     * @return TableInfo[]
     */
    public function getTables(): array
    {
        return $this->tables;
    }

    public function setMetrics(array $jobResults): void
    {
        $this->metrics = new Metrics($jobResults);
    }

    public function setInputTableStateList(InputTableStateList $inputTableStateList): void
    {
        $this->inputTableStateList = $inputTableStateList;
    }

    public function getInputTableStateList(): InputTableStateList
    {
        return $this->inputTableStateList;
    }

    public function getMetrics(): ?Metrics
    {
        return $this->metrics;
    }
}
