<?php

namespace Keboola\InputMapping\Table;

use Generator;
use Keboola\InputMapping\State\InputTableStateList;
use Keboola\InputMapping\Table\Result\Metrics;
use Keboola\InputMapping\Table\Result\TableInfo;

class Result
{
    /** @var TableInfo[] */
    private $tables = [];

    /** @var InputTableStateList */
    private $inputTableStateList;

    /** @var Metrics */
    private $metrics;

    public function __construct()
    {
    }

    public function addTable(TableInfo $table)
    {
        $this->tables[] = $table;
    }

    /**
     * @return Generator
     */
    public function getTables()
    {
        foreach ($this->tables as $table) {
            yield $table;
        }
    }

    public function setMetrics(array $jobResults)
    {
        $this->metrics = new Metrics($jobResults);
    }

    public function setInputTableStateList(InputTableStateList $inputTableStateList)
    {
        $this->inputTableStateList = $inputTableStateList;
    }

    /**
     * @return InputTableStateList
     */
    public function getInputTableStateList()
    {
        return $this->inputTableStateList;
    }

    /**
     * @return Metrics
     */
    public function getMetrics()
    {
        return $this->metrics;
    }
}
