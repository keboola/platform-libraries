<?php

declare(strict_types=1);

namespace Keboola\OutputMapping\Table;

use Keboola\InputMapping\Table\Result\TableInfo;
use Keboola\OutputMapping\Table\Result\Metrics;

class Result
{
    /** @var TableInfo[] */
    private array $tables = [];

    private ?Metrics $metrics = null;

    /** @var array<string, array<string, int|string>> */
    private array $genericVariables = [];

    /** @var array<string, mixed> */
    private array $customVariables = [];

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

    public function setMetrics(Metrics $metrics): void
    {
        $this->metrics = $metrics;
    }

    public function getMetrics(): ?Metrics
    {
        return $this->metrics;
    }

    public function addGenericVariable(string $tableId, string $variableName, int|string $variableValue): void
    {
        $this->genericVariables[$tableId][$variableName] = $variableValue;
    }

    /** @return array<string, array<string, int|string>> */
    public function getGenericVariables(): array
    {
        return $this->genericVariables;
    }

    /** @param array<string, mixed> $variables */
    public function setCustomVariables(array $variables): void
    {
        $this->customVariables = $variables;
    }

    /** @return array<string, mixed> */
    public function getCustomVariables(): array
    {
        return $this->customVariables;
    }
}
