<?php

declare(strict_types=1);

namespace Keboola\InputMapping\Table\Options;

class RewrittenInputTableOptions extends InputTableOptions
{
    private array $tableInfo;

    public function __construct(array $definition, string $source, int $sourceBranchId, array $tableInfo)
    {
        parent::__construct($definition);
        $this->definition['source'] = $source;
        $this->definition['sourceBranchId'] = $sourceBranchId;
        $this->tableInfo = $tableInfo;
    }

    public function getSourceBranchId(): int
    {
        return $this->definition['sourceBranchId'];
    }

    public function getTableInfo(): array
    {
        return $this->tableInfo;
    }
}
