<?php

declare(strict_types=1);

namespace Keboola\InputMapping\Table\Strategy;

use Keboola\InputMapping\Table\Options\RewrittenInputTableOptions;
use Keboola\InputMapping\Table\StrategyInterface;
use Keboola\StorageApi\TableExporter;

/**
 * @phpstan-import-type ExportOptions from TableExporter
 */
final class TableExportQueue implements TableLoadQueueInterface
{
    /**
     * @param array<int|string, RewrittenInputTableOptions> $tablesByJobId map of export jobId => table.
     *     Note: numeric-string job ids are cast to int by PHP array-key semantics, so getJobIds() may return
     *     ints — callers must accept int|string.
     * @param class-string<StrategyInterface> $strategyClass
     * @param array<int|string, array{
     *     tableId: string,
     *     destination: string,
     *     exportOptions: ExportOptions,
     * }> $exportJobs TableExporter::queueTableExports() map; needed by Local to download files (empty for S3/ABS)
     */
    public function __construct(
        public readonly array $tablesByJobId,
        private readonly string $strategyClass,
        private readonly string $destination,
        public readonly array $exportJobs = [],
    ) {
    }

    public function getJobIds(): array
    {
        return array_keys($this->tablesByJobId);
    }

    /**
     * @return RewrittenInputTableOptions[]
     */
    public function getAllTables(): array
    {
        return array_values($this->tablesByJobId);
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
