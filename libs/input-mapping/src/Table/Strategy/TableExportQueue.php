<?php

declare(strict_types=1);

namespace Keboola\InputMapping\Table\Strategy;

use Keboola\InputMapping\Table\Options\RewrittenInputTableOptions;

final class TableExportQueue implements TableLoadQueueInterface
{
    /**
     * @param array<int|string, RewrittenInputTableOptions> $tablesByJobId map of export jobId => table
     * @param array<int|string, array{
     *     tableId: string,
     *     destination: string,
     *     exportOptions: array<string, mixed>,
     * }> $exportJobs TableExporter::queueTableExports() map; needed by Local to download files (empty for S3/ABS)
     */
    public function __construct(
        public readonly array $tablesByJobId,
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
}
