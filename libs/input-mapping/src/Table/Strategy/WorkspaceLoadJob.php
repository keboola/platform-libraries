<?php

declare(strict_types=1);

namespace Keboola\InputMapping\Table\Strategy;

use Keboola\InputMapping\Table\Options\RewrittenInputTableOptions;

final class WorkspaceLoadJob
{
    /**
     * @param RewrittenInputTableOptions[] $tables
     */
    public function __construct(
        public readonly string $jobId,
        public readonly WorkspaceJobType $jobType,
        public readonly array $tables,
    ) {
    }
}
