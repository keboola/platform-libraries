<?php

declare(strict_types=1);

namespace Keboola\InputMapping\Table\Strategy;

use Keboola\InputMapping\Exception\InputOperationException;
use Keboola\InputMapping\Table\Options\RewrittenInputTableOptions;

final class WorkspaceLoadJob
{
    /**
     * @param RewrittenInputTableOptions[] $tables
     */
    public function __construct(
        public readonly string $jobId,
        public readonly WorkspaceLoadType $jobType,
        public readonly array $tables,
    ) {
        // Guard: only CLONE or COPY allowed for job types
        if (!in_array($jobType, [WorkspaceLoadType::CLONE, WorkspaceLoadType::COPY], true)) {
            throw new InputOperationException(
                sprintf('Invalid job type "%s". Only CLONE and COPY are allowed for jobs.', $jobType->value),
            );
        }
    }
}
