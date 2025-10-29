<?php

declare(strict_types=1);

namespace Keboola\QueryApi\Response;

use Keboola\QueryApi\Response\JobResultsResponse;

class WorkspaceQueryResponse
{
    /**
     * @param array<Statement> $statements
     * @param array<JobResultsResponse> $results
     */
    public function __construct(
        readonly string $queryJobId,
        readonly string $status,
        readonly array $statements,
        readonly array $results,
    ) {
    }

    public function getQueryJobId(): string
    {
        return $this->queryJobId;
    }

    public function getStatus(): string
    {
        return $this->status;
    }

    /**
     * @return array<Statement>
     */
    public function getStatements(): array
    {
        return $this->statements;
    }

    /**
     * @return array<JobResultsResponse>
     */
    public function getResults(): array
    {
        return $this->results;
    }
}
