<?php

declare(strict_types=1);

namespace Keboola\QueryApi\Response;

class ExecuteQueryResponse
{
    /**
     * @param array<array<string, mixed>> $statements
     * @param JobResultsResponse[] $results
     */
    public function __construct(
        private readonly string $queryJobId,
        private readonly string $status,
        private readonly array $statements,
        private readonly array $results,
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
     * @return array<array<string, mixed>>
     */
    public function getStatements(): array
    {
        return $this->statements;
    }

    /**
     * @return JobResultsResponse[]
     */
    public function getResults(): array
    {
        return $this->results;
    }
}
