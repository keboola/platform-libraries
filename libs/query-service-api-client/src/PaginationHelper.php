<?php

declare(strict_types=1);

namespace Keboola\QueryApi;

use Keboola\QueryApi\Response\JobResultsResponse;
use Keboola\QueryApi\Response\JobStatusResponse;

class PaginationHelper
{
    public function __construct(private readonly Client $client)
    {
    }

    /**
     * Fetches all pages of results for each completed statement in the given job status.
     *
     * @return array<string, JobResultsResponse> Keyed by statementId
     */
    public function getAllResults(JobStatusResponse $jobStatus, int $pageSize = 500): array
    {
        if ($pageSize < 1) {
            throw new ClientException(sprintf('pageSize must be at least 1, %d given', $pageSize));
        }

        $results = [];
        $queryJobId = $jobStatus->getQueryJobId();

        foreach ($jobStatus->getStatements() as $statement) {
            if ($statement->getStatus() !== 'completed') {
                continue;
            }

            $statementId = $statement->getId();
            $results[$statementId] = $this->fetchAllPages($queryJobId, $statementId, $pageSize);
        }

        return $results;
    }

    private function fetchAllPages(string $queryJobId, string $statementId, int $pageSize): JobResultsResponse
    {
        // Fetch first page before the loop — $firstPage is always JobResultsResponse, never null
        $firstPage = $this->client->getJobResults($queryJobId, $statementId, $pageSize, 0);
        /** @var array<array<array<mixed>>> $pages */
        $pages = [$firstPage->getData()];
        $offset = $pageSize;
        $lastPage = $firstPage;

        // $lastPage tracks most recently fetched page; loop continues while last page was full
        while (count($lastPage->getData()) >= $pageSize) {
            $lastPage = $this->client->getJobResults($queryJobId, $statementId, $pageSize, $offset);
            $pages[] = $lastPage->getData();
            $offset += $pageSize;
        }

        /** @var array<array<mixed>> $allData */
        $allData = array_merge(...$pages);

        return new JobResultsResponse(
            $firstPage->getStatus(),
            count($allData),
            $firstPage->getRowsAffected(), // rowsAffected is per-statement, not per-page; first page is authoritative
            $allData,
            $firstPage->getColumns(),
            $firstPage->getMessage(),
        );
    }
}
