<?php

declare(strict_types=1);

namespace Keboola\QueryApi\Tests\Functional;

use Keboola\QueryApi\ClientException;
use Keboola\QueryApi\Response\JobResultsResponse;
use Keboola\QueryApi\ResultHelper;

class BasicQueryTest extends BaseFunctionalTestCase
{
    public function testSubmitSimpleSelectQuery(): void
    {
        // Test a simple SELECT query that doesn't require any tables
        $response = $this->queryClient->submitQueryJob(
            $this->getTestBranchId(),
            $this->getTestWorkspaceId(),
            [
                'statements' => ['SELECT CURRENT_TIMESTAMP() AS "current_time"'],
                'transactional' => false,
            ],
        );

        $queryJobId = $response->getQueryJobId();
        self::assertNotEmpty($queryJobId);

        // Wait for job completion
        $finalStatus = $this->queryClient->waitForJobCompletion($queryJobId);

        self::assertEquals('completed', $finalStatus->getStatus());
        self::assertEquals($queryJobId, $finalStatus->getQueryJobId());
        $statements = $finalStatus->getStatements();
        self::assertCount(1, $statements);

        $statement = $statements[0];
        self::assertEquals('completed', $statement->getStatus());

        // Get job results
        $statementId = $statement->getId();
        $resultsResponse = $this->queryClient->getJobResults($queryJobId, $statementId);
        $resultsResponse = ResultHelper::mapColumnNamesIntoData($resultsResponse);

        self::assertEquals('completed', $resultsResponse->getStatus());
        self::assertGreaterThanOrEqual(1, $resultsResponse->getNumberOfRows());

        // Verify it's a valid timestamp (numeric string)

        /**
         * @var array{current_time: string}[] $data
         */
        $data = $resultsResponse->getData();
        self::assertMatchesRegularExpression('/^\d+\.\d+$/', $data[0]['current_time']);
    }


    public function testExecuteWorkspaceQuery(): void
    {
        // Test the new executeWorkspaceQuery method with a simple query
        $response = $this->queryClient->executeWorkspaceQuery(
            $this->getTestBranchId(),
            $this->getTestWorkspaceId(),
            [
                'statements' => ['SELECT CURRENT_TIMESTAMP() AS "current_time"'],
                'transactional' => false,
            ],
        );

        // Verify job completed successfully
        self::assertEquals('completed', $response->getStatus());
        self::assertNotEmpty($response->getQueryJobId());

        // Verify statements
        $statements = $response->getStatements();
        self::assertCount(1, $statements);

        $statement = $statements[0];
        self::assertEquals('completed', $statement->getStatus());

        // Verify results
        $results = $response->getResults();
        self::assertCount(1, $results);

        $result = $results[0];
        self::assertInstanceOf(JobResultsResponse::class, $result);
        self::assertEquals('completed', $result->getStatus());
        self::assertGreaterThanOrEqual(1, $result->getNumberOfRows());
    }

    public function testExecuteInvalidWorkspaceQuery(): void
    {
        self::expectException(ClientException::class);
        self::expectExceptionMessage('\'COOTIES\' does not exist or not authorized');
        self::expectExceptionCode(400);
        $this->queryClient->executeWorkspaceQuery(
            $this->getTestBranchId(),
            $this->getTestWorkspaceId(),
            [
                'statements' => ['SELECT 1', 'SELECT * FROM Cooties', 'SELECT 2'],
                'transactional' => false,
            ],
        );
    }
}
