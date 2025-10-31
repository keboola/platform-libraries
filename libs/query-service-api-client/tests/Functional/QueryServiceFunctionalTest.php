<?php

declare(strict_types=1);

namespace Keboola\QueryApi\Tests\Functional;

use Keboola\QueryApi\Client;
use Keboola\QueryApi\ClientException;

class QueryServiceFunctionalTest extends BaseFunctionalTestCase
{
    public function testSubmitAndGetSimpleQuery(): void
    {
        // Create test table with sample data
        $tableName = $this->createTestTable();

        // Submit a simple SELECT query
        $response = $this->queryClient->submitQueryJob(
            $this->getTestBranchId(),
            $this->getTestWorkspaceId(),
            [
                'statements' => [sprintf('SELECT COUNT(*) as row_count FROM %s', $tableName)],
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

        self::assertEquals('completed', $resultsResponse->getStatus());
        self::assertGreaterThanOrEqual(1, $resultsResponse->getNumberOfRows());
    }

    public function testSubmitTransactionalQuery(): void
    {
        // Create test table
        $tableName = $this->createTestTable();

        // Submit transactional queries (INSERT and SELECT)
        $response = $this->queryClient->submitQueryJob(
            $this->getTestBranchId(),
            $this->getTestWorkspaceId(),
            [
                'statements' => [
                    sprintf('INSERT INTO %s (id, name, value) VALUES (4, \'test4\', 400)', $tableName),
                    sprintf('SELECT COUNT(*) as row_count FROM %s', $tableName),
                ],
                'transactional' => true,
            ],
        );

        $queryJobId = $response->getQueryJobId();

        // Wait for completion
        $finalStatus = $this->queryClient->waitForJobCompletion($queryJobId);

        self::assertEquals('completed', $finalStatus->getStatus());
        $statements = $finalStatus->getStatements();
        self::assertCount(2, $statements);

        // Check INSERT statement
        $insertStatement = $statements[0];
        self::assertEquals('completed', $insertStatement->getStatus());

        // Check SELECT statement and its results
        $selectStatement = $statements[1];
        self::assertEquals('completed', $selectStatement->getStatus());

        $selectStatementId = $selectStatement->getId();
        $resultsResponse = $this->queryClient->getJobResults($queryJobId, $selectStatementId);
        self::assertEquals('completed', $resultsResponse->getStatus());
        // After INSERT, there should be 4 rows, but COUNT(*) returns 1 row of data
        self::assertGreaterThanOrEqual(1, $resultsResponse->getNumberOfRows());
    }

    public function testCancelQueryJob(): void
    {
        $response = $this->queryClient->submitQueryJob(
            $this->getTestBranchId(),
            $this->getTestWorkspaceId(),
            [
                'statements' => [
                    'CALL SYSTEM$WAIT(10);', // Wait for 10 seconds to allow time for cancellation
                ],
                'transactional' => false,
            ],
        );

        $queryJobId = $response->getQueryJobId();
        self::assertNotEmpty($queryJobId);

        // Cancel the job
        $cancelResponse = $this->queryClient->cancelJob($queryJobId, [
            'reason' => 'Test cancellation',
        ]);

        self::assertEquals($queryJobId, $cancelResponse->getQueryJobId());

        // Wait for final status
        $finalStatus = $this->queryClient->waitForJobCompletion($queryJobId, 15);

        // Job should be canceled
        self::assertEquals('canceled', $finalStatus->getStatus());
        self::assertEquals('Test cancellation', $finalStatus->getCancellationReason());
        self::assertNotNull($finalStatus->getCanceledAt());

        // Verify job has statements but don't assert on their status
        $statements = $finalStatus->getStatements();
        self::assertCount(1, $statements);
    }

    public function testQueryJobWithInvalidSQL(): void
    {
        // Submit query with invalid SQL
        $response = $this->queryClient->submitQueryJob(
            $this->getTestBranchId(),
            $this->getTestWorkspaceId(),
            [
            'statements' => ['SELECT * FROM non_existent_table_12345'],
            'transactional' => false,
            ],
        );

        $queryJobId = $response->getQueryJobId();

        // Wait for job completion
        $finalStatus = $this->queryClient->waitForJobCompletion($queryJobId);

        // Job should fail due to invalid SQL
        self::assertEquals('failed', $finalStatus->getStatus());
        $statements = $finalStatus->getStatements();
        self::assertCount(1, $statements);

        $statement = $statements[0];
        self::assertEquals('failed', $statement->getStatus());
        $query = $statement->getQuery();
        self::assertEquals('SELECT * FROM non_existent_table_12345', $query);
    }

    public function testQueryJobWithEmptyStatements(): void
    {
        $this->expectException(ClientException::class);
        $this->expectExceptionMessage('Statements must not be empty');

        $this->queryClient->submitQueryJob(
            $this->getTestBranchId(),
            $this->getTestWorkspaceId(),
            [
            'statements' => [],
            'transactional' => false,
            ],
        );
    }

    public function testQueryJobWithInvalidBranch(): void
    {
        // Submit job with an invalid branch ID
        $this->expectException(ClientException::class);
        $this->expectExceptionMessage('Failed to get workspace credentials');
        $response = $this->queryClient->submitQueryJob(
            '1234567890',
            $this->getTestWorkspaceId(),
            [
            'statements' => ['SELECT 1'],
            'transactional' => false,
            ],
        );
    }

    public function testQueryJobWithInvalidWorkspace(): void
    {
        $this->expectException(ClientException::class);
        $this->expectExceptionMessage('workspace');

        $this->queryClient->submitQueryJob(
            $this->getTestBranchId(),
            'non-existent-workspace-12345',
            [
            'statements' => ['SELECT 1'],
            'transactional' => false,
            ],
        );
    }

    public function testGetJobStatusForNonExistentJob(): void
    {
        $this->expectException(ClientException::class);
        $this->expectExceptionMessage('Invalid job ID format');

        $this->queryClient->getJobStatus('non-existent-job-12345');
    }

    public function testGetJobResultsForNonExistentJob(): void
    {
        $this->expectException(ClientException::class);
        $this->expectExceptionMessage('Invalid job ID format');

        $this->queryClient->getJobResults('non-existent-job-12345', 'non-existent-statement-12345');
    }

    public function testCancelNonExistentJob(): void
    {
        $this->expectException(ClientException::class);
        $this->expectExceptionMessage('Invalid job ID format');

        $this->queryClient->cancelJob('non-existent-job-12345', ['reason' => 'Test']);
    }

    public function testInvalidStorageToken(): void
    {
        // Create a client with an invalid storage token
        $hostnameSuffix = is_string($_ENV['HOSTNAME_SUFFIX']) ? $_ENV['HOSTNAME_SUFFIX'] : '';
        $invalidTokenClient = new Client([
            'url' => sprintf('https://query.%s', $hostnameSuffix),
            'token' => 'invalid-token-12345',
        ]);

        $this->expectException(ClientException::class);
        $this->expectExceptionMessage('Authentication failed');

        // Attempt to submit a query job with invalid token
        $invalidTokenClient->submitQueryJob(
            $this->getTestBranchId(),
            $this->getTestWorkspaceId(),
            [
                'statements' => ['SELECT 1'],
            ],
        );
    }
}
