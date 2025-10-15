<?php

declare(strict_types=1);

namespace Keboola\QueryApi\Tests\Functional;

use Keboola\QueryApi\Client;
use Keboola\QueryApi\ClientException;

class QueryServiceFunctionalTest extends BaseFunctionalTestCase
{
    public function testHealthCheck(): void
    {
        $result = $this->queryClient->healthCheck();

        self::assertArrayHasKey('status', $result);
        self::assertEquals('ok', $result['status']);
    }

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

        self::assertArrayHasKey('queryJobId', $response);
        $queryJobId = $response['queryJobId'];
        assert(is_string($queryJobId));
        self::assertNotEmpty($queryJobId);

        // Wait for job completion
        $finalStatus = $this->queryClient->waitForJobCompletion($queryJobId);

        self::assertEquals('completed', $finalStatus['status']);
        self::assertEquals($queryJobId, $finalStatus['queryJobId']);
        self::assertArrayHasKey('statements', $finalStatus);
        $statements = $finalStatus['statements'];
        assert(is_array($statements));
        self::assertCount(1, $statements);

        $statement = $statements[0];
        assert(is_array($statement));
        self::assertEquals('completed', $statement['status']);

        // Get job results
        self::assertArrayHasKey('id', $statement);
        $results = $this->queryClient->getJobResults($queryJobId, $statement['id']);

        self::assertArrayHasKey('data', $results);
        self::assertArrayHasKey('status', $results);
        self::assertEquals('completed', $results['status']);

        // Verify the result contains our count
        self::assertArrayHasKey('data', $results);
        $data = $results['data'];
        assert(is_array($data));
        self::assertCount(1, $data);
        $row = $data[0];
        assert(is_array($row));
        self::assertEquals(3, $row[0]); // We inserted 3 rows
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

        self::assertArrayHasKey('queryJobId', $response);
        $queryJobId = $response['queryJobId'];
        assert(is_string($queryJobId));

        // Wait for completion
        $finalStatus = $this->queryClient->waitForJobCompletion($queryJobId);

        self::assertEquals('completed', $finalStatus['status']);
        self::assertArrayHasKey('statements', $finalStatus);
        $statements = $finalStatus['statements'];
        assert(is_array($statements));
        self::assertCount(2, $statements);

        // Check INSERT statement
        $insertStatement = $statements[0];
        assert(is_array($insertStatement));
        self::assertEquals('completed', $insertStatement['status']);

        // Check SELECT statement and its results
        $selectStatement = $statements[1];
        assert(is_array($selectStatement));
        self::assertEquals('completed', $selectStatement['status']);

        self::assertArrayHasKey('id', $selectStatement);
        $results = $this->queryClient->getJobResults($queryJobId, $selectStatement['id']);
        self::assertArrayHasKey('data', $results);
        $data = $results['data'];
        assert(is_array($data));
        $row = $data[0];
        assert(is_array($row));
        self::assertEquals(4, $row[0]); // Should be 4 rows now
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

        self::assertArrayHasKey('queryJobId', $response);
        $queryJobId = $response['queryJobId'];
        assert(is_string($queryJobId));
        self::assertNotEmpty($queryJobId);

        // Cancel the job
        $cancelResponse = $this->queryClient->cancelJob($queryJobId, [
            'reason' => 'Test cancellation',
        ]);

        self::assertEquals($queryJobId, $cancelResponse['queryJobId']);

        // Wait for final status
        $finalStatus = $this->queryClient->waitForJobCompletion($queryJobId, 15);

        // Job should be canceled
        self::assertEquals('canceled', $finalStatus['status']);
        self::assertArrayHasKey('cancellationReason', $finalStatus);
        self::assertEquals('Test cancellation', $finalStatus['cancellationReason']);
        self::assertArrayHasKey('canceledAt', $finalStatus);

        // Verify job has statements but don't assert on their status
        self::assertArrayHasKey('statements', $finalStatus);
        $statements = $finalStatus['statements'];
        assert(is_array($statements));
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

        self::assertArrayHasKey('queryJobId', $response);
        $queryJobId = $response['queryJobId'];
        assert(is_string($queryJobId));

        // Wait for job completion
        $finalStatus = $this->queryClient->waitForJobCompletion($queryJobId);

        // Job should fail due to invalid SQL
        self::assertEquals('failed', $finalStatus['status']);
        self::assertArrayHasKey('statements', $finalStatus);
        $statements = $finalStatus['statements'];
        assert(is_array($statements));
        self::assertCount(1, $statements);

        $statement = $statements[0];
        assert(is_array($statement));
        self::assertEquals('failed', $statement['status']);
        assert(is_string($statement['query']));
        self::assertEquals('SELECT * FROM non_existent_table_12345', $statement['query']);
    }

    public function testQueryJobWithEmptyStatements(): void
    {
        $this->expectException(ClientException::class);

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

        $this->queryClient->getJobStatus('non-existent-job-12345');
    }

    public function testGetJobResultsForNonExistentJob(): void
    {
        $this->expectException(ClientException::class);

        $this->queryClient->getJobResults('non-existent-job-12345', 'non-existent-statement-12345');
    }

    public function testCancelNonExistentJob(): void
    {
        $this->expectException(ClientException::class);

        $this->queryClient->cancelJob('non-existent-job-12345', ['reason' => 'Test']);
    }

    public function testInvalidStorageToken(): void
    {
        // Create a client with an invalid storage token
        $invalidTokenClient = new Client([
            'url' => $_ENV['QUERY_API_URL'],
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
