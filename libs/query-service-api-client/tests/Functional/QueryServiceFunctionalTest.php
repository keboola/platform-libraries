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

        self::assertTrue($result->isOk());
        self::assertSame('ok', $result->getStatus());
    }

    public function testSubmitAndGetSimpleQuery(): void
    {
        $response = $this->queryClient->submitQueryJob(
            $this->getTestBranchId(),
            $this->getTestWorkspaceId(),
            [
                'statements' => ['SELECT 1 as test_value'],
                'transactional' => false,
            ],
        );

        self::assertNotEmpty($response->getQueryJobId());
        $queryJobId = $response->getQueryJobId();

        $finalStatus = $this->queryClient->waitForJobCompletion($queryJobId);

        self::assertTrue($finalStatus->isCompleted());
        self::assertSame($queryJobId, $finalStatus->getQueryJobId());
        self::assertCount(1, $finalStatus->getStatements());

        $statement = $finalStatus->getStatements()[0];
        self::assertIsArray($statement);
        self::assertSame('completed', $statement['status']);

        self::assertArrayHasKey('id', $statement);
        self::assertIsString($statement['id']);
        $results = $this->queryClient->getJobResults($queryJobId, $statement['id']);

        self::assertSame('completed', $results->getStatus());
        self::assertCount(1, $results->getData());
        $row = $results->getData()[0];
        self::assertIsArray($row);
        self::assertSame('1', $row[0]);
    }

    public function testSubmitTransactionalQuery(): void
    {
        $tableName = 'test_table_' . time();

        $response = $this->queryClient->submitQueryJob(
            $this->getTestBranchId(),
            $this->getTestWorkspaceId(),
            [
                'statements' => [
                    sprintf('CREATE OR REPLACE TABLE %s (id INTEGER, name STRING)', $tableName),
                    sprintf('INSERT INTO %s (id, name) VALUES (1, \'test1\'), (2, \'test2\')', $tableName),
                    sprintf('SELECT COUNT(*) as row_count FROM %s', $tableName),
                ],
                'transactional' => true,
            ],
        );

        self::assertNotEmpty($response->getQueryJobId());
        $queryJobId = $response->getQueryJobId();

        $finalStatus = $this->queryClient->waitForJobCompletion($queryJobId);

        self::assertTrue($finalStatus->isCompleted());
        self::assertCount(3, $finalStatus->getStatements());

        $createStatement = $finalStatus->getStatements()[0];
        self::assertIsArray($createStatement);
        self::assertSame('completed', $createStatement['status']);

        $insertStatement = $finalStatus->getStatements()[1];
        self::assertIsArray($insertStatement);
        self::assertSame('completed', $insertStatement['status']);

        $selectStatement = $finalStatus->getStatements()[2];
        self::assertIsArray($selectStatement);
        self::assertSame('completed', $selectStatement['status']);

        self::assertArrayHasKey('id', $selectStatement);
        self::assertIsString($selectStatement['id']);
        $results = $this->queryClient->getJobResults($queryJobId, $selectStatement['id']);
        self::assertCount(1, $results->getData());
        $row = $results->getData()[0];
        self::assertIsArray($row);
        self::assertSame('2', $row[0]);
    }

    public function testCancelQueryJob(): void
    {
        $response = $this->queryClient->submitQueryJob(
            $this->getTestBranchId(),
            $this->getTestWorkspaceId(),
            [
                'statements' => [
                    'CALL SYSTEM$WAIT(10);',
                ],
                'transactional' => false,
            ],
        );

        self::assertNotEmpty($response->getQueryJobId());
        $queryJobId = $response->getQueryJobId();

        $cancelResponse = $this->queryClient->cancelJob($queryJobId, [
            'reason' => 'Test cancellation',
        ]);

        self::assertSame($queryJobId, $cancelResponse->getQueryJobId());

        $finalStatus = $this->queryClient->waitForJobCompletion($queryJobId, 15);

        self::assertTrue($finalStatus->isCanceled());
        self::assertSame('Test cancellation', $finalStatus->getCancellationReason());
        self::assertNotNull($finalStatus->getCanceledAt());

        self::assertCount(1, $finalStatus->getStatements());
    }

    public function testQueryJobWithInvalidSQL(): void
    {
        $response = $this->queryClient->submitQueryJob(
            $this->getTestBranchId(),
            $this->getTestWorkspaceId(),
            [
                'statements' => ['SELECT * FROM non_existent_table_12345'],
                'transactional' => false,
            ],
        );

        self::assertNotEmpty($response->getQueryJobId());
        $queryJobId = $response->getQueryJobId();

        $finalStatus = $this->queryClient->waitForJobCompletion($queryJobId);

        self::assertTrue($finalStatus->isFailed());
        self::assertCount(1, $finalStatus->getStatements());

        $statement = $finalStatus->getStatements()[0];
        self::assertIsArray($statement);
        self::assertSame('failed', $statement['status']);
        self::assertIsString($statement['query']);
        self::assertSame('SELECT * FROM non_existent_table_12345', $statement['query']);
    }

    public function testQueryJobWithEmptyStatements(): void
    {
        $this->expectException(ClientException::class);
        $this->expectExceptionMessageMatches('/statements.*required|empty/i');

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
        $this->expectException(ClientException::class);
        $this->expectExceptionMessage('Failed to get workspace credentials');

        $this->queryClient->submitQueryJob(
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
        $this->expectExceptionMessageMatches('/workspace|Failed to parse/i');

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
        $this->expectExceptionMessageMatches('/not found|does not exist|Invalid.*ID/i');

        $this->queryClient->getJobStatus('non-existent-job-12345');
    }

    public function testGetJobResultsForNonExistentJob(): void
    {
        $this->expectException(ClientException::class);
        $this->expectExceptionMessageMatches('/not found|does not exist|Invalid.*ID/i');

        $this->queryClient->getJobResults('non-existent-job-12345', 'non-existent-statement-12345');
    }

    public function testCancelNonExistentJob(): void
    {
        $this->expectException(ClientException::class);
        $this->expectExceptionMessageMatches('/not found|does not exist|Invalid.*ID/i');

        $this->queryClient->cancelJob('non-existent-job-12345', ['reason' => 'Test']);
    }

    public function testInvalidStorageToken(): void
    {
        $invalidTokenClient = new Client([
            'url' => (string) getenv('QUERY_API_URL'),
            'token' => 'invalid-token-12345',
        ]);

        $this->expectException(ClientException::class);
        $this->expectExceptionMessage('Authentication failed');

        $invalidTokenClient->submitQueryJob(
            $this->getTestBranchId(),
            $this->getTestWorkspaceId(),
            [
                'statements' => ['SELECT 1'],
            ],
        );
    }
}
