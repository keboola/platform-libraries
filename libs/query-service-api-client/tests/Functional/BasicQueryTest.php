<?php

declare(strict_types=1);

namespace Keboola\QueryApi\Tests\Functional;

use Keboola\QueryApi\ClientException;

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

        self::assertArrayHasKey('queryJobId', $response);
        $queryJobId = $response['queryJobId'];
        self::assertIsString($queryJobId);
        self::assertNotEmpty($queryJobId);

        // Wait for job completion
        $finalStatus = $this->queryClient->waitForJobCompletion($queryJobId);

        self::assertEquals('completed', $finalStatus['status']);
        self::assertEquals($queryJobId, $finalStatus['queryJobId']);
        self::assertArrayHasKey('statements', $finalStatus);
        $statements = $finalStatus['statements'];
        self::assertIsArray($statements);
        self::assertCount(1, $statements);

        $statement = $statements[0];
        self::assertIsArray($statement);
        self::assertEquals('completed', $statement['status']);

        // Get job results
        self::assertArrayHasKey('id', $statement);
        $results = $this->queryClient->getJobResults($queryJobId, $statement['id']);

        self::assertArrayHasKey('status', $results);
        self::assertEquals('completed', $results['status']);

        // Verify we got a timestamp result
        self::assertArrayHasKey('data', $results);
        $data = $results['data'];
        self::assertIsArray($data);
        self::assertCount(1, $data);
        $row = $data[0];
        self::assertIsArray($row);
        self::assertCount(1, $row);
        // Query API returns indexed arrays, not associative arrays with column names
        self::assertArrayHasKey(0, $row);
        self::assertIsString($row[0]);
        self::assertNotEmpty($row[0]);
        // Verify it's a valid timestamp (numeric string)
        self::assertMatchesRegularExpression('/^\d+\.\d+$/', $row[0]);
    }

    public function testSubmitInformationSchemaQuery(): void
    {
        // Test a query against information_schema to verify database connectivity
        $response = $this->queryClient->submitQueryJob(
            $this->getTestBranchId(),
            $this->getTestWorkspaceId(),
            [
                'statements' => [
                    'SELECT COUNT(*) AS "table_count" FROM information_schema.tables ' .
                    'WHERE table_schema = CURRENT_SCHEMA()',
                ],
                'transactional' => false,
            ],
        );

        self::assertArrayHasKey('queryJobId', $response);
        $queryJobId = $response['queryJobId'];
        self::assertIsString($queryJobId);
        self::assertNotEmpty($queryJobId);

        // Wait for job completion
        $finalStatus = $this->queryClient->waitForJobCompletion($queryJobId);

        self::assertEquals('completed', $finalStatus['status']);
        self::assertEquals($queryJobId, $finalStatus['queryJobId']);
        self::assertArrayHasKey('statements', $finalStatus);
        $statements = $finalStatus['statements'];
        self::assertIsArray($statements);
        self::assertCount(1, $statements);

        $statement = $statements[0];
        self::assertIsArray($statement);
        self::assertEquals('completed', $statement['status']);

        // Get job results
        self::assertArrayHasKey('id', $statement);
        $results = $this->queryClient->getJobResults($queryJobId, $statement['id']);

        self::assertArrayHasKey('status', $results);
        self::assertEquals('completed', $results['status']);

        // Verify we got a count result
        self::assertArrayHasKey('data', $results);
        $data = $results['data'];
        self::assertIsArray($data);
        self::assertCount(1, $data);
        $row = $data[0];
        self::assertIsArray($row);
        self::assertCount(1, $row);
        // Query API returns indexed arrays, not associative arrays with column names
        self::assertArrayHasKey(0, $row);
        self::assertIsNumeric($row[0]);
        self::assertGreaterThanOrEqual(0, (int) $row[0]);
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

        // Verify the response structure
        self::assertArrayHasKey('queryJobId', $response);
        self::assertArrayHasKey('status', $response);
        self::assertArrayHasKey('statements', $response);
        self::assertArrayHasKey('results', $response);

        // Verify job completed successfully
        self::assertEquals('completed', $response['status']);
        self::assertNotEmpty($response['queryJobId']);

        // Verify statements
        $statements = $response['statements'];
        self::assertIsArray($statements);
        self::assertCount(1, $statements);

        $statement = $statements[0];
        self::assertIsArray($statement);
        self::assertEquals('completed', $statement['status']);

        // Verify results
        $results = $response['results'];
        self::assertIsArray($results);
        self::assertCount(1, $results);

        $result = $results[0];
        self::assertIsArray($result);
        self::assertEquals('completed', $result['status']);

        // Verify we got timestamp data
        self::assertArrayHasKey('data', $result);
        $data = $result['data'];
        self::assertIsArray($data);
        self::assertCount(1, $data);
        $row = $data[0];
        self::assertIsArray($row);
        self::assertCount(1, $row);
        // Query API returns indexed arrays, not associative arrays with column names
        self::assertArrayHasKey(0, $row);
        self::assertIsString($row[0]);
        self::assertNotEmpty($row[0]);
        // Verify it's a valid timestamp (numeric string)
        self::assertMatchesRegularExpression('/^\d+\.\d+$/', $row[0]);
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
