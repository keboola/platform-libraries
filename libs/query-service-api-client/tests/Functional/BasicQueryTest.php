<?php

declare(strict_types=1);

namespace Keboola\QueryApi\Tests\Functional;

use Keboola\QueryApi\ClientException;

class BasicQueryTest extends BaseFunctionalTestCase
{
    public function testSubmitSimpleSelectQuery(): void
    {
        $response = $this->queryClient->submitQueryJob(
            $this->getTestBranchId(),
            $this->getTestWorkspaceId(),
            [
                'statements' => ['SELECT CURRENT_TIMESTAMP() AS "current_time"'],
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
        self::assertCount(1, $row);
        self::assertArrayHasKey(0, $row);
        self::assertIsString($row[0]);
        self::assertNotEmpty($row[0]);
        self::assertMatchesRegularExpression('/^\d+\.\d+$/', $row[0]);
    }

    public function testSubmitInformationSchemaQuery(): void
    {
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
        self::assertCount(1, $row);
        self::assertArrayHasKey(0, $row);
        self::assertIsNumeric($row[0]);
        self::assertGreaterThanOrEqual(0, (int) $row[0]);
    }

    public function testExecuteWorkspaceQuery(): void
    {
        $response = $this->queryClient->executeWorkspaceQuery(
            $this->getTestBranchId(),
            $this->getTestWorkspaceId(),
            [
                'statements' => ['SELECT CURRENT_TIMESTAMP() AS "current_time"'],
                'transactional' => false,
            ],
        );

        self::assertNotEmpty($response->getQueryJobId());
        self::assertTrue($response->getStatus() === 'completed');

        self::assertCount(1, $response->getStatements());
        $statement = $response->getStatements()[0];
        self::assertIsArray($statement);
        self::assertSame('completed', $statement['status']);

        self::assertCount(1, $response->getResults());
        $result = $response->getResults()[0];
        self::assertSame('completed', $result->getStatus());

        self::assertCount(1, $result->getData());
        $row = $result->getData()[0];
        self::assertIsArray($row);
        self::assertCount(1, $row);
        self::assertArrayHasKey(0, $row);
        self::assertIsString($row[0]);
        self::assertNotEmpty($row[0]);
        self::assertMatchesRegularExpression('/^\d+\.\d+$/', $row[0]);
    }

    public function testExecuteInvalidWorkspaceQuery(): void
    {
        $this->expectException(ClientException::class);
        $this->expectExceptionMessage('\'COOTIES\' does not exist or not authorized');
        $this->expectExceptionCode(400);

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
