<?php

declare(strict_types=1);

namespace Keboola\QueryApi\Tests\Functional;

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

        self::assertSame('completed', $finalStatus->getStatus());
        self::assertEquals($queryJobId, $finalStatus->getQueryJobId());
        $statements = $finalStatus->getStatements();
        self::assertCount(1, $statements);

        $statement = $statements[0];
        self::assertSame('completed', $statement->getStatus());

        // Get job results
        $statementId = $statement->getId();
        $resultsResponse = $this->queryClient->getJobResults($queryJobId, $statementId);
        $resultsResponse = ResultHelper::mapColumnNamesIntoData($resultsResponse);

        self::assertSame('completed', $resultsResponse->getStatus());
        self::assertGreaterThanOrEqual(1, $resultsResponse->getNumberOfRows());

        // Verify it's a valid timestamp (numeric string)

        /**
         * @var array{current_time: string}[] $data
         */
        $data = $resultsResponse->getData();
        self::assertMatchesRegularExpression('/^\d+\.\d+$/', $data[0]['current_time']);
    }
}
