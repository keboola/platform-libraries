<?php

declare(strict_types=1);

namespace Keboola\QueryApi\Tests\Phpunit\Response;

use Keboola\QueryApi\Response\JobResultsResponse;
use Keboola\QueryApi\Response\Statement;
use Keboola\QueryApi\Response\WorkspaceQueryResponse;
use PHPUnit\Framework\TestCase;

class WorkspaceQueryResponseTest extends TestCase
{
    public function testWorkspaceQueryResponseCreation(): void
    {
        $statements = [
            new Statement([
                'id' => 'stmt-1',
                'queryId' => 'query-1',
                'sessionId' => 'session-1',
                'query' => 'SELECT 1',
                'status' => 'completed',
                'numberOfRows' => 1,
                'rowsAffected' => 0,
                'executedAt' => '2024-01-01T10:00:00Z',
                'completedAt' => '2024-01-01T10:00:10Z',
                'createdAt' => '2024-01-01T10:00:00Z',
            ]),
        ];

        $results = [
            $this->createMock(JobResultsResponse::class),
        ];

        $workspaceResponse = new WorkspaceQueryResponse(
            'job-123',
            'completed',
            $statements,
            $results,
        );

        self::assertEquals('job-123', $workspaceResponse->getQueryJobId());
        self::assertEquals('completed', $workspaceResponse->getStatus());
        self::assertCount(1, $workspaceResponse->getStatements());
        self::assertCount(1, $workspaceResponse->getResults());
    }
}
