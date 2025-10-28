<?php

declare(strict_types=1);

namespace Keboola\QueryApi\Tests\Phpunit\Response;

use Generator;
use GuzzleHttp\Psr7\Response;
use Keboola\QueryApi\ClientException;
use Keboola\QueryApi\Response\JobStatusResponse;
use PHPUnit\Framework\TestCase;

class JobStatusResponseTest extends TestCase
{
    public function testJobStatusResponseCreationWithAllFields(): void
    {
        $responseData = [
            'queryJobId' => 'job-123',
            'status' => 'processing',
            'actorType' => 'user',
            'createdAt' => '2024-01-01T10:00:00Z',
            'changedAt' => '2024-01-01T10:01:00Z',
            'statements' => [
                [
                    'id' => 'stmt-1',
                    'queryId' => 'query-1',
                    'sessionId' => 'session-1',
                    'query' => 'SELECT * FROM table',
                    'status' => 'waiting',
                    'numberOfRows' => 0,
                    'rowsAffected' => 0,
                    'executedAt' => '2024-01-01T10:00:05Z',
                    'completedAt' => '2024-01-01T10:00:10Z',
                    'createdAt' => '2024-01-01T10:00:00Z',
                ],
            ],
        ];

        $response = new Response(200, [], json_encode($responseData) ?: '');
        $jobStatus = JobStatusResponse::fromResponse($response);

        self::assertEquals('job-123', $jobStatus->getQueryJobId());
        self::assertEquals('processing', $jobStatus->getStatus());
        self::assertEquals('user', $jobStatus->getActorType());
        self::assertEquals('2024-01-01T10:00:00Z', $jobStatus->getCreatedAt());
        self::assertEquals('2024-01-01T10:01:00Z', $jobStatus->getChangedAt());
        self::assertNull($jobStatus->getCanceledAt());
        self::assertNull($jobStatus->getCancellationReason());
        self::assertCount(1, $jobStatus->getStatements());
    }

    public function testJobStatusResponseWithCancellationFields(): void
    {
        $responseData = [
            'queryJobId' => 'job-456',
            'status' => 'canceled',
            'actorType' => 'user',
            'createdAt' => '2024-01-01T11:00:00Z',
            'changedAt' => '2024-01-01T11:02:00Z',
            'canceledAt' => '2024-01-01T11:02:10Z',
            'cancellationReason' => 'User requested cancellation',
            'statements' => [],
        ];

        $response = new Response(200, [], json_encode($responseData) ?: '');
        $jobStatus = JobStatusResponse::fromResponse($response);

        self::assertEquals('job-456', $jobStatus->getQueryJobId());
        self::assertEquals('canceled', $jobStatus->getStatus());
        self::assertEquals('2024-01-01T11:02:10Z', $jobStatus->getCanceledAt());
        self::assertEquals('User requested cancellation', $jobStatus->getCancellationReason());
    }

    /**
     * @param array<string, mixed> $responseData
     * @dataProvider missingFieldDataProvider
     */
    public function testJobStatusResponseThrowsExceptionForMissingField(
        array $responseData,
        string $expectedField,
    ): void {
        $response = new Response(200, [], json_encode($responseData) ?: '');

        $this->expectException(ClientException::class);
        $this->expectExceptionMessage("Invalid response: missing or invalid $expectedField");

        JobStatusResponse::fromResponse($response);
    }

    public static function missingFieldDataProvider(): Generator
    {
        $requiredFields = [
            'queryJobId' => 'job-789',
            'status' => 'running',
            'actorType' => 'user',
            'changedAt' => '2024-01-01T11:00:00Z',
            'createdAt' => '2024-01-01T10:00:00Z',
            'statements' => [],
        ];

        foreach (['queryJobId', 'status', 'actorType', 'changedAt', 'createdAt'] as $fieldToRemove) {
            $incompleteData = $requiredFields;
            unset($incompleteData[$fieldToRemove]);
            yield "missing $fieldToRemove" => [$incompleteData, $fieldToRemove];
        }
    }
}
