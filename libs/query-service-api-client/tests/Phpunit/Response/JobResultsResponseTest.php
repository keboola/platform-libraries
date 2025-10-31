<?php

declare(strict_types=1);

namespace Keboola\QueryApi\Tests\Phpunit\Response;

use Generator;
use GuzzleHttp\Psr7\Response;
use Keboola\QueryApi\ClientException;
use Keboola\QueryApi\Response\JobResultsResponse;
use PHPUnit\Framework\TestCase;

class JobResultsResponseTest extends TestCase
{
    public function testJobResultsResponseCreation(): void
    {
        $responseData = [
            'columns' => [
                ['name' => 'id', 'type' => 'string'],
                ['name' => 'name', 'type' => 'string'],
            ],
            'data' => [
                ['1', 'Alice'],
                ['2', 'Bob'],
            ],
            'message' => 'Query executed successfully',
            'numberOfRows' => 2,
            'rowsAffected' => 0,
            'status' => 'completed',
        ];

        $response = new Response(200, [], json_encode($responseData) ?: '');
        $results = JobResultsResponse::fromResponse($response);

        self::assertEquals('completed', $results->getStatus());
        self::assertEquals(2, $results->getNumberOfRows());
        self::assertEquals(0, $results->getRowsAffected());
        self::assertCount(2, $results->getColumns());
        self::assertEquals('Query executed successfully', $results->getMessage());
    }

    public function testJobResultsResponseWithoutOptionalFields(): void
    {
        $responseData = [
            'data' => [['value']],
            'numberOfRows' => 1,
            'rowsAffected' => 0,
            'status' => 'completed',
        ];

        $response = new Response(200, [], json_encode($responseData) ?: '');
        $results = JobResultsResponse::fromResponse($response);

        self::assertEquals('completed', $results->getStatus());
        self::assertEquals(1, $results->getNumberOfRows());
        self::assertEquals(0, $results->getRowsAffected());
        self::assertEmpty($results->getColumns());
        self::assertNull($results->getMessage());
    }

    /**
     * @param array<string, mixed> $responseData
     * @dataProvider missingFieldDataProvider
     */
    public function testJobResultsResponseThrowsExceptionForMissingField(array $responseData): void
    {
        $response = new Response(200, [], json_encode($responseData) ?: '');

        $this->expectException(ClientException::class);
        $this->expectExceptionMessage('Invalid response: missing');

        JobResultsResponse::fromResponse($response);
    }

    public static function missingFieldDataProvider(): Generator
    {
        $requiredFields = [
            'data' => [],
            'status' => 'completed',
            'numberOfRows' => 0,
            'rowsAffected' => 0,
        ];

        foreach (array_keys($requiredFields) as $fieldToRemove) {
            $incompleteData = $requiredFields;
            unset($incompleteData[$fieldToRemove]);
            yield "missing $fieldToRemove" => [$incompleteData];
        }
    }
}
