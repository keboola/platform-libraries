<?php

declare(strict_types=1);

namespace Keboola\QueryApi\Tests\Phpunit\Response;

use Generator;
use GuzzleHttp\Psr7\Response;
use Keboola\QueryApi\ClientException;
use Keboola\QueryApi\Response\CancelJobResponse;
use PHPUnit\Framework\TestCase;

class CancelJobResponseTest extends TestCase
{
    public function testCancelJobResponseCreation(): void
    {
        $responseData = [
            'queryJobId' => 'job-cancel-123',
        ];

        $response = new Response(200, [], json_encode($responseData) ?: '');
        $cancelResponse = CancelJobResponse::fromResponse($response);

        self::assertEquals('job-cancel-123', $cancelResponse->getQueryJobId());
    }

    /**
     * @param array<string, mixed> $responseData
     * @dataProvider invalidDataProvider
     */
    public function testCancelJobResponseThrowsExceptionForInvalidData(array $responseData): void
    {
        $response = new Response(200, [], json_encode($responseData) ?: '');

        $this->expectException(ClientException::class);
        $this->expectExceptionMessage('Invalid response: missing or invalid queryJobId');

        CancelJobResponse::fromResponse($response);
    }

    public static function invalidDataProvider(): Generator
    {
        yield 'missing field' => [[]];
        yield 'invalid type' => [['queryJobId' => 12345]];
        yield 'null value' => [['queryJobId' => null]];
    }
}
