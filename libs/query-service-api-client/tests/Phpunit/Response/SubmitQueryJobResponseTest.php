<?php

declare(strict_types=1);

namespace Keboola\QueryApi\Tests\Phpunit\Response;

use Generator;
use GuzzleHttp\Psr7\Response;
use Keboola\QueryApi\ClientException;
use Keboola\QueryApi\Response\SubmitQueryJobResponse;
use PHPUnit\Framework\TestCase;

class SubmitQueryJobResponseTest extends TestCase
{
    public function testSubmitQueryJobResponseCreation(): void
    {
        $responseData = [
            'queryJobId' => 'job-submit-456',
        ];

        $response = new Response(201, [], json_encode($responseData) ?: '');
        $submitResponse = SubmitQueryJobResponse::fromResponse($response);

        self::assertEquals('job-submit-456', $submitResponse->getQueryJobId());
    }

    /**
     * @param array<string, mixed> $responseData
     * @dataProvider invalidDataProvider
     */
    public function testSubmitQueryJobResponseThrowsExceptionForInvalidData(array $responseData): void
    {
        $response = new Response(201, [], json_encode($responseData) ?: '');

        $this->expectException(ClientException::class);
        $this->expectExceptionMessage('Invalid response: missing or invalid queryJobId');

        SubmitQueryJobResponse::fromResponse($response);
    }

    public static function invalidDataProvider(): Generator
    {
        yield 'missing field' => [[]];
        yield 'invalid type' => [['queryJobId' => 99999]];
        yield 'null value' => [['queryJobId' => null]];
    }
}
