<?php

declare(strict_types=1);

namespace Keboola\QueryApi\Tests\Phpunit\Response;

use Generator;
use Keboola\QueryApi\Response\CancelJobResponse;
use PHPUnit\Framework\TestCase;
use Webmozart\Assert\InvalidArgumentException;

class CancelJobResponseTest extends TestCase
{
    public function testCancelJobResponseCreation(): void
    {
        $response = CancelJobResponse::fromResponseData(['queryJobId' => 'job-cancel-123']);

        self::assertSame('job-cancel-123', $response->getQueryJobId());
    }

    /**
     * @param array<string, mixed> $responseData
     * @dataProvider invalidDataProvider
     */
    public function testCancelJobResponseThrowsExceptionForInvalidData(array $responseData): void
    {
        $this->expectException(InvalidArgumentException::class);

        CancelJobResponse::fromResponseData($responseData);
    }

    public static function invalidDataProvider(): Generator
    {
        yield 'missing field' => [[]];
        yield 'invalid type' => [['queryJobId' => 12345]];
        yield 'null value' => [['queryJobId' => null]];
    }
}
