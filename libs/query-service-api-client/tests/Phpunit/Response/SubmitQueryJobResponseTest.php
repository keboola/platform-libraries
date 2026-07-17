<?php

declare(strict_types=1);

namespace Keboola\QueryApi\Tests\Phpunit\Response;

use Generator;
use Keboola\QueryApi\Response\SubmitQueryJobResponse;
use PHPUnit\Framework\TestCase;
use Webmozart\Assert\InvalidArgumentException;

class SubmitQueryJobResponseTest extends TestCase
{
    public function testSubmitQueryJobResponseCreation(): void
    {
        $response = SubmitQueryJobResponse::fromResponseData(['queryJobId' => 'job-submit-456']);

        self::assertSame('job-submit-456', $response->getQueryJobId());
    }

    /**
     * @param array<string, mixed> $responseData
     * @dataProvider invalidDataProvider
     */
    public function testThrowsForInvalidData(array $responseData): void
    {
        $this->expectException(InvalidArgumentException::class);

        SubmitQueryJobResponse::fromResponseData($responseData);
    }

    public static function invalidDataProvider(): Generator
    {
        yield 'missing field' => [[]];
        yield 'invalid type' => [['queryJobId' => 99999]];
        yield 'null value' => [['queryJobId' => null]];
    }
}
