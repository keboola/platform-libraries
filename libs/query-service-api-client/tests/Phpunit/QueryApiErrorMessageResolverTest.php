<?php

declare(strict_types=1);

namespace Keboola\QueryApi\Tests\Phpunit;

use Keboola\ApiClientBase\Json;
use Keboola\QueryApi\QueryApiErrorMessageResolver;
use PHPUnit\Framework\TestCase;

class QueryApiErrorMessageResolverTest extends TestCase
{
    public function testReadsExceptionField(): void
    {
        $resolver = new QueryApiErrorMessageResolver();
        $body = Json::encodeArray(['exception' => 'Invalid job ID format']);
        self::assertSame('Invalid job ID format', $resolver($body, 400));
    }

    public function testReturnsNullForNonJsonBody(): void
    {
        $resolver = new QueryApiErrorMessageResolver();
        self::assertNull($resolver('<html>oops</html>', 500));
    }

    public function testReturnsNullWhenExceptionFieldMissingOrEmpty(): void
    {
        $resolver = new QueryApiErrorMessageResolver();
        self::assertNull($resolver(Json::encodeArray(['message' => 'x']), 400));
        self::assertNull($resolver(Json::encodeArray(['exception' => '']), 400));
    }
}
