<?php

declare(strict_types=1);

namespace Keboola\AppsApiClient\Tests;

use Generator;
use Keboola\AppsApiClient\AppsApiErrorMessageResolver;
use PHPUnit\Framework\TestCase;

class AppsApiErrorMessageResolverTest extends TestCase
{
    /**
     * @dataProvider responseBodyProvider
     */
    public function testResolve(string $responseBody, ?string $expectedMessage): void
    {
        self::assertSame($expectedMessage, (new AppsApiErrorMessageResolver())($responseBody, 400));
    }

    public static function responseBodyProvider(): Generator
    {
        yield 'error message' => ['{"error": "App \"123\" not found"}', 'App "123" not found'];
        yield 'empty error' => ['{"error": ""}', null];
        yield 'non-string error' => ['{"error": 42}', null];
        yield 'no error key' => ['{"message": "Something"}', null];
        yield 'not json' => ['<html>Bad Gateway</html>', null];
    }
}
