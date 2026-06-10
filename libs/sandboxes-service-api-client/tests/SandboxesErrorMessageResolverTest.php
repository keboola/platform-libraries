<?php

declare(strict_types=1);

namespace Keboola\SandboxesServiceApiClient\Tests;

use Generator;
use Keboola\SandboxesServiceApiClient\SandboxesErrorMessageResolver;
use PHPUnit\Framework\TestCase;

class SandboxesErrorMessageResolverTest extends TestCase
{
    private SandboxesErrorMessageResolver $resolver;

    protected function setUp(): void
    {
        $this->resolver = new SandboxesErrorMessageResolver();
    }

    public function testCombinedErrorAndMessageFormat(): void
    {
        $body = json_encode(['error' => 'NotFound', 'message' => 'Sandbox does not exist'], JSON_THROW_ON_ERROR);
        $result = ($this->resolver)($body, 404);

        self::assertSame('NotFound: Sandbox does not exist', $result);
    }

    public function testTrimsWhitespace(): void
    {
        $body = json_encode(['error' => ' Conflict ', 'message' => ' already exists '], JSON_THROW_ON_ERROR);
        $result = ($this->resolver)($body, 409);

        self::assertSame('Conflict :  already exists', $result);
    }

    /**
     * @dataProvider nullCasesProvider
     */
    public function testReturnsNullForNullCases(string $body, int $statusCode): void
    {
        $result = ($this->resolver)($body, $statusCode);

        self::assertNull($result);
    }

    public function nullCasesProvider(): Generator
    {
        yield 'invalid JSON' => [
            'body' => 'not json at all',
            'statusCode' => 500,
        ];

        yield 'empty body' => [
            'body' => '',
            'statusCode' => 400,
        ];

        yield 'JSON array instead of object' => [
            'body' => '["error","message"]',
            'statusCode' => 400,
        ];

        yield 'missing error field' => [
            'body' => json_encode(['message' => 'Something went wrong'], JSON_THROW_ON_ERROR),
            'statusCode' => 400,
        ];

        yield 'missing message field' => [
            'body' => json_encode(['error' => 'SomeError'], JSON_THROW_ON_ERROR),
            'statusCode' => 400,
        ];

        yield 'empty error string' => [
            'body' => json_encode(['error' => '', 'message' => 'Something went wrong'], JSON_THROW_ON_ERROR),
            'statusCode' => 400,
        ];

        yield 'empty message string' => [
            'body' => json_encode(['error' => 'SomeError', 'message' => ''], JSON_THROW_ON_ERROR),
            'statusCode' => 400,
        ];

        yield 'error is not a string' => [
            'body' => json_encode(['error' => 42, 'message' => 'Something went wrong'], JSON_THROW_ON_ERROR),
            'statusCode' => 400,
        ];

        yield 'message is not a string' => [
            'body' => json_encode(['error' => 'SomeError', 'message' => ['nested' => 'array']], JSON_THROW_ON_ERROR),
            'statusCode' => 400,
        ];

        yield 'both fields null' => [
            'body' => json_encode(['error' => null, 'message' => null], JSON_THROW_ON_ERROR),
            'statusCode' => 500,
        ];
    }

    public function testStatusCodeIsNotUsedInLogic(): void
    {
        $body = json_encode(['error' => 'Forbidden', 'message' => 'Access denied'], JSON_THROW_ON_ERROR);

        self::assertSame('Forbidden: Access denied', ($this->resolver)($body, 403));
        self::assertSame('Forbidden: Access denied', ($this->resolver)($body, 200));
        self::assertSame('Forbidden: Access denied', ($this->resolver)($body, 500));
    }
}
