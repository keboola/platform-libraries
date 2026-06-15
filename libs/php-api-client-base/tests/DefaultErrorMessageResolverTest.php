<?php

declare(strict_types=1);

namespace Keboola\ApiClientBase\Tests;

use Keboola\ApiClientBase\DefaultErrorMessageResolver;
use PHPUnit\Framework\TestCase;

class DefaultErrorMessageResolverTest extends TestCase
{
    private DefaultErrorMessageResolver $resolver;

    protected function setUp(): void
    {
        $this->resolver = new DefaultErrorMessageResolver();
    }

    public function testReturnsErrorField(): void
    {
        $result = ($this->resolver)('{"error":"something went wrong"}', 400);
        self::assertSame('something went wrong', $result);
    }

    public function testFallsBackToMessageField(): void
    {
        $result = ($this->resolver)('{"message":"bad request"}', 400);
        self::assertSame('bad request', $result);
    }

    public function testPrefersErrorOverMessage(): void
    {
        $result = ($this->resolver)('{"error":"error value","message":"message value"}', 400);
        self::assertSame('error value', $result);
    }

    public function testReturnsNullWhenNeitherPresent(): void
    {
        $result = ($this->resolver)('{"code":42}', 400);
        self::assertNull($result);
    }

    public function testReturnsNullOnInvalidJson(): void
    {
        $result = ($this->resolver)('not json at all', 400);
        self::assertNull($result);
    }

    public function testReturnsNullOnEmptyErrorField(): void
    {
        $result = ($this->resolver)('{"error":""}', 400);
        self::assertNull($result);
    }

    public function testReturnsNullOnNonStringErrorField(): void
    {
        $result = ($this->resolver)('{"error":123}', 400);
        self::assertNull($result);
    }
}
