<?php

declare(strict_types=1);

namespace Keboola\VaultApiClient\Tests;

use Keboola\VaultApiClient\VaultErrorMessageResolver;
use PHPUnit\Framework\TestCase;

class VaultErrorMessageResolverTest extends TestCase
{
    private VaultErrorMessageResolver $resolver;

    protected function setUp(): void
    {
        $this->resolver = new VaultErrorMessageResolver();
    }

    public function testResolvesCodeAndError(): void
    {
        self::assertSame(
            'X: msg',
            ($this->resolver)('{"code":"X","error":"msg"}', 400),
        );
    }

    public function testTrimsWhitespace(): void
    {
        self::assertSame(
            '400 :  some error',
            ($this->resolver)('{"code":" 400 ","error":" some error "}', 400),
        );
    }

    public function testReturnsNullWhenCodeMissing(): void
    {
        self::assertNull(
            ($this->resolver)('{"error":"msg"}', 400),
        );
    }

    public function testReturnsNullWhenErrorMissing(): void
    {
        self::assertNull(
            ($this->resolver)('{"code":"X"}', 400),
        );
    }

    public function testReturnsNullWhenCodeEmpty(): void
    {
        self::assertNull(
            ($this->resolver)('{"code":"","error":"msg"}', 400),
        );
    }

    public function testReturnsNullWhenErrorEmpty(): void
    {
        self::assertNull(
            ($this->resolver)('{"code":"X","error":""}', 400),
        );
    }

    public function testReturnsNullOnInvalidJson(): void
    {
        self::assertNull(
            ($this->resolver)('not-valid-json', 400),
        );
    }

    public function testReturnsNullOnEmptyBody(): void
    {
        self::assertNull(
            ($this->resolver)('', 400),
        );
    }

    public function testStatusCodeDoesNotAffectResult(): void
    {
        self::assertSame(
            '404: Not found',
            ($this->resolver)('{"code":"404","error":"Not found"}', 404),
        );
        self::assertSame(
            '404: Not found',
            ($this->resolver)('{"code":"404","error":"Not found"}', 500),
        );
    }
}
