<?php

declare(strict_types=1);

namespace Keboola\ApiClientBase\Tests\Exception;

use Keboola\ApiClientBase\Exception\ClientException;
use PHPUnit\Framework\TestCase;
use RuntimeException;

class ClientExceptionTest extends TestCase
{
    public function testDefaults(): void
    {
        $e = new ClientException('boom');

        self::assertInstanceOf(RuntimeException::class, $e);
        self::assertSame('boom', $e->getMessage());
        self::assertSame(0, $e->getCode());
        self::assertNull($e->getPrevious());
        self::assertNull($e->getStatusCode());
        self::assertNull($e->getResponseBody());
    }

    public function testCarriesContext(): void
    {
        $previous = new RuntimeException('prev');
        $e = new ClientException('boom', 500, $previous, 404, '{"error":"not found"}');

        self::assertSame('boom', $e->getMessage());
        self::assertSame(500, $e->getCode());
        self::assertSame($previous, $e->getPrevious());
        self::assertSame(404, $e->getStatusCode());
        self::assertSame('{"error":"not found"}', $e->getResponseBody());
    }
}
