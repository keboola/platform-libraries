<?php

declare(strict_types=1);

namespace Keboola\ApiClientBase\Tests\Exception;

use Keboola\ApiClientBase\Exception\ClientException;
use PHPUnit\Framework\TestCase;
use RuntimeException;

class ClientExceptionTest extends TestCase
{
    public function testIsRuntimeException(): void
    {
        $e = new ClientException('boom', 500);
        self::assertInstanceOf(RuntimeException::class, $e);
        self::assertSame('boom', $e->getMessage());
        self::assertSame(500, $e->getCode());
    }
}
