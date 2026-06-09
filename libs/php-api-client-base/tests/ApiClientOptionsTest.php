<?php

declare(strict_types=1);

namespace Keboola\ApiClientBase\Tests;

use Keboola\ApiClientBase\ApiClientOptions;
use PHPUnit\Framework\TestCase;
use Psr\Log\NullLogger;

class ApiClientOptionsTest extends TestCase
{
    public function testDefaults(): void
    {
        $options = new ApiClientOptions();
        self::assertSame('Keboola PHP API Client', $options->userAgent);
        self::assertSame(5, $options->backoffMaxTries);
        self::assertSame(10, $options->connectTimeout);
        self::assertSame(120, $options->requestTimeout);
        self::assertNull($options->requestHandler);
        self::assertInstanceOf(NullLogger::class, $options->logger);
    }

    public function testOverrides(): void
    {
        $options = new ApiClientOptions(
            userAgent: 'My Client',
            backoffMaxTries: 2,
        );
        self::assertSame('My Client', $options->userAgent);
        self::assertSame(2, $options->backoffMaxTries);
    }
}
