<?php

declare(strict_types=1);

namespace Keboola\ApiClientBase\Tests;

use Keboola\ApiClientBase\ApiClientConfiguration;
use Keboola\ApiClientBase\Auth\ManageApiTokenAuthenticator;
use PHPUnit\Framework\TestCase;
use Psr\Log\NullLogger;

class ApiClientConfigurationTest extends TestCase
{
    public function testDefaults(): void
    {
        $config = new ApiClientConfiguration();
        self::assertNull($config->authenticator);
        self::assertSame('Keboola PHP API Client', $config->userAgent);
        self::assertSame(5, $config->backoffMaxTries);
        self::assertSame([], $config->retryableStatusCodes);
        self::assertSame(10, $config->connectTimeout);
        self::assertSame(120, $config->requestTimeout);
        self::assertNull($config->requestHandler);
        self::assertInstanceOf(NullLogger::class, $config->logger);
        self::assertNull($config->errorMessageResolver);
    }

    public function testOverrides(): void
    {
        $auth = new ManageApiTokenAuthenticator('t');
        $config = new ApiClientConfiguration(
            authenticator: $auth,
            userAgent: 'My Client',
            backoffMaxTries: 2,
            retryableStatusCodes: [429],
        );
        self::assertSame($auth, $config->authenticator);
        self::assertSame('My Client', $config->userAgent);
        self::assertSame(2, $config->backoffMaxTries);
        self::assertSame([429], $config->retryableStatusCodes);
    }
}
