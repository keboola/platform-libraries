<?php

declare(strict_types=1);

namespace Keboola\VaultApiClient\Tests;

use InvalidArgumentException;
use Keboola\VaultApiClient\ApiClientConfiguration;
use PHPUnit\Framework\TestCase;

class ApiClientConfigurationTest extends TestCase
{
    public function testInvalidBackoffMaxTries(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Backoff max tries must be greater than or equal to 0');
        new ApiClientConfiguration(backoffMaxTries: -1); // @phpstan-ignore-line
    }
}
