<?php

declare(strict_types=1);

namespace Keboola\GitServiceApiClient\Tests;

use Keboola\GitServiceApiClient\ApiClientConfiguration;
use Keboola\GitServiceApiClient\Auth\KeboolaServiceAccountAuth;
use PHPUnit\Framework\TestCase;

class ApiClientConfigurationTest extends TestCase
{
    public function testDefaultAuthIsKeboolaServiceAccount(): void
    {
        $config = new ApiClientConfiguration();

        self::assertInstanceOf(KeboolaServiceAccountAuth::class, $config->auth);
    }
}
