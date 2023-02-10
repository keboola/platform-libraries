<?php

declare(strict_types=1);

namespace Keboola\AzureApiClient\Tests\Authentication;

use Keboola\AzureApiClient\Authentication\StaticTokenCredentialsAuthenticator;
use PHPUnit\Framework\TestCase;

class StaticTokenCredentialsAuthenticatorTest extends TestCase
{
    public function testGetAuthenticateToken(): void
    {
        $authenticator = new StaticTokenCredentialsAuthenticator('tokenString');
        $this->assertSame('tokenString', $authenticator->getAuthenticationToken('does not matter'));
    }
}
