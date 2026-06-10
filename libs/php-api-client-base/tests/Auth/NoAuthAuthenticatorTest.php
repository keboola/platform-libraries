<?php

declare(strict_types=1);

namespace Keboola\ApiClientBase\Tests\Auth;

use Keboola\ApiClientBase\Auth\NoAuthAuthenticator;
use PHPUnit\Framework\TestCase;

class NoAuthAuthenticatorTest extends TestCase
{
    public function testReturnsNoHeaders(): void
    {
        $authenticator = new NoAuthAuthenticator();

        self::assertSame([], $authenticator->getAuthenticationHeaders());
    }
}
