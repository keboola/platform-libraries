<?php

declare(strict_types=1);

namespace Keboola\AzureApiClient\Tests\Authentication\Authenticator;

use Keboola\AzureApiClient\Authentication\Authenticator\StaticBearerTokenAuth;
use PHPUnit\Framework\TestCase;

class StaticBearerTokenAuthTest extends TestCase
{
    public function testGetAuthenticationToken(): void
    {
        $authenticator = new StaticBearerTokenAuth('my-token');
        $token = $authenticator->getAuthenticationToken('foo-resource');

        self::assertSame('my-token', $token->value);
        self::assertNull($token->expiresAt);
    }
}
