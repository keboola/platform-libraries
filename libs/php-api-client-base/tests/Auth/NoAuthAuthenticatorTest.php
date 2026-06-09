<?php

declare(strict_types=1);

namespace Keboola\ApiClientBase\Tests\Auth;

use GuzzleHttp\Psr7\Request;
use Keboola\ApiClientBase\Auth\NoAuthAuthenticator;
use PHPUnit\Framework\TestCase;

class NoAuthAuthenticatorTest extends TestCase
{
    public function testReturnsRequestUnchanged(): void
    {
        $authenticator = new NoAuthAuthenticator();
        $request = new Request('GET', 'https://example.test', ['X-Existing' => 'keep']);
        $result = $authenticator($request);

        self::assertSame('keep', $result->getHeaderLine('X-Existing'));
        self::assertSame($request->getHeaders(), $result->getHeaders());
    }
}
