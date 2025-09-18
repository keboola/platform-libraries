<?php

declare(strict_types=1);

namespace Keboola\SyncActionsClient\Tests;

use GuzzleHttp\Psr7\Request;
use Keboola\SyncActionsClient\StorageApiTokenAuthenticator;
use PHPUnit\Framework\TestCase;

class StorageApiTokenAuthenticatorTest extends TestCase
{
    public function testInvoke(): void
    {
        $token = 'test-token';
        $authenticator = new StorageApiTokenAuthenticator($token);

        $request = new Request('GET', 'https://example.com');
        $authenticatedRequest = $authenticator($request);

        self::assertTrue($authenticatedRequest->hasHeader('X-StorageApi-Token'));
        self::assertSame([$token], $authenticatedRequest->getHeader('X-StorageApi-Token'));
        self::assertNotSame($request, $authenticatedRequest);
    }
}
