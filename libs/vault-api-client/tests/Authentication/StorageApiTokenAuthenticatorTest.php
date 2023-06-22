<?php

declare(strict_types=1);

namespace Keboola\VaultApiClient\Tests\Authentication;

use GuzzleHttp\Psr7\Request;
use Keboola\VaultApiClient\Authentication\StorageApiTokenAuthenticator;
use PHPUnit\Framework\TestCase;

class StorageApiTokenAuthenticatorTest extends TestCase
{
    public function testInvocation(): void
    {
        $authenticator = new StorageApiTokenAuthenticator('my-token');

        $request = new Request(
            'GET',
            'https://example.com',
            ['Content-Type' => 'application/json'],
        );
        $modifiedRequest = $authenticator->__invoke($request);

        self::assertSame(
            [
                'Host' => ['example.com'],
                'Content-Type' => ['application/json'],
                'X-StorageApi-Token' => ['my-token'],
            ],
            $modifiedRequest->getHeaders(),
        );
    }
}
