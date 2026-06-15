<?php

declare(strict_types=1);

namespace Keboola\ApiClientBase\Tests\Auth;

use GuzzleHttp\Psr7\Request;
use InvalidArgumentException;
use Keboola\ApiClientBase\Auth\StorageApiTokenAuthenticator;
use PHPUnit\Framework\TestCase;

class StorageApiTokenAuthenticatorTest extends TestCase
{
    public function testAddsStorageApiTokenHeader(): void
    {
        $authenticator = new StorageApiTokenAuthenticator('secret-token');
        $request = $authenticator(new Request('GET', 'https://example.test'));

        self::assertSame('secret-token', $request->getHeaderLine('X-StorageApi-Token'));
    }

    public function testRejectsEmptyToken(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Storage API token must not be empty');
        /** @phpstan-ignore-next-line argument.type — exercising the runtime guard */
        new StorageApiTokenAuthenticator('');
    }
}
