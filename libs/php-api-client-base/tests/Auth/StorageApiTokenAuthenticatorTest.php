<?php

declare(strict_types=1);

namespace Keboola\ApiClientBase\Tests\Auth;

use InvalidArgumentException;
use Keboola\ApiClientBase\Auth\StorageApiTokenAuthenticator;
use PHPUnit\Framework\TestCase;

class StorageApiTokenAuthenticatorTest extends TestCase
{
    public function testReturnsStorageApiTokenHeader(): void
    {
        $authenticator = new StorageApiTokenAuthenticator('secret-token');

        self::assertSame(
            ['X-StorageApi-Token' => 'secret-token'],
            $authenticator->getAuthenticationHeaders(),
        );
    }

    public function testRejectsEmptyToken(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Storage API token must not be empty');
        /** @phpstan-ignore-next-line argument.type — exercising the runtime guard */
        new StorageApiTokenAuthenticator('');
    }
}
