<?php

declare(strict_types=1);

namespace Keboola\ApiClientBase\Tests\Auth;

use InvalidArgumentException;
use Keboola\ApiClientBase\Auth\ManageApiTokenAuthenticator;
use PHPUnit\Framework\TestCase;

class ManageApiTokenAuthenticatorTest extends TestCase
{
    public function testReturnsManageApiTokenHeader(): void
    {
        $authenticator = new ManageApiTokenAuthenticator('secret-token');

        self::assertSame(
            ['X-KBC-ManageApiToken' => 'secret-token'],
            $authenticator->getAuthenticationHeaders(),
        );
    }

    public function testRejectsEmptyToken(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Manage API token must not be empty');
        /** @phpstan-ignore-next-line argument.type — exercising the runtime guard */
        new ManageApiTokenAuthenticator('');
    }
}
