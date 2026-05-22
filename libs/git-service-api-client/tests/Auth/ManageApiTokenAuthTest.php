<?php

declare(strict_types=1);

namespace Keboola\GitServiceApiClient\Tests\Auth;

use InvalidArgumentException;
use Keboola\GitServiceApiClient\Auth\ManageApiTokenAuth;
use PHPUnit\Framework\TestCase;

class ManageApiTokenAuthTest extends TestCase
{
    public function testReturnsManageApiTokenHeader(): void
    {
        $auth = new ManageApiTokenAuth('secret-token');

        self::assertSame(
            ['X-KBC-ManageApiToken' => 'secret-token'],
            $auth->getAuthenticationHeaders(),
        );
    }

    public function testRejectsEmptyToken(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Manage API token must not be empty');
        /** @phpstan-ignore-next-line argument.type — exercising the runtime guard */
        new ManageApiTokenAuth('');
    }
}
