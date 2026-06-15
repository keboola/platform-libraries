<?php

declare(strict_types=1);

namespace Keboola\ApiClientBase\Tests\Auth;

use GuzzleHttp\Psr7\Request;
use InvalidArgumentException;
use Keboola\ApiClientBase\Auth\ManageApiTokenAuthenticator;
use PHPUnit\Framework\TestCase;

class ManageApiTokenAuthenticatorTest extends TestCase
{
    public function testAddsManageApiTokenHeader(): void
    {
        $authenticator = new ManageApiTokenAuthenticator('secret-token');
        $request = $authenticator(new Request('GET', 'https://example.test'));

        self::assertSame('secret-token', $request->getHeaderLine('X-KBC-ManageApiToken'));
    }

    public function testRejectsEmptyToken(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Manage API token must not be empty');
        /** @phpstan-ignore-next-line argument.type — exercising the runtime guard */
        new ManageApiTokenAuthenticator('');
    }
}
