<?php

declare(strict_types=1);

namespace Keboola\SandboxesServiceApiClient\Tests\Authentication;

use Keboola\SandboxesServiceApiClient\Authentication\StorageTokenAuthenticator;
use PHPUnit\Framework\TestCase;

class StorageTokenAuthenticatorTest extends TestCase
{
    public function testInvoke(): void
    {
        $request = new Request('GET', '/foo');

        $authenticator = new StorageTokenAuthenticator('token-string-value');

        $modifiedRequest = $authenticator->__invoke($request);
        self::assertSame(
            'token-string-value',
            $modifiedRequest->getHeaderLine(StorageTokenAuthenticator::STORAGE_TOKEN_HEADER)
        );
    }

}
