<?php

declare(strict_types=1);

namespace Keboola\AzureApiClient\Tests\ApiClientFactory;

use DateTimeImmutable;
use GuzzleHttp\Psr7\Request;
use Keboola\AzureApiClient\ApiClientFactory\BearerAuthorizationHeaderResolver;
use Keboola\AzureApiClient\Authentication\AuthenticatorFactory;
use Keboola\AzureApiClient\Authentication\AuthenticatorInterface;
use Keboola\AzureApiClient\Authentication\TokenWithExpiration;
use PHPUnit\Framework\TestCase;

class AuthorizationHeaderResolverTest extends TestCase
{
    public function testTokenResolve(): void
    {
        $request = new Request('GET', '/foo');

        $authenticator = $this->createMock(AuthenticatorInterface::class);
        $authenticator
            ->method('getAuthenticationToken')
            ->with('foo-resource')
            ->willReturn(new TokenWithExpiration(
                'auth-token',
                new DateTimeImmutable('+1 hour'),
            ))
        ;

        $authenticatorFactory = $this->createMock(AuthenticatorFactory::class);
        $authenticatorFactory->expects(self::once())
            ->method('createAuthenticator')
            ->willReturn($authenticator)
        ;

        $resolver = new BearerAuthorizationHeaderResolver(
            $authenticatorFactory,
            'foo-resource',
        );

        $modifiedRequest = $resolver->__invoke($request);
        self::assertSame('Bearer auth-token', $modifiedRequest->getHeaderLine('Authorization'));
    }

    public function testTokenIsRefreshesBeforeExpiration(): void
    {
        $request = new Request('GET', '/foo');

        $authenticator = $this->createMock(AuthenticatorInterface::class);
        $authenticator->expects(self::exactly(2))
            ->method('getAuthenticationToken')
            ->with('foo-resource')
            ->willReturnOnConsecutiveCalls(
                new TokenWithExpiration(
                    'auth-token1',
                    new DateTimeImmutable('+63 seconds'), // 60 seconds is EXPIRATION_MARGIN
                ),
                new TokenWithExpiration(
                    'auth-token2',
                    new DateTimeImmutable('+1 hour'),
                ),
            )
        ;

        $authenticatorFactory = $this->createMock(AuthenticatorFactory::class);
        $authenticatorFactory->expects(self::once())
            ->method('createAuthenticator')
            ->willReturn($authenticator)
        ;

        $resolver = new BearerAuthorizationHeaderResolver(
            $authenticatorFactory,
            'foo-resource',
        );

        // normally resolved token
        // this is first invocation that fetches the token
        // the token has 63 seconds expiration, minus 60 seconds margin means it is usable for 3 seconds
        $modifiedRequest = $resolver->__invoke($request);
        self::assertSame('Bearer auth-token1', $modifiedRequest->getHeaderLine('Authorization'));

        // after 1 second, we should be still within the token lifetime, the token is re-used (no refresh)
        sleep(1);
        $modifiedRequest = $resolver->__invoke($request);
        self::assertSame('Bearer auth-token1', $modifiedRequest->getHeaderLine('Authorization'));

        // after another 3 seconds we are past the token lifetime (minus the 60 seconds margin), a new token is issued
        sleep(3);
        $modifiedRequest = $resolver->__invoke($request);
        self::assertSame('Bearer auth-token2', $modifiedRequest->getHeaderLine('Authorization'));
    }
}
