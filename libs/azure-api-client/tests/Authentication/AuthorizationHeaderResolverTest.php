<?php

declare(strict_types=1);

namespace Keboola\AzureApiClient\Tests\Authentication;

use DateTimeImmutable;
use GuzzleHttp\Psr7\Request;
use Keboola\AzureApiClient\Authentication\AuthenticationToken;
use Keboola\AzureApiClient\Authentication\Authenticator\AuthenticatorInterface;
use Keboola\AzureApiClient\Authentication\AuthorizationHeaderResolver;
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
            ->willReturn(new AuthenticationToken(
                'auth-token',
                new DateTimeImmutable('+1 hour'),
            ))
        ;

        $resolver = new AuthorizationHeaderResolver(
            $authenticator,
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
                new AuthenticationToken(
                    'auth-token1',
                    new DateTimeImmutable('+63 seconds'), // 60 seconds is EXPIRATION_MARGIN
                ),
                new AuthenticationToken(
                    'auth-token2',
                    new DateTimeImmutable('+1 hour'),
                ),
            )
        ;

        $resolver = new AuthorizationHeaderResolver(
            $authenticator,
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
