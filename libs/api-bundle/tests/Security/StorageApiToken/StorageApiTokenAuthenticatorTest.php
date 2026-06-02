<?php

declare(strict_types=1);

namespace Keboola\ApiBundle\Tests\Security\StorageApiToken;

use Keboola\ApiBundle\Attribute\StorageApiTokenAuth;
use Keboola\ApiBundle\Security\StorageApiToken\StorageApiToken;
use Keboola\ApiBundle\Security\StorageApiToken\StorageApiTokenAuthenticator;
use Keboola\ApiBundle\Security\StorageApiToken\StorageApiTokenExchange;
use Keboola\ApiBundle\Security\StorageApiToken\StorageApiTokenFactory;
use PHPUnit\Framework\TestCase;
use Symfony\Component\HttpFoundation\Request;
use Symfony\Component\Security\Core\Exception\AccessDeniedException;

class StorageApiTokenAuthenticatorTest extends TestCase
{
    // ---------------------------------------------------------------------------
    // extractToken
    // ---------------------------------------------------------------------------

    public function testExtractTokenFromPrimaryHeader(): void
    {
        $authenticator = new StorageApiTokenAuthenticator(
            $this->createMock(StorageApiTokenFactory::class),
        );

        $request = Request::create('https://keboola.com');
        $request->headers->set('X-StorageApi-Token', 'my-token');

        self::assertSame('my-token', $authenticator->extractToken($request));
    }

    public function testExtractTokenFromBearerHeader(): void
    {
        $authenticator = new StorageApiTokenAuthenticator(
            $this->createMock(StorageApiTokenFactory::class),
        );

        $request = Request::create('https://keboola.com');
        $request->headers->set('Authorization', 'Bearer my-bearer-token');

        self::assertSame('my-bearer-token', $authenticator->extractToken($request));
    }

    public function testExtractTokenFromAuthorizationHeaderWithoutBearer(): void
    {
        $authenticator = new StorageApiTokenAuthenticator(
            $this->createMock(StorageApiTokenFactory::class),
        );

        $request = Request::create('https://keboola.com');
        $request->headers->set('Authorization', 'some-token-without-bearer');

        self::assertSame('some-token-without-bearer', $authenticator->extractToken($request));
    }

    public function testExtractTokenPrefersAuthorizationHeader(): void
    {
        $authenticator = new StorageApiTokenAuthenticator(
            $this->createMock(StorageApiTokenFactory::class),
        );

        $request = Request::create('https://keboola.com');
        $request->headers->set('X-StorageApi-Token', 'storage-token');
        $request->headers->set('Authorization', 'Bearer bearer-token');

        self::assertSame('bearer-token', $authenticator->extractToken($request));
    }

    public function testExtractTokenReturnsNullWhenNoHeader(): void
    {
        $authenticator = new StorageApiTokenAuthenticator(
            $this->createMock(StorageApiTokenFactory::class),
        );

        $request = Request::create('https://keboola.com');

        self::assertNull($authenticator->extractToken($request));
    }

    // ---------------------------------------------------------------------------
    // authenticateToken – routing logic
    // ---------------------------------------------------------------------------

    public function testAuthenticateTokenRoutesLegacyTokenToFactory(): void
    {
        $expectedToken = $this->createMock(StorageApiToken::class);

        $tokenFactory = $this->createMock(StorageApiTokenFactory::class);
        $tokenFactory
            ->expects(self::once())
            ->method('createFromRequest')
            ->willReturn($expectedToken);

        $tokenExchange = $this->createMock(StorageApiTokenExchange::class);
        $tokenExchange
            ->expects(self::never())
            ->method('exchange');

        $request = Request::create('https://keboola.com');

        $authenticator = new StorageApiTokenAuthenticator(
            tokenFactory: $tokenFactory,
            tokenExchange: $tokenExchange,
            exchangeEnabled: false,
        );

        $result = $authenticator->authenticateToken(new StorageApiTokenAuth(), 'legacy-token', $request);

        self::assertSame($expectedToken, $result);
    }

    public function testAuthenticateTokenRoutesProgrammaticTokenToExchangeWhenEnabled(): void
    {
        $expectedToken = $this->createMock(StorageApiToken::class);

        $request = Request::create('https://keboola.com');

        $tokenExchange = $this->createMock(StorageApiTokenExchange::class);
        $tokenExchange
            ->expects(self::once())
            ->method('exchange')
            ->with($request, 'kbc_at_x', 'X-KBC-ProjectId')
            ->willReturn($expectedToken);

        $tokenFactory = $this->createMock(StorageApiTokenFactory::class);
        $tokenFactory
            ->expects(self::never())
            ->method('createFromRequest');

        $authenticator = new StorageApiTokenAuthenticator(
            tokenFactory: $tokenFactory,
            tokenExchange: $tokenExchange,
            exchangeEnabled: true,
        );

        $result = $authenticator->authenticateToken(new StorageApiTokenAuth(), 'kbc_at_x', $request);

        self::assertSame($expectedToken, $result);
    }

    public function testAuthenticateTokenIgnoresExchangeForProgrammaticTokenWhenDisabled(): void
    {
        $expectedToken = $this->createMock(StorageApiToken::class);

        $tokenFactory = $this->createMock(StorageApiTokenFactory::class);
        $tokenFactory
            ->expects(self::once())
            ->method('createFromRequest')
            ->willReturn($expectedToken);

        $tokenExchange = $this->createMock(StorageApiTokenExchange::class);
        $tokenExchange
            ->expects(self::never())
            ->method('exchange');

        $request = Request::create('https://keboola.com');

        $authenticator = new StorageApiTokenAuthenticator(
            tokenFactory: $tokenFactory,
            tokenExchange: $tokenExchange,
            exchangeEnabled: false,
        );

        $result = $authenticator->authenticateToken(new StorageApiTokenAuth(), 'kbc_at_secret', $request);

        self::assertSame($expectedToken, $result);
    }

    public function testAuthenticateTokenFallsBackToFactoryWhenExchangeEnabledButNull(): void
    {
        $expectedToken = $this->createMock(StorageApiToken::class);

        $tokenFactory = $this->createMock(StorageApiTokenFactory::class);
        $tokenFactory
            ->expects(self::once())
            ->method('createFromRequest')
            ->willReturn($expectedToken);

        $request = Request::create('https://keboola.com');

        $authenticator = new StorageApiTokenAuthenticator(
            tokenFactory: $tokenFactory,
            tokenExchange: null,
            exchangeEnabled: true,
        );

        $result = $authenticator->authenticateToken(new StorageApiTokenAuth(), 'kbc_at_secret', $request);

        self::assertSame($expectedToken, $result);
    }

    // ---------------------------------------------------------------------------
    // authorizeToken
    // ---------------------------------------------------------------------------

    public function testAuthorizeTokenPassesWhenRequiredFeaturesPresent(): void
    {
        $tokenData = [
            'id' => '1',
            'description' => 'test',
            'owner' => ['features' => ['feature-a']],
        ];
        $storageApiToken = new StorageApiToken($tokenData, 'tok');

        $authenticator = new StorageApiTokenAuthenticator(
            $this->createMock(StorageApiTokenFactory::class),
        );

        $authenticator->authorizeToken(new StorageApiTokenAuth(features: ['feature-a']), $storageApiToken);

        $this->expectNotToPerformAssertions();
    }

    public function testAuthorizeTokenThrowsAccessDeniedWhenFeatureMissing(): void
    {
        $tokenData = [
            'id' => '1',
            'description' => 'test',
            'owner' => ['features' => ['feature-a']],
        ];
        $storageApiToken = new StorageApiToken($tokenData, 'tok');

        $authenticator = new StorageApiTokenAuthenticator(
            $this->createMock(StorageApiTokenFactory::class),
        );

        self::expectException(AccessDeniedException::class);
        self::expectExceptionMessage('missing following features: feature-b');

        $authenticator->authorizeToken(
            new StorageApiTokenAuth(features: ['feature-b']),
            $storageApiToken,
        );
    }
}
