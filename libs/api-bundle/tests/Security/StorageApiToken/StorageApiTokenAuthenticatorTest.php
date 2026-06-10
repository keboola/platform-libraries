<?php

declare(strict_types=1);

namespace Keboola\ApiBundle\Tests\Security\StorageApiToken;

use Generator;
use Keboola\ApiBundle\Attribute\StorageApiTokenAuth;
use Keboola\ApiBundle\Security\StorageApiToken\StorageApiToken;
use Keboola\ApiBundle\Security\StorageApiToken\StorageApiTokenAuthenticator;
use Keboola\ApiBundle\Security\StorageApiToken\StorageApiTokenFactory;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;
use Symfony\Component\HttpFoundation\Request;
use Symfony\Component\Security\Core\Exception\AccessDeniedException;

class StorageApiTokenAuthenticatorTest extends TestCase
{
    private const SUBJECT_TOKEN = 'kbc_at_secret';
    private const PROJECT_ID_HEADER = 'X-KBC-ProjectId';

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

    public function testAuthenticateTokenRoutesLegacyTokenToLegacyVerification(): void
    {
        $expectedToken = $this->createMock(StorageApiToken::class);

        $request = Request::create('https://keboola.com');
        $request->headers->set('X-StorageApi-Token', 'legacy-token');

        $tokenFactory = $this->createMock(StorageApiTokenFactory::class);
        $tokenFactory
            ->expects(self::once())
            ->method('createFromRequest')
            ->with($request)
            ->willReturn($expectedToken);
        $tokenFactory
            ->expects(self::never())
            ->method('createFromProgrammaticToken');

        $authenticator = new StorageApiTokenAuthenticator($tokenFactory);

        $result = $authenticator->authenticateToken(new StorageApiTokenAuth(), 'legacy-token', $request);

        self::assertSame($expectedToken, $result);
    }

    public function testAuthenticateTokenRoutesProgrammaticBearerTokenToExchange(): void
    {
        $expectedToken = $this->createMock(StorageApiToken::class);

        $request = Request::create('https://keboola.com');
        $request->headers->set('Authorization', 'Bearer ' . self::SUBJECT_TOKEN);
        $request->headers->set(self::PROJECT_ID_HEADER, '123');

        $tokenFactory = $this->createMock(StorageApiTokenFactory::class);
        $tokenFactory
            ->expects(self::never())
            ->method('createFromRequest');
        $tokenFactory
            ->expects(self::once())
            ->method('createFromProgrammaticToken')
            ->with($request, self::SUBJECT_TOKEN)
            ->willReturn($expectedToken);

        $authenticator = new StorageApiTokenAuthenticator($tokenFactory);

        $result = $authenticator->authenticateToken(new StorageApiTokenAuth(), self::SUBJECT_TOKEN, $request);

        self::assertSame($expectedToken, $result);
    }

    #[DataProvider('provideNonBearerProgrammaticTokenCarriers')]
    public function testAuthenticateTokenDoesNotExchangeProgrammaticTokenFromNonBearerCarrier(
        string $headerName,
        string $headerValue,
    ): void {
        $expectedToken = $this->createMock(StorageApiToken::class);

        // The programmatic token does not arrive as `Authorization: Bearer`, so the legacy
        // verification path must be used and exchange must not be attempted.
        $tokenFactory = $this->createMock(StorageApiTokenFactory::class);
        $tokenFactory
            ->expects(self::once())
            ->method('createFromRequest')
            ->willReturn($expectedToken);
        $tokenFactory
            ->expects(self::never())
            ->method('createFromProgrammaticToken');

        $request = Request::create('https://keboola.com');
        $request->headers->set($headerName, $headerValue);
        $request->headers->set(self::PROJECT_ID_HEADER, '123');

        $authenticator = new StorageApiTokenAuthenticator($tokenFactory);

        $result = $authenticator->authenticateToken(new StorageApiTokenAuth(), self::SUBJECT_TOKEN, $request);

        self::assertSame($expectedToken, $result);
    }

    public function testAuthenticateTokenDoesNotExchangeWhenBearerHeaderDiffersFromExtractedToken(): void
    {
        $expectedToken = $this->createMock(StorageApiToken::class);

        // Defensive consistency check: exchange may only run with the token extractToken()
        // returned. If the Authorization header somehow carries a different programmatic token
        // than $token, the request falls back to legacy verification.
        $tokenFactory = $this->createMock(StorageApiTokenFactory::class);
        $tokenFactory
            ->expects(self::once())
            ->method('createFromRequest')
            ->willReturn($expectedToken);
        $tokenFactory
            ->expects(self::never())
            ->method('createFromProgrammaticToken');

        $request = Request::create('https://keboola.com');
        $request->headers->set('Authorization', 'Bearer kbc_at_different');
        $request->headers->set(self::PROJECT_ID_HEADER, '123');

        $authenticator = new StorageApiTokenAuthenticator($tokenFactory);

        $result = $authenticator->authenticateToken(new StorageApiTokenAuth(), self::SUBJECT_TOKEN, $request);

        self::assertSame($expectedToken, $result);
    }

    public static function provideNonBearerProgrammaticTokenCarriers(): Generator
    {
        yield 'bare Authorization header' => [
            'headerName' => 'Authorization',
            'headerValue' => self::SUBJECT_TOKEN,
        ];
        yield 'X-StorageApi-Token header' => [
            'headerName' => 'X-StorageApi-Token',
            'headerValue' => self::SUBJECT_TOKEN,
        ];
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
