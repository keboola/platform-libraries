<?php

declare(strict_types=1);

namespace Keboola\ApiBundle\Tests\Security\StorageApiToken;

use Keboola\ApiBundle\Attribute\StorageApiTokenAuth;
use Keboola\ApiBundle\Security\StorageApiToken\RequestToken;
use Keboola\ApiBundle\Security\StorageApiToken\RequestTokenType;
use Keboola\ApiBundle\Security\StorageApiToken\StorageApiToken;
use Keboola\ApiBundle\Security\StorageApiToken\StorageApiTokenAuthenticator;
use Keboola\ApiBundle\Security\StorageApiToken\StorageApiTokenFactory;
use Keboola\StorageApiBranch\Factory\AuthType;
use PHPUnit\Framework\TestCase;
use Symfony\Component\HttpFoundation\Request;
use Symfony\Component\Security\Core\Exception\AccessDeniedException;

class StorageApiTokenAuthenticatorTest extends TestCase
{
    private const SUBJECT_TOKEN = 'kbc_at_secret';

    // ---------------------------------------------------------------------------
    // extractCredential
    // ---------------------------------------------------------------------------

    /**
     * Classification itself is covered by {@see RequestTokenTest}; here we only verify the
     * authenticator exposes it through the interface method.
     */
    public function testExtractCredentialClassifiesViaRequestToken(): void
    {
        $authenticator = new StorageApiTokenAuthenticator(
            $this->createMock(StorageApiTokenFactory::class),
        );

        $request = Request::create('https://keboola.com');
        $request->headers->set('Authorization', 'Bearer ' . self::SUBJECT_TOKEN);

        $credential = $authenticator->extractCredential($request);

        self::assertNotNull($credential);
        self::assertSame(self::SUBJECT_TOKEN, $credential->token);
        self::assertSame(RequestTokenType::Programmatic, $credential->type);
    }

    // ---------------------------------------------------------------------------
    // authenticateToken – dispatch by credential type
    // ---------------------------------------------------------------------------

    public function testAuthenticateTokenVerifiesStorageTokenCredential(): void
    {
        $expectedToken = $this->createMock(StorageApiToken::class);
        $request = Request::create('https://keboola.com');

        $tokenFactory = $this->createMock(StorageApiTokenFactory::class);
        $tokenFactory
            ->expects(self::once())
            ->method('createFromStorageToken')
            ->with($request, 'legacy-token')
            ->willReturn($expectedToken);
        $tokenFactory->expects(self::never())->method('createFromOAuthToken');
        $tokenFactory->expects(self::never())->method('createFromProgrammaticToken');

        $authenticator = new StorageApiTokenAuthenticator($tokenFactory);

        $result = $authenticator->authenticateToken(
            new StorageApiTokenAuth(),
            new RequestToken('legacy-token', RequestTokenType::StorageToken),
            $request,
        );

        self::assertSame($expectedToken, $result);
    }

    public function testAuthenticateTokenVerifiesOAuthTokenCredential(): void
    {
        $expectedToken = $this->createMock(StorageApiToken::class);
        $request = Request::create('https://keboola.com');

        $tokenFactory = $this->createMock(StorageApiTokenFactory::class);
        $tokenFactory
            ->expects(self::once())
            ->method('createFromOAuthToken')
            ->with($request, 'oauth-access-token')
            ->willReturn($expectedToken);
        $tokenFactory->expects(self::never())->method('createFromStorageToken');
        $tokenFactory->expects(self::never())->method('createFromProgrammaticToken');

        $authenticator = new StorageApiTokenAuthenticator($tokenFactory);

        $result = $authenticator->authenticateToken(
            new StorageApiTokenAuth(),
            new RequestToken('oauth-access-token', RequestTokenType::OAuthToken),
            $request,
        );

        self::assertSame($expectedToken, $result);
    }

    public function testAuthenticateTokenExchangesProgrammaticCredential(): void
    {
        $expectedToken = $this->createMock(StorageApiToken::class);
        $request = Request::create('https://keboola.com');

        $tokenFactory = $this->createMock(StorageApiTokenFactory::class);
        $tokenFactory
            ->expects(self::once())
            ->method('createFromProgrammaticToken')
            ->with($request, self::SUBJECT_TOKEN)
            ->willReturn($expectedToken);
        $tokenFactory->expects(self::never())->method('createFromStorageToken');
        $tokenFactory->expects(self::never())->method('createFromOAuthToken');

        $authenticator = new StorageApiTokenAuthenticator($tokenFactory);

        $result = $authenticator->authenticateToken(
            new StorageApiTokenAuth(),
            new RequestToken(self::SUBJECT_TOKEN, RequestTokenType::Programmatic),
            $request,
        );

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
        $storageApiToken = new StorageApiToken($tokenData, 'tok', AuthType::STORAGE_TOKEN);

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
        $storageApiToken = new StorageApiToken($tokenData, 'tok', AuthType::STORAGE_TOKEN);

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
