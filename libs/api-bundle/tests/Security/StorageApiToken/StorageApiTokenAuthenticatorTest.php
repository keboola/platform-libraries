<?php

declare(strict_types=1);

namespace Keboola\ApiBundle\Tests\Security\StorageApiToken;

use Generator;
use Keboola\ApiBundle\Attribute\StorageApiTokenAuth;
use Keboola\ApiBundle\Security\StorageApiToken\RequestToken;
use Keboola\ApiBundle\Security\StorageApiToken\RequestTokenType;
use Keboola\ApiBundle\Security\StorageApiToken\StorageApiToken;
use Keboola\ApiBundle\Security\StorageApiToken\StorageApiTokenAuthenticator;
use Keboola\ApiBundle\Security\StorageApiToken\StorageApiTokenFactory;
use Keboola\StorageApiBranch\Factory\AuthType;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;
use Symfony\Component\HttpFoundation\Request;
use Symfony\Component\Security\Core\Exception\AccessDeniedException;

class StorageApiTokenAuthenticatorTest extends TestCase
{
    private const SUBJECT_TOKEN = 'kbc_at_secret';

    // ---------------------------------------------------------------------------
    // extractCredential – classification
    // ---------------------------------------------------------------------------

    /**
     * @param array<string, string> $headers
     */
    #[DataProvider('provideCredentialClassifications')]
    public function testExtractCredentialClassifiesRequest(
        array $headers,
        string $expectedToken,
        RequestTokenType $expectedType,
    ): void {
        $authenticator = new StorageApiTokenAuthenticator(
            $this->createMock(StorageApiTokenFactory::class),
        );

        $request = Request::create('https://keboola.com');
        foreach ($headers as $name => $value) {
            $request->headers->set($name, $value);
        }

        $credential = $authenticator->extractCredential($request);

        self::assertNotNull($credential);
        self::assertSame($expectedToken, $credential->token);
        self::assertSame($expectedType, $credential->type);
    }

    public static function provideCredentialClassifications(): Generator
    {
        yield 'legacy X-StorageApi-Token' => [
            'headers' => ['X-StorageApi-Token' => 'my-token'],
            'expectedToken' => 'my-token',
            'expectedType' => RequestTokenType::StorageToken,
        ];
        yield 'OAuth Authorization: Bearer' => [
            'headers' => ['Authorization' => 'Bearer my-bearer-token'],
            'expectedToken' => 'my-bearer-token',
            'expectedType' => RequestTokenType::OAuthToken,
        ];
        yield 'non-Bearer Authorization taken verbatim as legacy token' => [
            'headers' => ['Authorization' => 'some-token-without-bearer'],
            'expectedToken' => 'some-token-without-bearer',
            'expectedType' => RequestTokenType::StorageToken,
        ];
        yield 'Authorization: Bearer wins over X-StorageApi-Token' => [
            'headers' => ['X-StorageApi-Token' => 'storage-token', 'Authorization' => 'Bearer bearer-token'],
            'expectedToken' => 'bearer-token',
            'expectedType' => RequestTokenType::OAuthToken,
        ];
        yield 'programmatic access token via Bearer' => [
            'headers' => ['Authorization' => 'Bearer ' . self::SUBJECT_TOKEN],
            'expectedToken' => self::SUBJECT_TOKEN,
            'expectedType' => RequestTokenType::Programmatic,
        ];
        yield 'programmatic personal access token via Bearer' => [
            'headers' => ['Authorization' => 'Bearer kbc_pat_secret'],
            'expectedToken' => 'kbc_pat_secret',
            'expectedType' => RequestTokenType::Programmatic,
        ];
        // A programmatic-looking token that does not arrive as `Authorization: Bearer` is an
        // undocumented shape and stays a legacy Storage token (never exchanged).
        yield 'programmatic token via bare Authorization is legacy' => [
            'headers' => ['Authorization' => self::SUBJECT_TOKEN],
            'expectedToken' => self::SUBJECT_TOKEN,
            'expectedType' => RequestTokenType::StorageToken,
        ];
        yield 'programmatic token via X-StorageApi-Token is legacy' => [
            'headers' => ['X-StorageApi-Token' => self::SUBJECT_TOKEN],
            'expectedToken' => self::SUBJECT_TOKEN,
            'expectedType' => RequestTokenType::StorageToken,
        ];
    }

    public function testExtractCredentialReturnsNullWhenNoHeader(): void
    {
        $authenticator = new StorageApiTokenAuthenticator(
            $this->createMock(StorageApiTokenFactory::class),
        );

        self::assertNull($authenticator->extractCredential(Request::create('https://keboola.com')));
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
