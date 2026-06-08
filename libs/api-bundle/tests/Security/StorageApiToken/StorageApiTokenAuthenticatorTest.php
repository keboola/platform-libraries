<?php

declare(strict_types=1);

namespace Keboola\ApiBundle\Tests\Security\StorageApiToken;

use Generator;
use GuzzleHttp\Exception\ConnectException;
use GuzzleHttp\Psr7\Request as GuzzleRequest;
use Keboola\ApiBundle\Attribute\StorageApiTokenAuth;
use Keboola\ApiBundle\Security\StorageApiToken\StorageApiToken;
use Keboola\ApiBundle\Security\StorageApiToken\StorageApiTokenAuthenticator;
use Keboola\ApiBundle\Security\StorageApiToken\StorageApiTokenFactory;
use Keboola\ManageApi\Client as ManageApiClient;
use Keboola\ManageApi\ClientException as ManageApiClientException;
use Keboola\ManageApi\MaintenanceException;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;
use Psr\Log\LoggerInterface;
use RuntimeException;
use Symfony\Component\HttpFoundation\Request;
use Symfony\Component\Security\Core\Exception\AccessDeniedException;
use Symfony\Component\Security\Core\Exception\CustomUserMessageAuthenticationException;

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

    public function testAuthenticateTokenRoutesLegacyTokenToFactory(): void
    {
        $expectedToken = $this->createMock(StorageApiToken::class);

        $tokenFactory = $this->createMock(StorageApiTokenFactory::class);
        $tokenFactory
            ->expects(self::once())
            ->method('createFromRequest')
            ->willReturn($expectedToken);
        $tokenFactory
            ->expects(self::never())
            ->method('createFromResolvedToken');

        $resolverClient = $this->createMock(ManageApiClient::class);
        $resolverClient
            ->expects(self::never())
            ->method('resolveStorageToken');

        $authenticator = new StorageApiTokenAuthenticator($tokenFactory, $resolverClient);

        $result = $authenticator->authenticateToken(
            new StorageApiTokenAuth(),
            'legacy-token',
            Request::create('https://keboola.com'),
        );

        self::assertSame($expectedToken, $result);
    }

    public function testAuthenticateTokenFallsBackToFactoryForProgrammaticTokenWhenNoResolverClient(): void
    {
        $expectedToken = $this->createMock(StorageApiToken::class);

        $tokenFactory = $this->createMock(StorageApiTokenFactory::class);
        $tokenFactory
            ->expects(self::once())
            ->method('createFromRequest')
            ->willReturn($expectedToken);

        $request = Request::create('https://keboola.com');
        $request->headers->set('Authorization', 'Bearer ' . self::SUBJECT_TOKEN);

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

        $tokenFactory = $this->createMock(StorageApiTokenFactory::class);
        $tokenFactory
            ->expects(self::once())
            ->method('createFromRequest')
            ->willReturn($expectedToken);
        $tokenFactory
            ->expects(self::never())
            ->method('createFromResolvedToken');

        // Resolver is wired, yet the programmatic token does not arrive as `Authorization: Bearer`,
        // so the legacy verification path must be used and exchange must not be attempted.
        $resolverClient = $this->createMock(ManageApiClient::class);
        $resolverClient
            ->expects(self::never())
            ->method('resolveStorageToken');

        $request = Request::create('https://keboola.com');
        $request->headers->set($headerName, $headerValue);
        $request->headers->set(self::PROJECT_ID_HEADER, '123');

        $authenticator = new StorageApiTokenAuthenticator($tokenFactory, $resolverClient);

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
    // authenticateToken – programmatic token exchange success
    // ---------------------------------------------------------------------------

    public function testAuthenticateTokenExchangesProgrammaticToken(): void
    {
        $expectedToken = $this->createMock(StorageApiToken::class);

        $request = Request::create('https://keboola.com');
        $request->headers->set('Authorization', 'Bearer ' . self::SUBJECT_TOKEN);
        $request->headers->set(self::PROJECT_ID_HEADER, '123');

        $resolverClient = $this->createMock(ManageApiClient::class);
        $resolverClient
            ->expects(self::once())
            ->method('resolveStorageToken')
            ->with(123, self::SUBJECT_TOKEN)
            ->willReturn([
                'storageToken' => 'legacy-storage-token',
                'projectId' => 123,
                'tokenId' => '42',
                'userId' => '7',
                'expiresAt' => null,
            ]);

        $tokenFactory = $this->createMock(StorageApiTokenFactory::class);
        $tokenFactory
            ->expects(self::never())
            ->method('createFromRequest');
        $tokenFactory
            ->expects(self::once())
            ->method('createFromResolvedToken')
            ->with($request, 'legacy-storage-token')
            ->willReturn($expectedToken);

        $authenticator = new StorageApiTokenAuthenticator($tokenFactory, $resolverClient);

        $result = $authenticator->authenticateToken(new StorageApiTokenAuth(), self::SUBJECT_TOKEN, $request);

        self::assertSame($expectedToken, $result);
    }

    // ---------------------------------------------------------------------------
    // authenticateToken – project id header validation
    // ---------------------------------------------------------------------------

    #[DataProvider('provideInvalidProjectIdHeaders')]
    public function testExchangeThrowsWith400ForInvalidProjectIdHeader(?string $headerValue): void
    {
        $resolverClient = $this->createMock(ManageApiClient::class);
        $resolverClient
            ->expects(self::never())
            ->method('resolveStorageToken');

        $request = Request::create('https://keboola.com');
        $request->headers->set('Authorization', 'Bearer ' . self::SUBJECT_TOKEN);
        if ($headerValue !== null) {
            $request->headers->set(self::PROJECT_ID_HEADER, $headerValue);
        }

        $authenticator = new StorageApiTokenAuthenticator(
            $this->createMock(StorageApiTokenFactory::class),
            $resolverClient,
        );

        $exception = $this->captureAuthException(
            fn() => $authenticator->authenticateToken(new StorageApiTokenAuth(), self::SUBJECT_TOKEN, $request),
        );

        self::assertSame(400, $exception->getCode());
    }

    public static function provideInvalidProjectIdHeaders(): Generator
    {
        yield 'missing header' => ['headerValue' => null];
        yield 'empty string' => ['headerValue' => ''];
        yield 'non-numeric string' => ['headerValue' => 'abc'];
        yield 'zero' => ['headerValue' => '0'];
        yield 'negative number' => ['headerValue' => '-5'];
        // Past PHP_INT_MAX: ctype_digit would have accepted this and the cast would have silently
        // wrapped to a different project id; filter_var rejects it.
        yield 'overflows int range' => ['headerValue' => '99999999999999999999999999'];
        yield 'leading zeros' => ['headerValue' => '0123'];
    }

    // ---------------------------------------------------------------------------
    // authenticateToken – resolver error mapping
    // ---------------------------------------------------------------------------

    #[DataProvider('provideResolverClientErrorMapping')]
    public function testExchangeMapsResolverClientErrorToExpectedCode(int $statusCode, int $expectedCode): void
    {
        $resolverClient = $this->createMock(ManageApiClient::class);
        $resolverClient
            ->expects(self::once())
            ->method('resolveStorageToken')
            ->willThrowException(new ManageApiClientException(self::SUBJECT_TOKEN, $statusCode));

        $request = Request::create('https://keboola.com');
        $request->headers->set('Authorization', 'Bearer ' . self::SUBJECT_TOKEN);
        $request->headers->set(self::PROJECT_ID_HEADER, '123');

        $authenticator = new StorageApiTokenAuthenticator(
            $this->createMock(StorageApiTokenFactory::class),
            $resolverClient,
        );

        $exception = $this->captureAuthException(
            fn() => $authenticator->authenticateToken(new StorageApiTokenAuth(), self::SUBJECT_TOKEN, $request),
        );

        self::assertSame($expectedCode, $exception->getCode());
        // The Manage API client embeds the response body in the exception message; assert our
        // mapping never echoes it back so subject/storage token material cannot leak.
        self::assertStringNotContainsString(self::SUBJECT_TOKEN, $exception->getMessageKey());
    }

    public static function provideResolverClientErrorMapping(): Generator
    {
        yield 'bad request -> 400' => ['statusCode' => 400, 'expectedCode' => 400];
        yield 'unauthorized -> 401' => ['statusCode' => 401, 'expectedCode' => 401];
        yield 'forbidden -> 403' => ['statusCode' => 403, 'expectedCode' => 403];
        yield 'server error -> 502' => ['statusCode' => 500, 'expectedCode' => 502];
        yield 'connect error without response (code 0) -> 502' => ['statusCode' => 0, 'expectedCode' => 502];
        yield 'unexpected client error -> 502' => ['statusCode' => 418, 'expectedCode' => 502];
    }

    public function testExchangeMapsMaintenanceExceptionTo503(): void
    {
        $resolverClient = $this->createMock(ManageApiClient::class);
        $resolverClient
            ->expects(self::once())
            ->method('resolveStorageToken')
            ->willThrowException(new MaintenanceException('Maintenance', 30, []));

        $logger = $this->createMock(LoggerInterface::class);
        $logger
            ->expects(self::once())
            ->method('warning')
            ->with(self::stringContains('maintenance'), ['projectId' => 123, 'retryAfter' => 30]);

        $request = Request::create('https://keboola.com');
        $request->headers->set('Authorization', 'Bearer ' . self::SUBJECT_TOKEN);
        $request->headers->set(self::PROJECT_ID_HEADER, '123');

        $authenticator = new StorageApiTokenAuthenticator(
            $this->createMock(StorageApiTokenFactory::class),
            $resolverClient,
            $logger,
        );

        $exception = $this->captureAuthException(
            fn() => $authenticator->authenticateToken(new StorageApiTokenAuth(), self::SUBJECT_TOKEN, $request),
        );

        self::assertSame(503, $exception->getCode());
    }

    public function testExchangeMapsConnectExceptionTo502(): void
    {
        $resolverClient = $this->createMock(ManageApiClient::class);
        $resolverClient
            ->expects(self::once())
            ->method('resolveStorageToken')
            ->willThrowException(new ConnectException('Connection refused', new GuzzleRequest('POST', 'resolve')));

        $request = Request::create('https://keboola.com');
        $request->headers->set('Authorization', 'Bearer ' . self::SUBJECT_TOKEN);
        $request->headers->set(self::PROJECT_ID_HEADER, '123');

        $authenticator = new StorageApiTokenAuthenticator(
            $this->createMock(StorageApiTokenFactory::class),
            $resolverClient,
        );

        $exception = $this->captureAuthException(
            fn() => $authenticator->authenticateToken(new StorageApiTokenAuth(), self::SUBJECT_TOKEN, $request),
        );

        self::assertSame(502, $exception->getCode());
    }

    public function testExchangeMapsServiceAccountTokenFailureTo502(): void
    {
        // KubernetesServiceAccountTokenAuthenticationStrategy throws RuntimeException when the
        // projected SA token file is missing/unreadable/empty.
        $resolverClient = $this->createMock(ManageApiClient::class);
        $resolverClient
            ->expects(self::once())
            ->method('resolveStorageToken')
            ->willThrowException(new RuntimeException('token file is empty'));

        $request = Request::create('https://keboola.com');
        $request->headers->set('Authorization', 'Bearer ' . self::SUBJECT_TOKEN);
        $request->headers->set(self::PROJECT_ID_HEADER, '123');

        $authenticator = new StorageApiTokenAuthenticator(
            $this->createMock(StorageApiTokenFactory::class),
            $resolverClient,
        );

        $exception = $this->captureAuthException(
            fn() => $authenticator->authenticateToken(new StorageApiTokenAuth(), self::SUBJECT_TOKEN, $request),
        );

        self::assertSame(502, $exception->getCode());
    }

    #[DataProvider('provideInvalidResolverResponses')]
    public function testExchangeThrowsWith502WhenResolverResponseLacksStorageToken(mixed $response): void
    {
        $resolverClient = $this->createMock(ManageApiClient::class);
        $resolverClient
            ->expects(self::once())
            ->method('resolveStorageToken')
            ->willReturn($response);

        $tokenFactory = $this->createMock(StorageApiTokenFactory::class);
        $tokenFactory
            ->expects(self::never())
            ->method('createFromResolvedToken');

        $request = Request::create('https://keboola.com');
        $request->headers->set('Authorization', 'Bearer ' . self::SUBJECT_TOKEN);
        $request->headers->set(self::PROJECT_ID_HEADER, '123');

        $authenticator = new StorageApiTokenAuthenticator($tokenFactory, $resolverClient);

        $exception = $this->captureAuthException(
            fn() => $authenticator->authenticateToken(new StorageApiTokenAuth(), self::SUBJECT_TOKEN, $request),
        );

        self::assertSame(502, $exception->getCode());
    }

    public static function provideInvalidResolverResponses(): Generator
    {
        yield 'missing storageToken' => ['response' => ['projectId' => 123]];
        yield 'empty storageToken' => ['response' => ['storageToken' => '']];
        yield 'non-string storageToken' => ['response' => ['storageToken' => 123]];
    }

    // ---------------------------------------------------------------------------
    // authenticateToken – failure logging
    // ---------------------------------------------------------------------------

    public function testExchangeLogsUnexpectedResolverStatusWithoutTokenMaterial(): void
    {
        $resolverClient = $this->createMock(ManageApiClient::class);
        $resolverClient
            ->expects(self::once())
            ->method('resolveStorageToken')
            // The Manage client embeds the response body (here the subject token) in the message.
            ->willThrowException(new ManageApiClientException(self::SUBJECT_TOKEN, 500));

        $loggedContext = null;
        $logger = $this->createMock(LoggerInterface::class);
        $logger
            ->expects(self::once())
            ->method('warning')
            ->willReturnCallback(function (string $message, array $context) use (&$loggedContext): void {
                self::assertStringNotContainsString(self::SUBJECT_TOKEN, $message);
                $loggedContext = $context;
            });

        $request = Request::create('https://keboola.com');
        $request->headers->set('Authorization', 'Bearer ' . self::SUBJECT_TOKEN);
        $request->headers->set(self::PROJECT_ID_HEADER, '123');

        $authenticator = new StorageApiTokenAuthenticator(
            $this->createMock(StorageApiTokenFactory::class),
            $resolverClient,
            $logger,
        );

        $exception = $this->captureAuthException(
            fn() => $authenticator->authenticateToken(new StorageApiTokenAuth(), self::SUBJECT_TOKEN, $request),
        );

        self::assertSame(502, $exception->getCode());
        self::assertSame(['projectId' => 123, 'resolverStatus' => 500], $loggedContext);
        // Defense in depth: the subject token must never reach the log context.
        self::assertStringNotContainsString(self::SUBJECT_TOKEN, json_encode($loggedContext, JSON_THROW_ON_ERROR));
    }

    public function testExchangeLogsMissingStorageTokenWithoutLoggingResolverResponse(): void
    {
        $resolverClient = $this->createMock(ManageApiClient::class);
        $resolverClient
            ->expects(self::once())
            ->method('resolveStorageToken')
            ->willReturn(['storageToken' => '', 'leaked' => 'super-secret-storage-token']);

        $loggedContext = null;
        $logger = $this->createMock(LoggerInterface::class);
        $logger
            ->expects(self::once())
            ->method('warning')
            ->willReturnCallback(function (string $message, array $context) use (&$loggedContext): void {
                $loggedContext = $context;
            });

        $request = Request::create('https://keboola.com');
        $request->headers->set('Authorization', 'Bearer ' . self::SUBJECT_TOKEN);
        $request->headers->set(self::PROJECT_ID_HEADER, '123');

        $authenticator = new StorageApiTokenAuthenticator(
            $this->createMock(StorageApiTokenFactory::class),
            $resolverClient,
            $logger,
        );

        $exception = $this->captureAuthException(
            fn() => $authenticator->authenticateToken(new StorageApiTokenAuth(), self::SUBJECT_TOKEN, $request),
        );

        self::assertSame(502, $exception->getCode());
        self::assertSame(['projectId' => 123], $loggedContext);
        // The raw resolver response (which may carry a storageToken value) must not be logged.
        self::assertStringNotContainsString(
            'super-secret-storage-token',
            json_encode($loggedContext, JSON_THROW_ON_ERROR),
        );
    }

    public function testExchangeDoesNotLogWarningForClientFaultStatuses(): void
    {
        $resolverClient = $this->createMock(ManageApiClient::class);
        $resolverClient
            ->expects(self::once())
            ->method('resolveStorageToken')
            ->willThrowException(new ManageApiClientException('unauthorized', 401));

        // 401/403/400 are client faults, not our-side incidents, so they must not raise a warning.
        $logger = $this->createMock(LoggerInterface::class);
        $logger
            ->expects(self::never())
            ->method('warning');

        $request = Request::create('https://keboola.com');
        $request->headers->set('Authorization', 'Bearer ' . self::SUBJECT_TOKEN);
        $request->headers->set(self::PROJECT_ID_HEADER, '123');

        $authenticator = new StorageApiTokenAuthenticator(
            $this->createMock(StorageApiTokenFactory::class),
            $resolverClient,
            $logger,
        );

        $exception = $this->captureAuthException(
            fn() => $authenticator->authenticateToken(new StorageApiTokenAuth(), self::SUBJECT_TOKEN, $request),
        );

        self::assertSame(401, $exception->getCode());
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

    private function captureAuthException(callable $fn): CustomUserMessageAuthenticationException
    {
        try {
            $fn();
        } catch (CustomUserMessageAuthenticationException $e) {
            return $e;
        }

        self::fail('Expected CustomUserMessageAuthenticationException was not thrown.');
    }
}
