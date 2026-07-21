<?php

declare(strict_types=1);

namespace Keboola\ApiBundle\Tests\Security\StorageApiToken;

use Exception;
use Generator;
use GuzzleHttp\Exception\ConnectException;
use GuzzleHttp\Psr7\Request as GuzzleRequest;
use Keboola\ApiBundle\Security\StorageApiToken\StorageApiTokenFactory;
use Keboola\ApiBundle\StorageApiClient\RequestStorageClientFactory;
use Keboola\ManageApi\Client as ManageApiClient;
use Keboola\ManageApi\ClientException as ManageApiClientException;
use Keboola\ManageApi\MaintenanceException as ManageApiMaintenanceException;
use Keboola\StorageApi\Client;
use Keboola\StorageApi\ClientException;
use Keboola\StorageApi\MaintenanceException;
use Keboola\StorageApiBranch\ClientWrapper;
use Keboola\StorageApiBranch\Factory\AuthType;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;
use Psr\Log\LoggerInterface;
use Psr\Log\NullLogger;
use RuntimeException;
use Symfony\Component\HttpFoundation\Request;
use Symfony\Component\Security\Core\Exception\CustomUserMessageAuthenticationException;

class StorageApiTokenFactoryTest extends TestCase
{
    private const SUBJECT_TOKEN = 'kbc_at_secret';
    private const PROJECT_ID_HEADER = 'X-KBC-ProjectId';

    private function createFactory(
        ?RequestStorageClientFactory $clientFactory = null,
        ?ManageApiClient $resolverClient = null,
        ?LoggerInterface $logger = null,
    ): StorageApiTokenFactory {
        return new StorageApiTokenFactory(
            $clientFactory ?? $this->createMock(RequestStorageClientFactory::class),
            $resolverClient ?? $this->createMock(ManageApiClient::class),
            $logger ?? new NullLogger(),
        );
    }

    private function createProgrammaticTokenRequest(): Request
    {
        $request = Request::create('https://keboola.com');
        $request->headers->set('Authorization', 'Bearer ' . self::SUBJECT_TOKEN);
        $request->headers->set(self::PROJECT_ID_HEADER, '123');

        return $request;
    }

    // ---------------------------------------------------------------------------
    // createFromValue – happy path
    // ---------------------------------------------------------------------------

    #[DataProvider('provideValueAuthTypes')]
    public function testCreateFromValueVerifiesWithGivenAuthType(AuthType $authType): void
    {
        $factoryMock = $this->mockClientFactoryReturningVerifiedToken('tok', $authType);

        $token = $this->createFactory(clientFactory: $factoryMock)
            ->createFromValue(Request::create('https://keboola.com'), 'tok', $authType);

        self::assertSame('tok', $token->getTokenValue());
        self::assertSame($authType, $token->getTokenType());
    }

    public static function provideValueAuthTypes(): Generator
    {
        yield 'legacy storage token' => ['authType' => AuthType::STORAGE_TOKEN];
        yield 'oauth bearer token' => ['authType' => AuthType::BEARER];
    }

    /**
     * Builds a RequestStorageClientFactory mock whose wrapper verifies successfully, asserting it is
     * asked to build the client for the expected token/auth type.
     */
    private function mockClientFactoryReturningVerifiedToken(
        string $expectedToken,
        AuthType $expectedAuthType,
    ): RequestStorageClientFactory {
        $clientMock = $this->createMock(Client::class);
        $clientMock->expects(self::once())->method('verifyToken')->willReturn(['id' => '42', 'description' => 'test']);
        $clientMock->expects(self::once())->method('getTokenString')->willReturn($expectedToken);

        $wrapperMock = $this->createMock(ClientWrapper::class);
        $wrapperMock->expects(self::once())->method('getBasicClient')->willReturn($clientMock);

        $factoryMock = $this->createMock(RequestStorageClientFactory::class);
        $factoryMock
            ->expects(self::once())
            ->method('createClientWrapper')
            ->with($expectedToken, $expectedAuthType, self::anything())
            ->willReturn($wrapperMock);

        return $factoryMock;
    }

    // ---------------------------------------------------------------------------
    // createFromValue – exception mapping
    // ---------------------------------------------------------------------------

    /**
     * @param class-string<Exception> $expectedExceptionClass
     */
    #[DataProvider('provideExceptionData')]
    public function testVerifyFailure(
        ClientException $clientException,
        string $expectedExceptionClass,
        string $expectedExceptionMessage,
        int $expectedExceptionCode,
    ): void {
        $clientMock = $this->createMock(Client::class);
        $clientMock
            ->expects(self::once())
            ->method('verifyToken')
            ->willThrowException($clientException);

        $wrapperMock = $this->createMock(ClientWrapper::class);
        $wrapperMock
            ->expects(self::once())
            ->method('getBasicClient')
            ->willReturn($clientMock);

        $factoryMock = $this->createMock(RequestStorageClientFactory::class);
        $factoryMock
            ->expects(self::once())
            ->method('createClientWrapper')
            ->willReturn($wrapperMock);

        self::expectException($expectedExceptionClass);
        self::expectExceptionMessage($expectedExceptionMessage);
        self::expectExceptionCode($expectedExceptionCode);

        $this->createFactory(clientFactory: $factoryMock)
            ->createFromValue(Request::create('https://keboola.com'), 'token', AuthType::STORAGE_TOKEN);
    }

    public static function provideExceptionData(): Generator
    {
        yield 'user exception' => [
            'clientException' => new ClientException('Invalid access token', 401),
            'expectedExceptionClass' => CustomUserMessageAuthenticationException::class,
            'expectedExceptionMessage' => 'Invalid access token',
            'expectedExceptionCode' => 401,
        ];
        yield 'maintenance exception' => [
            'clientException' => new MaintenanceException('Maintenance', null, 'token'),
            'expectedExceptionClass' => MaintenanceException::class,
            'expectedExceptionMessage' => 'Maintenance',
            'expectedExceptionCode' => 503,
        ];
        yield 'server exception' => [
            'clientException' => new ClientException('Invalid access token', 500),
            'expectedExceptionClass' => ClientException::class,
            'expectedExceptionMessage' => 'Invalid access token',
            'expectedExceptionCode' => 500,
        ];
    }

    // ---------------------------------------------------------------------------
    // exchangeFromProgrammaticToken – exchange success, no Storage API call
    // ---------------------------------------------------------------------------

    public function testExchangeFromProgrammaticTokenBuildsTokenFromResolverDetailWithoutStorageCall(): void
    {
        $tokenDetail = [
            'id' => '42',
            'description' => 'resolved',
            'owner' => [
                'id' => 123,
                'features' => ['feat-a'],
            ],
        ];

        $resolverClient = $this->createMock(ManageApiClient::class);
        $resolverClient
            ->expects(self::once())
            ->method('resolveStorageToken')
            ->with(123, self::SUBJECT_TOKEN)
            ->willReturn([
                'storageToken' => 'legacy-token',
                'projectId' => 123,
                'tokenId' => '42',
                'userId' => '7',
                'expiresAt' => null,
                'tokenDetail' => $tokenDetail,
            ]);

        // The resolver response carries the full token detail, so Storage API must not be hit.
        $factoryMock = $this->createMock(RequestStorageClientFactory::class);
        $factoryMock
            ->expects(self::never())
            ->method('createClientWrapper');

        $originalRequest = $this->createProgrammaticTokenRequest();

        $token = $this->createFactory(clientFactory: $factoryMock, resolverClient: $resolverClient)
            ->exchangeFromProgrammaticToken($originalRequest, self::SUBJECT_TOKEN);

        self::assertSame('legacy-token', $token->getTokenValue());
        self::assertSame($tokenDetail, $token->getTokenInfo());
        self::assertSame('123', $token->getProjectId());
        self::assertSame(['feat-a'], $token->getFeatures());
        // The exchange yields a legacy Storage token.
        self::assertSame(AuthType::STORAGE_TOKEN, $token->getTokenType());

        // The original request must remain untouched.
        self::assertSame(
            'Bearer ' . self::SUBJECT_TOKEN,
            $originalRequest->headers->get('Authorization'),
            'Original request Authorization header must not be mutated.',
        );
    }

    // ---------------------------------------------------------------------------
    // exchangeFromProgrammaticToken – project id header validation
    // ---------------------------------------------------------------------------

    #[DataProvider('provideInvalidProjectIdHeaders')]
    public function testExchangeFromProgrammaticTokenThrowsWith400ForInvalidProjectIdHeader(?string $headerValue): void
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

        $factory = $this->createFactory(resolverClient: $resolverClient);

        $exception = $this->captureAuthException(
            fn() => $factory->exchangeFromProgrammaticToken($request, self::SUBJECT_TOKEN),
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
    // exchangeFromProgrammaticToken – resolver error mapping
    // ---------------------------------------------------------------------------

    #[DataProvider('provideResolverClientErrorMapping')]
    public function testExchangeMapsResolverClientErrorToExpectedCode(int $statusCode, int $expectedCode): void
    {
        $resolverClient = $this->createMock(ManageApiClient::class);
        $resolverClient
            ->expects(self::once())
            ->method('resolveStorageToken')
            ->willThrowException(new ManageApiClientException(self::SUBJECT_TOKEN, $statusCode));

        $factory = $this->createFactory(resolverClient: $resolverClient);

        $exception = $this->captureAuthException(
            fn() => $factory->exchangeFromProgrammaticToken(
                $this->createProgrammaticTokenRequest(),
                self::SUBJECT_TOKEN,
            ),
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
            ->willThrowException(new ManageApiMaintenanceException('Maintenance', 30, []));

        $logger = $this->createMock(LoggerInterface::class);
        $logger
            ->expects(self::once())
            ->method('warning')
            ->with(self::stringContains('maintenance'), ['projectId' => 123, 'retryAfter' => 30]);

        $factory = $this->createFactory(resolverClient: $resolverClient, logger: $logger);

        $exception = $this->captureAuthException(
            fn() => $factory->exchangeFromProgrammaticToken(
                $this->createProgrammaticTokenRequest(),
                self::SUBJECT_TOKEN,
            ),
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

        $factory = $this->createFactory(resolverClient: $resolverClient);

        $exception = $this->captureAuthException(
            fn() => $factory->exchangeFromProgrammaticToken(
                $this->createProgrammaticTokenRequest(),
                self::SUBJECT_TOKEN,
            ),
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

        $factory = $this->createFactory(resolverClient: $resolverClient);

        $exception = $this->captureAuthException(
            fn() => $factory->exchangeFromProgrammaticToken(
                $this->createProgrammaticTokenRequest(),
                self::SUBJECT_TOKEN,
            ),
        );

        self::assertSame(502, $exception->getCode());
    }

    #[DataProvider('provideInvalidResolverResponses')]
    public function testExchangeThrowsWith502WhenResolverResponseLacksStorageTokenOrDetail(mixed $response): void
    {
        $resolverClient = $this->createMock(ManageApiClient::class);
        $resolverClient
            ->expects(self::once())
            ->method('resolveStorageToken')
            ->willReturn($response);

        $clientFactory = $this->createMock(RequestStorageClientFactory::class);
        $clientFactory
            ->expects(self::never())
            ->method('createClientWrapper');

        $factory = $this->createFactory(
            clientFactory: $clientFactory,
            resolverClient: $resolverClient,
        );

        $exception = $this->captureAuthException(
            fn() => $factory->exchangeFromProgrammaticToken(
                $this->createProgrammaticTokenRequest(),
                self::SUBJECT_TOKEN,
            ),
        );

        self::assertSame(502, $exception->getCode());
    }

    public static function provideInvalidResolverResponses(): Generator
    {
        $tokenDetail = ['id' => '42', 'owner' => ['id' => 123, 'features' => []]];

        yield 'missing storageToken' => ['response' => ['projectId' => 123, 'tokenDetail' => $tokenDetail]];
        yield 'empty storageToken' => ['response' => ['storageToken' => '', 'tokenDetail' => $tokenDetail]];
        yield 'non-string storageToken' => ['response' => ['storageToken' => 123, 'tokenDetail' => $tokenDetail]];
        // Missing detail = Connection deploy without the detail-enriched resolver response
        // (keboola/connection#7604); the bundle must not fall back to a second verify call.
        yield 'missing tokenDetail' => ['response' => ['storageToken' => 'legacy-token']];
        yield 'empty tokenDetail' => ['response' => ['storageToken' => 'legacy-token', 'tokenDetail' => []]];
        yield 'non-array tokenDetail' => ['response' => ['storageToken' => 'legacy-token', 'tokenDetail' => 'x']];
    }

    // ---------------------------------------------------------------------------
    // exchangeFromProgrammaticToken – failure logging
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

        $factory = $this->createFactory(resolverClient: $resolverClient, logger: $logger);

        $exception = $this->captureAuthException(
            fn() => $factory->exchangeFromProgrammaticToken(
                $this->createProgrammaticTokenRequest(),
                self::SUBJECT_TOKEN,
            ),
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

        $factory = $this->createFactory(resolverClient: $resolverClient, logger: $logger);

        $exception = $this->captureAuthException(
            fn() => $factory->exchangeFromProgrammaticToken(
                $this->createProgrammaticTokenRequest(),
                self::SUBJECT_TOKEN,
            ),
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

        $factory = $this->createFactory(resolverClient: $resolverClient, logger: $logger);

        $exception = $this->captureAuthException(
            fn() => $factory->exchangeFromProgrammaticToken(
                $this->createProgrammaticTokenRequest(),
                self::SUBJECT_TOKEN,
            ),
        );

        self::assertSame(401, $exception->getCode());
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
