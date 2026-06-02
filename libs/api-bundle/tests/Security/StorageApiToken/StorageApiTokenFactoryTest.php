<?php

declare(strict_types=1);

namespace Keboola\ApiBundle\Tests\Security\StorageApiToken;

use Exception;
use Generator;
use Keboola\ApiBundle\Security\StorageApiToken\StorageApiTokenFactory;
use Keboola\StorageApi\Client;
use Keboola\StorageApi\ClientException;
use Keboola\StorageApi\MaintenanceException;
use Keboola\StorageApiBranch\ClientWrapper;
use Keboola\StorageApiBranch\Factory\StorageClientRequestFactory;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;
use Symfony\Component\HttpFoundation\Request;
use Symfony\Component\Security\Core\Exception\CustomUserMessageAuthenticationException;

class StorageApiTokenFactoryTest extends TestCase
{
    // ---------------------------------------------------------------------------
    // createFromRequest – happy path
    // ---------------------------------------------------------------------------

    public function testCreateFromRequestSuccess(): void
    {
        $clientMock = $this->createMock(Client::class);
        $clientMock
            ->expects(self::once())
            ->method('verifyToken')
            ->willReturn(['id' => '42', 'description' => 'test']);
        $clientMock
            ->expects(self::once())
            ->method('getTokenString')
            ->willReturn('tok');

        $wrapperMock = $this->createMock(ClientWrapper::class);
        $wrapperMock
            ->expects(self::once())
            ->method('getBasicClient')
            ->willReturn($clientMock);

        $factoryMock = $this->createMock(StorageClientRequestFactory::class);
        $factoryMock
            ->expects(self::once())
            ->method('createClientWrapper')
            ->willReturn($wrapperMock);

        $request = Request::create('https://keboola.com');
        $request->headers->set(StorageClientRequestFactory::TOKEN_HEADER, 'tok');

        $token = (new StorageApiTokenFactory($factoryMock))->createFromRequest($request);

        self::assertSame('tok', $token->getTokenValue());
    }

    // ---------------------------------------------------------------------------
    // createFromRequest – exception mapping
    // ---------------------------------------------------------------------------

    /**
     * @param class-string<Exception> $expectedExceptionClass
     */
    #[DataProvider('provideExceptionData')]
    public function testCreateFromRequestFailure(
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

        $factoryMock = $this->createMock(StorageClientRequestFactory::class);
        $factoryMock
            ->expects(self::once())
            ->method('createClientWrapper')
            ->willReturn($wrapperMock);

        $request = Request::create('https://keboola.com');
        $request->headers->set(StorageClientRequestFactory::TOKEN_HEADER, 'token');

        self::expectException($expectedExceptionClass);
        self::expectExceptionMessage($expectedExceptionMessage);
        self::expectExceptionCode($expectedExceptionCode);

        (new StorageApiTokenFactory($factoryMock))->createFromRequest($request);
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
    // createFromResolvedToken – request mutation and isolation
    // ---------------------------------------------------------------------------

    public function testCreateFromResolvedTokenPassesLegacyTokenAndDropsAuthorization(): void
    {
        /** @var Request|null $capturedRequest */
        $capturedRequest = null;

        $clientMock = $this->createMock(Client::class);
        $clientMock
            ->expects(self::once())
            ->method('verifyToken')
            ->willReturn(['id' => '7', 'description' => 'resolved']);
        $clientMock
            ->expects(self::once())
            ->method('getTokenString')
            ->willReturn('legacy-token');

        $wrapperMock = $this->createMock(ClientWrapper::class);
        $wrapperMock
            ->expects(self::once())
            ->method('getBasicClient')
            ->willReturn($clientMock);

        $factoryMock = $this->createMock(StorageClientRequestFactory::class);
        $factoryMock
            ->expects(self::once())
            ->method('createClientWrapper')
            ->willReturnCallback(function (Request $r) use (&$capturedRequest, $wrapperMock) {
                $capturedRequest = $r;
                return $wrapperMock;
            });

        $originalRequest = Request::create('https://keboola.com');
        $originalRequest->headers->set('Authorization', 'Bearer kbc_at_secret');
        $originalRequest->headers->set('X-KBC-RunId', 'run-1');

        $token = (new StorageApiTokenFactory($factoryMock))
            ->createFromResolvedToken($originalRequest, 'legacy-token');

        self::assertSame('legacy-token', $token->getTokenValue());
        self::assertNotNull($capturedRequest);

        // The request passed to createClientWrapper must NOT have the Authorization header.
        self::assertFalse(
            $capturedRequest->headers->has('Authorization'),
            'Authorization header must be removed from the cloned request.',
        );

        // The legacy token must be set as the Storage API token header.
        self::assertSame(
            'legacy-token',
            $capturedRequest->headers->get(StorageClientRequestFactory::TOKEN_HEADER),
        );

        // Other headers must be preserved on the clone.
        self::assertSame(
            'run-1',
            $capturedRequest->headers->get('X-KBC-RunId'),
        );

        // The ORIGINAL request must remain untouched.
        self::assertSame(
            'Bearer kbc_at_secret',
            $originalRequest->headers->get('Authorization'),
            'Original request Authorization header must not be mutated.',
        );
    }
}
