<?php

declare(strict_types=1);

namespace Keboola\ApiBundle\Tests\Security\StorageApiToken;

use Exception;
use Generator;
use Keboola\ApiBundle\Attribute\StorageApiTokenAuth;
use Keboola\ApiBundle\Security\StorageApiToken\StorageApiTokenAuthenticator;
use Keboola\StorageApi\Client;
use Keboola\StorageApi\ClientException;
use Keboola\StorageApi\MaintenanceException;
use Keboola\StorageApiBranch\ClientWrapper;
use Keboola\StorageApiBranch\Factory\StorageClientRequestFactory;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;
use Symfony\Component\HttpFoundation\Request;
use Symfony\Component\HttpFoundation\RequestStack;
use Symfony\Component\Security\Core\Exception\CustomUserMessageAuthenticationException;

class StorageApiTokenAuthenticatorTest extends TestCase
{
    public function testAuthenticateTokenSuccess(): void
    {
        $clientMock = $this->createMock(Client::class);
        $clientMock
            ->expects(self::once())
            ->method('verifyToken')
            ->willReturn([]);
        $clientMock
            ->method('getTokenString')
            ->willReturn('');

        $clientWrapperMock = $this->createMock(ClientWrapper::class);
        $clientWrapperMock
            ->method('getBasicClient')
            ->willReturn($clientMock);

        $clientRequestFactoryMock = $this->createMock(StorageClientRequestFactory::class);
        $clientRequestFactoryMock
            ->method('createClientWrapper')
            ->willReturn($clientWrapperMock);

        $requestStack = new RequestStack();
        $request = Request::create('https://keboola.com');
        $request->headers->add([StorageClientRequestFactory::TOKEN_HEADER => 'token']);
        $requestStack->push($request);

        $storageApiTokenAuthenticator = new StorageApiTokenAuthenticator($clientRequestFactoryMock, $requestStack);
        $storageApiTokenAuthenticator->authenticateToken(new StorageApiTokenAuth(), 'token');
    }

    /**
     * @param class-string<Exception> $expectedExceptionClass
     */
    #[DataProvider('provideExceptionData')]
    public function testAuthenticateTokenFailure(
        ClientException $clientException,
        string $expectedExceptionClass,
        string $expectedExceptionMessage,
        int $expectedExceptionCode,
    ): void {
        $clientMock = $this->createMock(Client::class);
        $clientMock
            ->method('verifyToken')
            ->willThrowException($clientException);

        $clientWrapperMock = $this->createMock(ClientWrapper::class);
        $clientWrapperMock
            ->method('getBasicClient')
            ->willReturn($clientMock);

        $clientRequestFactoryMock = $this->createMock(StorageClientRequestFactory::class);
        $clientRequestFactoryMock
            ->method('createClientWrapper')
            ->willReturn($clientWrapperMock);

        $requestStack = new RequestStack();
        $request = Request::create('https://keboola.com');
        $request->headers->add([StorageClientRequestFactory::TOKEN_HEADER => 'token']);
        $requestStack->push($request);

        $storageApiTokenAuthenticator = new StorageApiTokenAuthenticator($clientRequestFactoryMock, $requestStack);

        self::expectException($expectedExceptionClass);
        self::expectExceptionMessage($expectedExceptionMessage);
        self::expectExceptionCode($expectedExceptionCode);
        $storageApiTokenAuthenticator->authenticateToken(new StorageApiTokenAuth(), 'token');
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

    public function testExtractTokenFromPrimaryHeader(): void
    {
        $authenticator = new StorageApiTokenAuthenticator(
            $this->createMock(StorageClientRequestFactory::class),
            new RequestStack(),
        );

        $request = Request::create('https://keboola.com');
        $request->headers->set('X-StorageApi-Token', 'my-token');

        self::assertSame('my-token', $authenticator->extractToken($request));
    }

    public function testExtractTokenFromBearerHeader(): void
    {
        $authenticator = new StorageApiTokenAuthenticator(
            $this->createMock(StorageClientRequestFactory::class),
            new RequestStack(),
        );

        $request = Request::create('https://keboola.com');
        $request->headers->set('Authorization', 'Bearer my-bearer-token');

        self::assertSame('my-bearer-token', $authenticator->extractToken($request));
    }

    public function testExtractTokenFromAuthorizationHeaderWithoutBearer(): void
    {
        $authenticator = new StorageApiTokenAuthenticator(
            $this->createMock(StorageClientRequestFactory::class),
            new RequestStack(),
        );

        $request = Request::create('https://keboola.com');
        $request->headers->set('Authorization', 'some-token-without-bearer');

        self::assertSame('some-token-without-bearer', $authenticator->extractToken($request));
    }

    public function testExtractTokenPrefersStorageApiTokenHeader(): void
    {
        $authenticator = new StorageApiTokenAuthenticator(
            $this->createMock(StorageClientRequestFactory::class),
            new RequestStack(),
        );

        $request = Request::create('https://keboola.com');
        $request->headers->set('X-StorageApi-Token', 'primary-token');
        $request->headers->set('Authorization', 'Bearer bearer-token');

        self::assertSame('primary-token', $authenticator->extractToken($request));
    }

    public function testExtractTokenReturnsNullWhenNoHeader(): void
    {
        $authenticator = new StorageApiTokenAuthenticator(
            $this->createMock(StorageClientRequestFactory::class),
            new RequestStack(),
        );

        $request = Request::create('https://keboola.com');

        self::assertNull($authenticator->extractToken($request));
    }
}
