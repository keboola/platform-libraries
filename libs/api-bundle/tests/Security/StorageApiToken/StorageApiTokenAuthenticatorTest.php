<?php

declare(strict_types=1);

namespace Keboola\ApiBundle\Tests\Security\StorageApiToken;

use Exception;
use Generator;
use Keboola\ApiBundle\Attribute\StorageApiTokenAuth;
use Keboola\ApiBundle\Attribute\StorageApiTokenRole;
use Keboola\ApiBundle\Security\StorageApiToken\StorageApiToken;
use Keboola\ApiBundle\Security\StorageApiToken\StorageApiTokenAuthenticator;
use Keboola\StorageApi\Client;
use Keboola\StorageApi\ClientException;
use Keboola\StorageApi\MaintenanceException;
use Keboola\StorageApiBranch\ClientWrapper;
use Keboola\StorageApiBranch\Factory\StorageClientRequestFactory;
use PHPUnit\Framework\TestCase;
use Symfony\Component\HttpFoundation\Request;
use Symfony\Component\HttpFoundation\RequestStack;
use Symfony\Component\Security\Core\Exception\AccessDeniedException;
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

    /**
     * @dataProvider provideExceptionData
     * @param class-string<Exception> $expectedExceptionClass
     */
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

    public static function provideSuccessAuthorizationData(): iterable
    {
        yield 'no requirements' => [
            'attribute' => new StorageApiTokenAuth(),
            'token' => new StorageApiToken([
                'owner' => [
                    'features' => [],
                ],
            ], 'token'),
        ];

        yield 'require project features' => [
            'attribute' => new StorageApiTokenAuth(features: ['feat-a', 'feat-b']),
            'token' => new StorageApiToken([
                'owner' => [
                    'features' => ['feat-a', 'feat-b', 'feat-c'],
                ],
            ], 'token'),
        ];

        yield 'require admin token' => [
            'attribute' => new StorageApiTokenAuth(isAdmin: true),
            'token' => new StorageApiToken([
                'admin' => [
                    'role' => 'guest',
                ],
                'owner' => [
                    'features' => [],
                ],
            ], 'token'),
        ];

        yield 'require non-admin token' => [
            'attribute' => new StorageApiTokenAuth(isAdmin: false),
            'token' => new StorageApiToken([
                'owner' => [
                    'features' => [],
                ],
            ], 'token'),
        ];

        yield 'require specific role' => [
            'attribute' => new StorageApiTokenAuth(role: StorageApiTokenRole::ROLE_ADMIN),
            'token' => new StorageApiToken([
                'admin' => [
                    'role' => 'admin',
                ],
                'owner' => [
                    'features' => [],
                ],
            ], 'token'),
        ];

        yield 'require not specific role' => [
            'attribute' => new StorageApiTokenAuth(role: StorageApiTokenRole::ANY & ~StorageApiTokenRole::ROLE_ADMIN),
            'token' => new StorageApiToken([
                'admin' => [
                    'role' => 'guest',
                ],
                'owner' => [
                    'features' => [],
                ],
            ], 'token'),
        ];
    }

    /** @dataProvider provideSuccessAuthorizationData */
    public function testAuthorizeTokenSuccess(StorageApiTokenAuth $attribute, StorageApiToken $token): void
    {
        $storageApiTokenAuthenticator = new StorageApiTokenAuthenticator(
            $this->createMock(StorageClientRequestFactory::class),
            new RequestStack(),
        );
        $storageApiTokenAuthenticator->authorizeToken($attribute, $token);

        $this->expectNotToPerformAssertions();
    }

    public static function provideFailureAuthorizationData(): iterable
    {
        yield 'missing single feature' => [
            'attribute' => new StorageApiTokenAuth(features: ['feat-a']),
            'token' => new StorageApiToken([
                'owner' => [
                    'features' => ['feat-b', 'feat-c'],
                ],
            ], 'token'),
            'error' => 'Authentication token is valid but missing following features: feat-a',
        ];

        yield 'missing one of multiple' => [
            'attribute' => new StorageApiTokenAuth(features: ['feat-a', 'feat-b']),
            'token' => new StorageApiToken([
                'owner' => [
                    'features' => ['feat-b', 'feat-c'],
                ],
            ], 'token'),
            'error' => 'Authentication token is valid but missing following features: feat-a',
        ];

        yield 'missing multiple features' => [
            'attribute' => new StorageApiTokenAuth(features: ['feat-a', 'feat-b']),
            'token' => new StorageApiToken([
                'owner' => [
                    'features' => ['feat-c'],
                ],
            ], 'token'),
            'error' => 'Authentication token is valid but missing following features: feat-a, feat-b',
        ];

        yield 'require admin token' => [
            'attribute' => new StorageApiTokenAuth(isAdmin: true),
            'token' => new StorageApiToken([
                'owner' => [
                    'features' => [],
                ],
            ], 'token'),
            'error' => 'Authentication token is valid but is not admin token',
        ];

        yield 'require non-admin token' => [
            'attribute' => new StorageApiTokenAuth(isAdmin: false),
            'token' => new StorageApiToken([
                'admin' => [
                    'role' => 'guest',
                ],
                'owner' => [
                    'features' => [],
                ],
            ], 'token'),
            'error' => 'Authentication token is valid but is admin token',
        ];

        yield 'require specific role' => [
            'attribute' => new StorageApiTokenAuth(role: StorageApiTokenRole::ROLE_ADMIN),
            'token' => new StorageApiToken([
                'admin' => [
                    'role' => 'guest',
                ],
                'owner' => [
                    'features' => [],
                ],
            ], 'token'),
            'error' => 'Authentication token is valid but does not have any of required roles: admin',
        ];

        yield 'require one of roles' => [
            'attribute' => new StorageApiTokenAuth(
                role: StorageApiTokenRole::ROLE_ADMIN | StorageApiTokenRole::ROLE_GUEST,
            ),
            'token' => new StorageApiToken([
                'admin' => [
                    'role' => 'readonly',
                ],
                'owner' => [
                    'features' => [],
                ],
            ], 'token'),
            'error' => 'Authentication token is valid but does not have any of required roles: admin, guest',
        ];
    }

    /** @dataProvider provideFailureAuthorizationData */
    public function testAuthorizeTokenFailure(
        StorageApiTokenAuth $attribute,
        StorageApiToken $token,
        string $error,
    ): void {
        $storageApiTokenAuthenticator = new StorageApiTokenAuthenticator(
            $this->createMock(StorageClientRequestFactory::class),
            new RequestStack(),
        );

        $this->expectException(AccessDeniedException::class);
        $this->expectExceptionMessage($error);

        $storageApiTokenAuthenticator->authorizeToken($attribute, $token);
    }
}
