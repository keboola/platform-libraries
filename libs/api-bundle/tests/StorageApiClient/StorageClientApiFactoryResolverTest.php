<?php

declare(strict_types=1);

namespace Keboola\ApiBundle\Tests\StorageApiClient;

use Keboola\ApiBundle\Security\StorageApiToken\StorageApiToken as SecurityStorageApiToken;
use Keboola\ApiBundle\StorageApiClient\StorageClientApiFactory;
use Keboola\ApiBundle\StorageApiClient\StorageClientApiFactoryResolver;
use Keboola\StorageApiBranch\Factory\ClientOptions;
use PHPUnit\Framework\TestCase;
use RuntimeException;
use Symfony\Component\HttpFoundation\Request;
use Symfony\Component\HttpKernel\ControllerMetadata\ArgumentMetadata;
use Symfony\Component\HttpKernel\ControllerMetadata\ArgumentMetadataFactory;
use Symfony\Component\Security\Core\Authentication\Token\Storage\TokenStorageInterface;
use Symfony\Component\Security\Core\Authentication\Token\TokenInterface;

class StorageClientApiFactoryResolverTest extends TestCase
{
    private function resolver(TokenStorageInterface $tokenStorage): StorageClientApiFactoryResolver
    {
        return new StorageClientApiFactoryResolver(new ClientOptions('https://connection.test'), $tokenStorage);
    }

    private function metadataFor(object $controller, string $arg): ArgumentMetadata
    {
        foreach ((new ArgumentMetadataFactory())->createArgumentMetadata($controller) as $metadata) {
            if ($metadata->getName() === $arg) {
                return $metadata;
            }
        }

        self::fail(sprintf('Controller has no argument "%s"', $arg));
    }

    public function testResolvesNothingForOtherArgumentTypes(): void
    {
        $controller = new class {
            public function __invoke(string $foo): void
            {
            }
        };

        $tokenStorage = $this->createMock(TokenStorageInterface::class);
        $tokenStorage->expects(self::never())->method('getToken');

        $result = $this->resolver($tokenStorage)->resolve(new Request(), $this->metadataFor($controller, 'foo'));

        self::assertSame([], [...$result]);
    }

    public function testResolvesBoundFactoryBuildingClientFromSecurityToken(): void
    {
        $controller = new class {
            public function __invoke(StorageClientApiFactory $storage): void
            {
            }
        };

        $storageToken = new SecurityStorageApiToken([], 'resolved-token');
        $securityToken = $this->createMock(TokenInterface::class);
        $securityToken->expects(self::once())->method('getUser')->willReturn($storageToken);
        $tokenStorage = $this->createMock(TokenStorageInterface::class);
        $tokenStorage->expects(self::once())->method('getToken')->willReturn($securityToken);

        $result = [...$this->resolver($tokenStorage)->resolve(
            new Request(),
            $this->metadataFor($controller, 'storage'),
        )];

        self::assertCount(1, $result);
        self::assertInstanceOf(StorageClientApiFactory::class, $result[0]);
        self::assertSame('resolved-token', $result[0]->createClientWrapper()->getClientOptionsReadOnly()->getToken());
    }

    public function testThrowsForRequiredArgumentWhenNoStorageApiToken(): void
    {
        $controller = new class {
            public function __invoke(StorageClientApiFactory $storage): void
            {
            }
        };

        $tokenStorage = $this->createMock(TokenStorageInterface::class);
        $tokenStorage->expects(self::once())->method('getToken')->willReturn(null);

        $this->expectException(RuntimeException::class);
        $this->expectExceptionMessage('#[StorageApiTokenAuth]');

        [...$this->resolver($tokenStorage)->resolve(new Request(), $this->metadataFor($controller, 'storage'))];
    }

    public function testResolvesNullForNullableArgumentWhenNoStorageApiToken(): void
    {
        // Dual-guarded controller (e.g. also #[ApplicationTokenAuth]) authenticated through the
        // other path: the security user is not a StorageApiToken, so a nullable argument gets null.
        $controller = new class {
            public function __invoke(?StorageClientApiFactory $storage): void
            {
            }
        };

        $tokenStorage = $this->createMock(TokenStorageInterface::class);
        $tokenStorage->expects(self::once())->method('getToken')->willReturn(null);

        $result = [...$this->resolver($tokenStorage)->resolve(
            new Request(),
            $this->metadataFor($controller, 'storage'),
        )];

        self::assertSame([null], $result);
    }

    public function testResolvesFactoryForNullableArgumentWhenStorageApiTokenPresent(): void
    {
        $controller = new class {
            public function __invoke(?StorageClientApiFactory $storage): void
            {
            }
        };

        $storageToken = new SecurityStorageApiToken([], 'resolved-token');
        $securityToken = $this->createMock(TokenInterface::class);
        $securityToken->expects(self::once())->method('getUser')->willReturn($storageToken);
        $tokenStorage = $this->createMock(TokenStorageInterface::class);
        $tokenStorage->expects(self::once())->method('getToken')->willReturn($securityToken);

        $result = [...$this->resolver($tokenStorage)->resolve(
            new Request(),
            $this->metadataFor($controller, 'storage'),
        )];

        self::assertCount(1, $result);
        self::assertInstanceOf(StorageClientApiFactory::class, $result[0]);
    }
}
