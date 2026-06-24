<?php

declare(strict_types=1);

namespace Keboola\ApiBundle\Tests\StorageApiClient;

use Keboola\ApiBundle\Security\StorageApiToken\StorageApiToken as SecurityStorageApiToken;
use Keboola\ApiBundle\StorageApiClient\RequestStorageClientFactory;
use Keboola\ApiBundle\StorageApiClient\StorageApiClientResolver;
use Keboola\ApiBundle\StorageApiClient\StorageClientApiFactory;
use Keboola\StorageApiBranch\Factory\ClientOptions;
use PHPUnit\Framework\TestCase;
use RuntimeException;
use Symfony\Component\HttpFoundation\Request;
use Symfony\Component\HttpKernel\ControllerMetadata\ArgumentMetadata;
use Symfony\Component\HttpKernel\ControllerMetadata\ArgumentMetadataFactory;
use Symfony\Component\Security\Core\Authentication\Token\Storage\TokenStorageInterface;
use Symfony\Component\Security\Core\Authentication\Token\TokenInterface;

class StorageApiClientResolverTest extends TestCase
{
    private function resolver(TokenStorageInterface $tokenStorage): StorageApiClientResolver
    {
        return new StorageApiClientResolver(
            new StorageClientApiFactory(new ClientOptions('https://connection.test')),
            $tokenStorage,
        );
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
            public function __invoke(RequestStorageClientFactory $storage): void
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
        self::assertInstanceOf(RequestStorageClientFactory::class, $result[0]);
        self::assertSame('resolved-token', $result[0]->createClientWrapper()->getClientOptionsReadOnly()->getToken());
    }

    public function testThrowsWhenNoStorageApiTokenInSecurityContext(): void
    {
        $controller = new class {
            public function __invoke(RequestStorageClientFactory $storage): void
            {
            }
        };

        $tokenStorage = $this->createMock(TokenStorageInterface::class);
        $tokenStorage->expects(self::once())->method('getToken')->willReturn(null);

        $this->expectException(RuntimeException::class);
        $this->expectExceptionMessage('#[StorageApiTokenAuth]');

        [...$this->resolver($tokenStorage)->resolve(new Request(), $this->metadataFor($controller, 'storage'))];
    }
}
