<?php

declare(strict_types=1);

namespace Keboola\ApiBundle\Tests\Test;

use Keboola\ApiBundle\AuthBridge\StorageTokenResolverInterface;
use Keboola\ApiBundle\Security\ApplicationToken\ManageApiClientFactory;
use Keboola\ApiBundle\Security\StorageApiToken\StorageApiToken;
use Keboola\ApiBundle\Test\AuthenticatorTestTrait;
use Keboola\StorageApiBranch\Factory\StorageClientRequestFactory;
use PHPUnit\Framework\TestCase;
use Symfony\Component\DependencyInjection\Container;
use Symfony\Component\DependencyInjection\ContainerInterface;

class AuthenticatorTestTraitTest extends TestCase
{
    use AuthenticatorTestTrait;

    private static Container $testContainer;

    protected static function getContainer(): ContainerInterface
    {
        return self::$testContainer;
    }

    protected function setUp(): void
    {
        self::$testContainer = new Container();
    }

    public function testSetupFakeStorageApiToken(): void
    {
        $token = $this->setupFakeStorageApiToken(
            tokenString: 'my-token',
            projectId: '456',
            features: ['feat-a'],
            adminId: '99',
        );

        self::assertSame('my-token', $token->getTokenValue());
        self::assertSame('456', $token->getProjectId());
        self::assertSame(['feat-a'], $token->getFeatures());

        self::assertInstanceOf(
            StorageClientRequestFactory::class,
            self::$testContainer->get(StorageClientRequestFactory::class),
        );
    }

    public function testSetupFakeManageApiToken(): void
    {
        $this->setupFakeManageApiToken('manage-token', ['some:scope'], ['feat-b']);

        $factory = self::$testContainer->get(ManageApiClientFactory::class);
        self::assertInstanceOf(ManageApiClientFactory::class, $factory);

        $data = $factory->getClientForManageToken('manage-token')->verifyToken();
        self::assertSame(['some:scope'], $data['scopes']);
        self::assertArrayHasKey('user', $data);
        self::assertSame(['feat-b'], $data['user']['features']);

        // The Kubernetes ServiceAccount JWT path is stubbed identically.
        $jwtData = $factory->getClientForServiceAccountToken('manage-token')->verifyToken();
        self::assertSame(['some:scope'], $jwtData['scopes']);
    }

    public function testSetupFakeConnectionToken(): void
    {
        $token = $this->setupFakeConnectionToken(
            projectId: '789',
            features: ['feat-x'],
            tokenString: 'tok-x',
            adminId: '7',
        );

        // The returned StorageApiToken carries the values passed in.
        self::assertSame('tok-x', $token->getTokenValue());
        self::assertSame('789', $token->getProjectId());
        self::assertSame(['feat-x'], $token->getFeatures());

        // The StorageClientRequestFactory stub is wired into the container.
        self::assertInstanceOf(
            StorageClientRequestFactory::class,
            self::$testContainer->get(StorageClientRequestFactory::class),
        );

        // A StorageTokenResolverInterface mock is registered in the container.
        $resolver = self::$testContainer->get(StorageTokenResolverInterface::class);
        self::assertInstanceOf(StorageTokenResolverInterface::class, $resolver);

        // The mock resolver returns a ResolvedStorageToken whose projectId matches.
        $resolved = $resolver->resolve(789, 'kbc_at_x');
        self::assertSame(789, $resolved->projectId);
    }
}
