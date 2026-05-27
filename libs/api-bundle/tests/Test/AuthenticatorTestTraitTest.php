<?php

declare(strict_types=1);

namespace Keboola\ApiBundle\Tests\Test;

use Keboola\ApiBundle\Security\KubernetesServiceAccount\ManageApiClientFactory;
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

        $data = $factory->getClient('manage-token')->verifyToken();
        self::assertSame(['some:scope'], $data['scopes']);
        self::assertArrayHasKey('user', $data);
        self::assertSame(['feat-b'], $data['user']['features']);

        // The Kubernetes ServiceAccount JWT path is stubbed identically.
        $jwtData = $factory->getClientForJwt('manage-token')->verifyToken();
        self::assertSame(['some:scope'], $jwtData['scopes']);
    }
}
