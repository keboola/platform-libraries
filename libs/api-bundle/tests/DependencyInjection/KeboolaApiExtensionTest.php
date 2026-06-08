<?php

declare(strict_types=1);

namespace Keboola\ApiBundle\Tests\DependencyInjection;

use Keboola\ApiBundle\DependencyInjection\KeboolaApiExtension;
use Keboola\ApiBundle\Security\ApplicationToken\ManageApiClientFactory;
use Keboola\ApiBundle\Security\StorageApiToken\StorageApiTokenAuthenticator;
use Keboola\ApiBundle\Security\StorageApiToken\StorageApiTokenFactory;
use Keboola\ManageApi\Client as ManageApiClient;
use Keboola\ServiceClient\ServiceDnsType;
use PHPUnit\Framework\TestCase;
use Symfony\Component\DependencyInjection\ContainerBuilder;
use Symfony\Component\DependencyInjection\Reference;

class KeboolaApiExtensionTest extends TestCase
{
    private const SERVICE_ACCOUNT_TOKEN_PATH = '/var/run/secrets/connection.keboola.com/serviceaccount/token';

    /**
     * @param array<array<mixed>> $configs
     */
    private function buildContainer(array $configs): ContainerBuilder
    {
        $container = new ContainerBuilder();
        $container->setParameter('app_name', 'test-app');
        $extension = new KeboolaApiExtension();
        $extension->load($configs, $container);

        return $container;
    }

    // -------------------------------------------------------------------------
    // Service registration
    // -------------------------------------------------------------------------

    public function testStorageApiServicesAreRegistered(): void
    {
        $container = $this->buildContainer([[]]);

        self::assertTrue(
            $container->hasDefinition(StorageApiTokenFactory::class),
            'StorageApiTokenFactory must be registered',
        );
        self::assertTrue(
            $container->hasDefinition(KeboolaApiExtension::STORAGE_TOKEN_RESOLVER_CLIENT_ID),
            'Storage token resolver client must be registered',
        );
        self::assertTrue(
            $container->hasDefinition(StorageApiTokenAuthenticator::class),
            'StorageApiTokenAuthenticator must be registered',
        );
    }

    // -------------------------------------------------------------------------
    // Resolver client wiring
    // -------------------------------------------------------------------------

    public function testResolverClientIsBuiltFromServiceAccountTokenPathOverInternalDns(): void
    {
        $container = $this->buildContainer([[]]);

        $definition = $container->getDefinition(KeboolaApiExtension::STORAGE_TOKEN_RESOLVER_CLIENT_ID);

        self::assertSame(ManageApiClient::class, $definition->getClass());

        $factory = $definition->getFactory();
        self::assertIsArray($factory);
        self::assertInstanceOf(Reference::class, $factory[0]);
        self::assertSame(ManageApiClientFactory::class, (string) $factory[0]);
        self::assertSame('getClientForServiceAccountTokenPath', $factory[1]);

        self::assertSame(
            [self::SERVICE_ACCOUNT_TOKEN_PATH, ServiceDnsType::INTERNAL],
            $definition->getArguments(),
            'Resolver client must use the fixed SA token path and internal DNS',
        );
    }

    public function testAuthenticatorReceivesResolverClient(): void
    {
        $container = $this->buildContainer([[]]);

        $definition = $container->getDefinition(StorageApiTokenAuthenticator::class);

        $resolverClient = $definition->getArgument('$resolverClient');
        self::assertInstanceOf(Reference::class, $resolverClient);
        self::assertSame(KeboolaApiExtension::STORAGE_TOKEN_RESOLVER_CLIENT_ID, (string) $resolverClient);
    }
}
