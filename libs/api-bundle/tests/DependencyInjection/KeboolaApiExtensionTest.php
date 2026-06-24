<?php

declare(strict_types=1);

namespace Keboola\ApiBundle\Tests\DependencyInjection;

use Keboola\ApiBundle\DependencyInjection\KeboolaApiExtension;
use Keboola\ApiBundle\Security\ApplicationToken\ManageApiClientFactory;
use Keboola\ApiBundle\Security\StorageApiToken\StorageApiTokenAuthenticator;
use Keboola\ApiBundle\Security\StorageApiToken\StorageApiTokenFactory;
use Keboola\ApiBundle\StorageApiClient\StorageApiClientResolver;
use Keboola\ManageApi\Client as ManageApiClient;
use Keboola\ServiceClient\ServiceClient;
use Keboola\StorageApiBranch\Factory\ClientOptions;
use PHPUnit\Framework\TestCase;
use Symfony\Component\DependencyInjection\ContainerBuilder;
use Symfony\Component\DependencyInjection\Definition;
use Symfony\Component\DependencyInjection\Reference;
use Symfony\Component\Security\Core\Authentication\Token\Storage\TokenStorageInterface;

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

    public function testStorageApiClientResolverIsRegisteredWithBaseOptionsAndTagged(): void
    {
        $container = $this->buildContainer([['app_name' => 'storage-test-app']]);

        self::assertTrue(
            $container->hasDefinition(StorageApiClientResolver::class),
            'StorageApiClientResolver must be registered',
        );

        $definition = $container->getDefinition(StorageApiClientResolver::class);
        self::assertArrayHasKey('controller.argument_value_resolver', $definition->getTags());

        $baseClientOptions = $definition->getArgument('$baseClientOptions');
        self::assertInstanceOf(Definition::class, $baseClientOptions);
        self::assertSame(ClientOptions::class, $baseClientOptions->getClass());

        // userAgent is the configured app name
        self::assertSame('storage-test-app', $baseClientOptions->getArgument('$userAgent'));

        // logger is the shared @logger service
        $logger = $baseClientOptions->getArgument('$logger');
        self::assertInstanceOf(Reference::class, $logger);
        self::assertSame('logger', (string) $logger);

        // url is resolved at runtime from ServiceClient::getConnectionServiceUrl()
        $url = $baseClientOptions->getArgument('$url');
        self::assertInstanceOf(Definition::class, $url);
        $urlFactory = $url->getFactory();
        self::assertIsArray($urlFactory);
        self::assertInstanceOf(Reference::class, $urlFactory[0]);
        self::assertSame(ServiceClient::class, (string) $urlFactory[0]);
        self::assertSame('getConnectionServiceUrl', $urlFactory[1]);

        $tokenStorage = $definition->getArgument('$tokenStorage');
        self::assertInstanceOf(Reference::class, $tokenStorage);
        self::assertSame(TokenStorageInterface::class, (string) $tokenStorage);
    }

    // -------------------------------------------------------------------------
    // Resolver client wiring
    // -------------------------------------------------------------------------

    public function testResolverClientIsBuiltFromServiceAccountTokenPath(): void
    {
        $container = $this->buildContainer([[]]);

        $definition = $container->getDefinition(KeboolaApiExtension::STORAGE_TOKEN_RESOLVER_CLIENT_ID);

        self::assertSame(ManageApiClient::class, $definition->getClass());

        $factory = $definition->getFactory();
        self::assertIsArray($factory);
        self::assertInstanceOf(Reference::class, $factory[0]);
        self::assertSame(ManageApiClientFactory::class, (string) $factory[0]);
        self::assertSame('getClientForServiceAccountTokenPath', $factory[1]);

        // No explicit DNS type - the client follows the ServiceClient's configured default.
        self::assertSame(
            [self::SERVICE_ACCOUNT_TOKEN_PATH],
            $definition->getArguments(),
            'Resolver client must use the fixed SA token path and the default DNS type',
        );
    }

    public function testTokenFactoryReceivesResolverClientAndLogger(): void
    {
        $container = $this->buildContainer([[]]);

        $definition = $container->getDefinition(StorageApiTokenFactory::class);

        $resolverClient = $definition->getArgument('$resolverClient');
        self::assertInstanceOf(Reference::class, $resolverClient);
        self::assertSame(KeboolaApiExtension::STORAGE_TOKEN_RESOLVER_CLIENT_ID, (string) $resolverClient);

        $logger = $definition->getArgument('$logger');
        self::assertInstanceOf(Reference::class, $logger);
        self::assertSame('logger', (string) $logger);
    }

    public function testAuthenticatorReceivesTokenFactory(): void
    {
        $container = $this->buildContainer([[]]);

        $definition = $container->getDefinition(StorageApiTokenAuthenticator::class);

        $tokenFactory = $definition->getArgument('$tokenFactory');
        self::assertInstanceOf(Reference::class, $tokenFactory);
        self::assertSame(StorageApiTokenFactory::class, (string) $tokenFactory);
    }
}
