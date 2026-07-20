<?php

declare(strict_types=1);

namespace Keboola\ApiBundle\Tests\DependencyInjection;

use Keboola\ApiBundle\DependencyInjection\KeboolaApiExtension;
use Keboola\ApiBundle\Security\ApplicationToken\ManageApiClientFactory;
use Keboola\ApiBundle\Security\StorageApiToken\StorageApiTokenAuthenticator;
use Keboola\ApiBundle\Security\StorageApiToken\StorageApiTokenFactory;
use Keboola\ApiBundle\StorageApiClient\RequestStorageClientFactory;
use Keboola\ApiBundle\StorageApiClient\StorageClientApiFactoryResolver;
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

    /**
     * The base Storage ClientOptions is a shared service both token verification
     * ({@see RequestStorageClientFactory}) and the controller-facing resolver reference. Resolve
     * that reference from the given service argument and return the underlying definition, asserting
     * both consumers point at the very same service id.
     */
    private function resolveSharedBaseClientOptions(ContainerBuilder $container): Definition
    {
        $resolverRef = $container->getDefinition(StorageClientApiFactoryResolver::class)
            ->getArgument('$baseClientOptions');
        self::assertInstanceOf(Reference::class, $resolverRef);

        $verificationRef = $container->getDefinition(RequestStorageClientFactory::class)
            ->getArgument('$baseClientOptions');
        self::assertInstanceOf(Reference::class, $verificationRef);

        // token verification and the controller-facing client must share one base-options service
        self::assertSame((string) $resolverRef, (string) $verificationRef);

        return $container->getDefinition((string) $resolverRef);
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
            $container->hasDefinition(RequestStorageClientFactory::class),
            'RequestStorageClientFactory must be registered',
        );
        self::assertTrue(
            $container->hasDefinition(StorageApiTokenAuthenticator::class),
            'StorageApiTokenAuthenticator must be registered',
        );
    }

    public function testStorageClientApiFactoryResolverIsRegisteredWithBaseOptionsAndTagged(): void
    {
        $container = $this->buildContainer([['app_name' => 'storage-test-app']]);

        self::assertTrue(
            $container->hasDefinition(StorageClientApiFactoryResolver::class),
            'StorageClientApiFactoryResolver must be registered',
        );

        $definition = $container->getDefinition(StorageClientApiFactoryResolver::class);
        self::assertArrayHasKey('controller.argument_value_resolver', $definition->getTags());

        $baseClientOptions = $this->resolveSharedBaseClientOptions($container);
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

    public function testTokenFactoryReceivesClientFactoryResolverClientAndLogger(): void
    {
        $container = $this->buildContainer([[]]);

        $definition = $container->getDefinition(StorageApiTokenFactory::class);

        $clientFactory = $definition->getArgument('$clientFactory');
        self::assertInstanceOf(Reference::class, $clientFactory);
        self::assertSame(RequestStorageClientFactory::class, (string) $clientFactory);

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

    // -------------------------------------------------------------------------
    // storage_client_options
    // -------------------------------------------------------------------------

    public function testStorageClientOptionsObjectFormSetsScalarArgs(): void
    {
        $container = $this->buildContainer([[
            'storage_client_options' => [
                'backoff_max_tries' => 10,
                'aws_retries' => 5,
                'aws_debug' => true,
                'retry_on_maintenance' => true,
                'use_branch_storage' => false,
                'user_agent' => 'custom-ua/1.0',
            ],
        ]]);

        $base = $this->resolveSharedBaseClientOptions($container);

        self::assertSame(10, $base->getArgument('$backoffMaxTries'));
        self::assertSame(5, $base->getArgument('$awsRetries'));
        self::assertTrue($base->getArgument('$awsDebug'));
        self::assertTrue($base->getArgument('$retryOnMaintenance'));
        self::assertFalse($base->getArgument('$useBranchStorage'));
        self::assertSame('custom-ua/1.0', $base->getArgument('$userAgent'));

        // object form must not add any method calls
        self::assertSame([], $base->getMethodCalls());
    }

    public function testStorageClientOptionsLoggerOverridesDefaultLogger(): void
    {
        $container = $this->buildContainer([[
            'storage_client_options' => [
                'logger' => 'monolog.logger.storage_api',
            ],
        ]]);

        $base = $this->resolveSharedBaseClientOptions($container);

        $logger = $base->getArgument('$logger');
        self::assertInstanceOf(Reference::class, $logger);
        self::assertSame('monolog.logger.storage_api', (string) $logger);
    }

    public function testStorageClientOptionsServiceFormAddsAddValuesFromMerge(): void
    {
        $container = $this->buildContainer([[
            'storage_client_options' => 'app.storage_options',
        ]]);

        $base = $this->resolveSharedBaseClientOptions($container);

        $calls = $base->getMethodCalls();
        self::assertCount(1, $calls);

        $call = $calls[0];
        self::assertIsArray($call);
        self::assertSame('addValuesFrom', $call[0]);
        self::assertIsArray($call[1]);

        $reference = $call[1][0];
        self::assertInstanceOf(Reference::class, $reference);
        self::assertSame('app.storage_options', (string) $reference);

        // service form must not touch the scalar args
        $args = $base->getArguments();
        self::assertArrayNotHasKey('$backoffMaxTries', $args);
    }

    public function testStorageClientOptionsAbsentLeavesBaseUntouched(): void
    {
        $container = $this->buildContainer([[]]);

        $base = $this->resolveSharedBaseClientOptions($container);

        self::assertSame([], $base->getMethodCalls());
        self::assertSame(
            ['$url', '$logger', '$userAgent'],
            array_keys($base->getArguments()),
        );
    }
}
