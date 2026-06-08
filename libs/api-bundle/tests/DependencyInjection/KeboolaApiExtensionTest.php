<?php

declare(strict_types=1);

namespace Keboola\ApiBundle\Tests\DependencyInjection;

use Keboola\ApiBundle\AuthBridge\ManageApiStorageTokenResolver;
use Keboola\ApiBundle\AuthBridge\StorageTokenResolverInterface;
use Keboola\ApiBundle\DependencyInjection\KeboolaApiExtension;
use Keboola\ApiBundle\Security\ConnectionToken\ConnectionTokenAuthenticator;
use Keboola\ApiBundle\Security\StorageApiToken\StorageApiTokenAuthenticator;
use Keboola\ApiBundle\Security\StorageApiToken\StorageApiTokenExchange;
use Keboola\ApiBundle\Security\StorageApiToken\StorageApiTokenFactory;
use Keboola\ServiceClient\ServiceDnsType;
use PHPUnit\Framework\TestCase;
use Symfony\Component\DependencyInjection\ContainerBuilder;

class KeboolaApiExtensionTest extends TestCase
{
    // -------------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------------

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

    public function testStorageApiServicesAreRegisteredWithDefaultConfig(): void
    {
        $container = $this->buildContainer([[]]);

        self::assertTrue(
            $container->hasDefinition(StorageApiTokenFactory::class),
            'StorageApiTokenFactory must be registered',
        );
        self::assertTrue(
            $container->hasDefinition(StorageTokenResolverInterface::class),
            'StorageTokenResolverInterface must be registered',
        );
        self::assertTrue(
            $container->hasDefinition(StorageApiTokenExchange::class),
            'StorageApiTokenExchange must be registered',
        );
        self::assertTrue(
            $container->hasDefinition(StorageApiTokenAuthenticator::class),
            'StorageApiTokenAuthenticator must be registered',
        );
        self::assertTrue(
            $container->hasDefinition(ConnectionTokenAuthenticator::class),
            'ConnectionTokenAuthenticator must be registered',
        );
    }

    // -------------------------------------------------------------------------
    // Resolver implementation class
    // -------------------------------------------------------------------------

    public function testStorageTokenResolverDefinitionUsesManageApiImplementation(): void
    {
        $container = $this->buildContainer([[]]);

        $definition = $container->getDefinition(StorageTokenResolverInterface::class);

        self::assertSame(ManageApiStorageTokenResolver::class, $definition->getClass());
    }

    // -------------------------------------------------------------------------
    // Default argument values
    // -------------------------------------------------------------------------

    public function testStorageApiTokenAuthenticatorDefaultArguments(): void
    {
        $container = $this->buildContainer([[]]);

        $definition = $container->getDefinition(StorageApiTokenAuthenticator::class);

        self::assertFalse(
            $definition->getArgument('$exchangeEnabled'),
            '$exchangeEnabled must default to false',
        );
        self::assertSame(
            'X-KBC-ProjectId',
            $definition->getArgument('$projectIdHeader'),
            '$projectIdHeader must default to X-KBC-ProjectId',
        );
    }

    public function testStorageTokenResolverDefaultServiceAccountTokenPath(): void
    {
        $container = $this->buildContainer([[]]);

        $definition = $container->getDefinition(StorageTokenResolverInterface::class);

        self::assertSame(
            '/var/run/secrets/connection.keboola.com/serviceaccount/token',
            $definition->getArgument('$serviceAccountTokenPath'),
            '$serviceAccountTokenPath must match the default SA token mount path',
        );
    }

    public function testConnectionTokenAuthenticatorDefaultProjectIdHeader(): void
    {
        $container = $this->buildContainer([[]]);

        $definition = $container->getDefinition(ConnectionTokenAuthenticator::class);

        self::assertSame(
            'X-KBC-ProjectId',
            $definition->getArgument('$projectIdHeader'),
            '$projectIdHeader must default to X-KBC-ProjectId',
        );
    }

    // -------------------------------------------------------------------------
    // Overridden configuration values
    // -------------------------------------------------------------------------

    public function testStorageTokenExchangeConfigOverridesAreApplied(): void
    {
        $container = $this->buildContainer([[
            'storage_token_exchange' => [
                'enabled' => true,
                'project_id_header' => 'X-Project',
                'service_account_token_path' => '/custom/token',
                'connection_dns_type' => 'public',
            ],
        ]]);

        $authenticatorDef = $container->getDefinition(StorageApiTokenAuthenticator::class);
        self::assertTrue(
            $authenticatorDef->getArgument('$exchangeEnabled'),
            '$exchangeEnabled must be true when configured',
        );
        self::assertSame(
            'X-Project',
            $authenticatorDef->getArgument('$projectIdHeader'),
            '$projectIdHeader must reflect configured value',
        );

        $resolverDef = $container->getDefinition(StorageTokenResolverInterface::class);
        self::assertSame(
            '/custom/token',
            $resolverDef->getArgument('$serviceAccountTokenPath'),
            '$serviceAccountTokenPath must reflect configured path',
        );
        self::assertSame(
            ServiceDnsType::PUBLIC,
            $resolverDef->getArgument('$connectionDnsType'),
            '$connectionDnsType must be ServiceDnsType::PUBLIC for "public"',
        );
    }

    public function testConnectionTokenAuthenticatorReflectsOverriddenProjectIdHeader(): void
    {
        $container = $this->buildContainer([[
            'storage_token_exchange' => [
                'project_id_header' => 'X-My-Project',
            ],
        ]]);

        $definition = $container->getDefinition(ConnectionTokenAuthenticator::class);

        self::assertSame(
            'X-My-Project',
            $definition->getArgument('$projectIdHeader'),
        );
    }

    public function testInternalConnectionDnsTypeIsDefaultForResolver(): void
    {
        $container = $this->buildContainer([[]]);

        $resolverDef = $container->getDefinition(StorageTokenResolverInterface::class);

        self::assertSame(
            ServiceDnsType::INTERNAL,
            $resolverDef->getArgument('$connectionDnsType'),
            '$connectionDnsType must default to ServiceDnsType::INTERNAL',
        );
    }
}
