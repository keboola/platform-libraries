<?php

declare(strict_types=1);

namespace Keboola\ApiBundle\DependencyInjection;

use Keboola\ApiBundle\Attribute\ApplicationTokenAuth;
use Keboola\ApiBundle\Attribute\ConnectionTokenAuth;
use Keboola\ApiBundle\Attribute\StorageApiTokenAuth;
use Keboola\ApiBundle\AuthBridge\ManageApiStorageTokenResolver;
use Keboola\ApiBundle\AuthBridge\StorageTokenResolverInterface;
use Keboola\ApiBundle\Security\ApplicationToken\ApplicationTokenAuthenticator;
use Keboola\ApiBundle\Security\ApplicationToken\ManageApiClientFactory;
use Keboola\ApiBundle\Security\ConnectionToken\ConnectionTokenAuthenticator;
use Keboola\ApiBundle\Security\StorageApiToken\StorageApiTokenAuthenticator;
use Keboola\ApiBundle\Security\StorageApiToken\StorageApiTokenExchange;
use Keboola\ApiBundle\Security\StorageApiToken\StorageApiTokenFactory;
use Keboola\ServiceClient\ServiceClient;
use Keboola\ServiceClient\ServiceDnsType;
use Keboola\StorageApiBranch\Factory\StorageClientRequestFactory;
use Symfony\Component\Config\FileLocator;
use Symfony\Component\DependencyInjection\ContainerBuilder;
use Symfony\Component\DependencyInjection\Loader\PhpFileLoader;
use Symfony\Component\DependencyInjection\Reference;
use Symfony\Component\HttpKernel\DependencyInjection\Extension;

class KeboolaApiExtension extends Extension
{
    public function load(array $configs, ContainerBuilder $container): void
    {
        $loader = new PhpFileLoader($container, new FileLocator(__DIR__ . '/../Resources/config'));
        $loader->load('api_bundle.php');

        $configuration = new Configuration();
        $config = $this->processConfiguration($configuration, $configs);

        $config['app_name'] = $container->resolveEnvPlaceholders($config['app_name'], true);

        $defaultServiceDnsType = $container->resolveEnvPlaceholders($config['default_service_dns_type'], true);
        assert(is_string($defaultServiceDnsType) || $defaultServiceDnsType instanceof ServiceDnsType);
        $container->setParameter('keboola_api_bundle.default_service_dns_type', $defaultServiceDnsType);

        // Shared by #[ApplicationTokenAuth] and by the Storage token exchange resolver.
        $container->register(ManageApiClientFactory::class)
            ->setArgument('$appName', $config['app_name'])
            ->setArgument('$serviceClient', new Reference(ServiceClient::class))
        ;

        $authenticators = [];
        $this->setupStorageApiAuthenticator($container, $config, $authenticators);
        $this->setupApplicationTokenAuthenticator($container, $authenticators);

        $container->getDefinition('keboola.api_bundle.security.authenticators_locator')
            ->setArguments([
                $authenticators,
            ])
        ;
    }

    private function setupStorageApiAuthenticator(
        ContainerBuilder $container,
        array $config,
        array &$authenticators,
    ): void {
        if (!class_exists(StorageClientRequestFactory::class)) {
            return;
        }

        $exchangeConfig = $config['storage_token_exchange'];
        assert(is_array($exchangeConfig));

        $connectionDnsType = $exchangeConfig['connection_dns_type'];
        assert(is_string($connectionDnsType));

        $container->register(StorageApiTokenFactory::class)
            ->setArgument('$clientRequestFactory', new Reference(StorageClientRequestFactory::class))
        ;

        $container->register(StorageTokenResolverInterface::class, ManageApiStorageTokenResolver::class)
            ->setArgument('$clientFactory', new Reference(ManageApiClientFactory::class))
            ->setArgument('$serviceAccountTokenPath', $exchangeConfig['service_account_token_path'])
            ->setArgument('$connectionDnsType', ServiceDnsType::from($connectionDnsType))
        ;

        $container->register(StorageApiTokenExchange::class)
            ->setArgument('$resolver', new Reference(StorageTokenResolverInterface::class))
            ->setArgument('$tokenFactory', new Reference(StorageApiTokenFactory::class))
        ;

        $container->register(StorageApiTokenAuthenticator::class)
            ->setArgument('$tokenFactory', new Reference(StorageApiTokenFactory::class))
            ->setArgument('$tokenExchange', new Reference(StorageApiTokenExchange::class))
            ->setArgument('$exchangeEnabled', $exchangeConfig['enabled'])
            ->setArgument('$projectIdHeader', $exchangeConfig['project_id_header'])
        ;
        $authenticators[StorageApiTokenAuth::class] = new Reference(StorageApiTokenAuthenticator::class);

        $container->register(ConnectionTokenAuthenticator::class)
            ->setArgument('$tokenExchange', new Reference(StorageApiTokenExchange::class))
            ->setArgument('$projectIdHeader', $exchangeConfig['project_id_header'])
        ;
        $authenticators[ConnectionTokenAuth::class] = new Reference(ConnectionTokenAuthenticator::class);
    }

    private function setupApplicationTokenAuthenticator(
        ContainerBuilder $container,
        array &$authenticators,
    ): void {
        $container->register(ApplicationTokenAuthenticator::class)
            ->setArgument('$manageApiClientFactory', new Reference(ManageApiClientFactory::class))
        ;

        $authenticators[ApplicationTokenAuth::class] = new Reference(
            ApplicationTokenAuthenticator::class,
        );
    }
}
