<?php

declare(strict_types=1);

namespace Keboola\ApiBundle\DependencyInjection;

use Keboola\ApiBundle\Attribute\ManageApiTokenAuth;
use Keboola\ApiBundle\Attribute\StorageApiTokenAuth;
use Keboola\ApiBundle\Security\ManageApiToken\ManageApiClientFactory;
use Keboola\ApiBundle\Security\ManageApiToken\ManageApiTokenAuthenticator;
use Keboola\ApiBundle\Security\StorageApiToken\StorageApiTokenAuthenticator;
use Keboola\ManageApi\Client as ManageApiClient;
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

        $authenticators = [];
        $this->setupStorageApiAuthenticator($container, $authenticators);
        $this->setupManageApiAuthenticator($container, $config, $authenticators);

        $container->getDefinition('keboola.api_bundle.security.authenticators_locator')
            ->setArguments([
                $authenticators,
            ])
        ;

        $this->setupServiceClient($container, $config);
    }

    private function setupStorageApiAuthenticator(ContainerBuilder $container, array &$authenticators): void
    {
        if (!class_exists(StorageClientRequestFactory::class)) {
            return;
        }

        $container->register(StorageApiTokenAuthenticator::class)
            ->setArgument('$clientRequestFactory', new Reference(StorageClientRequestFactory::class))
            ->setArgument('$requestStack', new Reference('request_stack'))
        ;

        $authenticators[StorageApiTokenAuth::class] = new Reference(StorageApiTokenAuthenticator::class);
    }

    private function setupManageApiAuthenticator(
        ContainerBuilder $container,
        array $config,
        array &$authenticators,
    ): void {
        if (!class_exists(ManageApiClient::class)) {
            return;
        }

        $container->register(ManageApiClientFactory::class)
            ->setArgument('$appName', $config['app_name'])
            ->setArgument('$serviceClient', new Reference(ServiceClient::class))
        ;

        $container->register(ManageApiTokenAuthenticator::class)
            ->setArgument('$manageApiClientFactory', new Reference(ManageApiClientFactory::class))
        ;

        $authenticators[ManageApiTokenAuth::class] = new Reference(ManageApiTokenAuthenticator::class);
    }

    private function setupServiceClient(ContainerBuilder $container, array $config): void
    {
        $container->getDefinition(ServiceClient::class)
            ->setArgument('$defaultDnsType', ServiceDnsType::from($config['default_service_dns_type']))
        ;
    }
}
