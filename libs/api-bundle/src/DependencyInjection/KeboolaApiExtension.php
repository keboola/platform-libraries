<?php

declare(strict_types=1);

namespace Keboola\ApiBundle\DependencyInjection;

use Keboola\ApiBundle\Attribute\ManageApiTokenAuth;
use Keboola\ApiBundle\Attribute\StorageApiTokenAuth;
use Keboola\ApiBundle\Security\ManageApiToken\ManageApiClientFactory;
use Keboola\ApiBundle\Security\ManageApiToken\ManageApiTokenAuthenticator;
use Keboola\ApiBundle\Security\StorageApiToken\StorageApiToken;
use Keboola\ApiBundle\Security\StorageApiToken\StorageApiTokenAuthenticator;
use Keboola\ManageApi\Client as ManageApiClient;
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
        $loader->load('security.php');

        $configuration = new Configuration();
        $config = $this->processConfiguration($configuration, $configs);

        $config['app_name'] = $container->resolveEnvPlaceholders($config['app_name'], true);
        $config['storage_api_url'] = $container->resolveEnvPlaceholders($config['storage_api_url'], true);

        $authenticators = [];
        $this->setupStorageApiAuthenticator($container, $authenticators);
        $this->setupManageApiAuthenticator($container, $config, $authenticators);

        $container->getDefinition('keboola.api_bundle.security.authenticators_locator')
            ->setArguments([
                $authenticators,
            ])
        ;
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
            ->setArgument('$storageApiUrl', $config['storage_api_url'])
        ;

        $container->register(ManageApiTokenAuthenticator::class)
            ->setArgument('$manageApiClientFactory', new Reference(ManageApiClientFactory::class))
        ;

        $authenticators[ManageApiTokenAuth::class] = new Reference(ManageApiTokenAuthenticator::class);
    }
}
