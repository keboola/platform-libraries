<?php

declare(strict_types=1);

namespace Keboola\ApiBundle\DependencyInjection;

use Keboola\ApiBundle\Attribute\ApplicationTokenAuth;
use Keboola\ApiBundle\Attribute\StorageApiTokenAuth;
use Keboola\ApiBundle\Security\ApplicationToken\ApplicationTokenAuthenticator;
use Keboola\ApiBundle\Security\ApplicationToken\ManageApiClientFactory;
use Keboola\ApiBundle\Security\StorageApiToken\StorageApiTokenAuthenticator;
use Keboola\ApiBundle\Security\StorageApiToken\StorageApiTokenFactory;
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
    /**
     * Service id of the Manage API client used to exchange Connection programmatic tokens for
     * legacy Storage tokens. Exposed so functional tests can swap it for a mock.
     */
    public const STORAGE_TOKEN_RESOLVER_CLIENT_ID = 'keboola.api_bundle.storage_token_resolver_client';

    /**
     * Path to the projected Kubernetes ServiceAccount token (audience keboola-connection) the
     * resolver client authenticates with. Infra mounts it at this fixed path on every stack.
     */
    private const SERVICE_ACCOUNT_TOKEN_PATH = '/var/run/secrets/connection.keboola.com/serviceaccount/token';

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

        // Manage API client that exchanges programmatic tokens for legacy Storage tokens. It
        // authenticates with the service's projected Kubernetes ServiceAccount JWT (read per
        // request) and calls Connection over the ServiceClient's default DNS.
        $container->register(self::STORAGE_TOKEN_RESOLVER_CLIENT_ID, ManageApiClient::class)
            ->setFactory([new Reference(ManageApiClientFactory::class), 'getClientForServiceAccountTokenPath'])
            ->setArguments([self::SERVICE_ACCOUNT_TOKEN_PATH])
        ;

        $container->register(StorageApiTokenFactory::class)
            ->setArgument('$clientRequestFactory', new Reference(StorageClientRequestFactory::class))
            ->setArgument('$resolverClient', new Reference(self::STORAGE_TOKEN_RESOLVER_CLIENT_ID))
            ->setArgument('$logger', new Reference('logger'))
        ;

        $container->register(StorageApiTokenAuthenticator::class)
            ->setArgument('$tokenFactory', new Reference(StorageApiTokenFactory::class))
        ;
        $authenticators[StorageApiTokenAuth::class] = new Reference(StorageApiTokenAuthenticator::class);
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
