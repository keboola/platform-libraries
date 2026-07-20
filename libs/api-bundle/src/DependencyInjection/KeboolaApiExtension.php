<?php

declare(strict_types=1);

namespace Keboola\ApiBundle\DependencyInjection;

use Keboola\ApiBundle\Attribute\ApplicationTokenAuth;
use Keboola\ApiBundle\Attribute\StorageApiTokenAuth;
use Keboola\ApiBundle\Security\ApplicationToken\ApplicationTokenAuthenticator;
use Keboola\ApiBundle\Security\ApplicationToken\ManageApiClientFactory;
use Keboola\ApiBundle\Security\StorageApiToken\StorageApiTokenAuthenticator;
use Keboola\ApiBundle\Security\StorageApiToken\StorageApiTokenFactory;
use Keboola\ApiBundle\StorageApiClient\RequestStorageClientFactory;
use Keboola\ApiBundle\StorageApiClient\StorageClientApiFactoryResolver;
use Keboola\ManageApi\Client as ManageApiClient;
use Keboola\ServiceClient\ServiceClient;
use Keboola\ServiceClient\ServiceDnsType;
use Keboola\StorageApiBranch\ClientWrapper;
use Keboola\StorageApiBranch\Factory\ClientOptions;
use Symfony\Component\Config\FileLocator;
use Symfony\Component\DependencyInjection\ContainerBuilder;
use Symfony\Component\DependencyInjection\Definition;
use Symfony\Component\DependencyInjection\Loader\PhpFileLoader;
use Symfony\Component\DependencyInjection\Reference;
use Symfony\Component\HttpKernel\DependencyInjection\Extension;
use Symfony\Component\Security\Core\Authentication\Token\Storage\TokenStorageInterface;

class KeboolaApiExtension extends Extension
{
    /**
     * Service id of the Manage API client used to exchange Connection programmatic tokens for
     * legacy Storage tokens. Exposed so functional tests can swap it for a mock.
     */
    public const STORAGE_TOKEN_RESOLVER_CLIENT_ID = 'keboola.api_bundle.storage_token_resolver_client';

    /**
     * Service id of the base Storage {@see ClientOptions} shared by token verification and the
     * controller-facing Storage client, so both use identical Connection URL / logger / options.
     */
    private const STORAGE_CLIENT_BASE_OPTIONS_ID = 'keboola.api_bundle.storage_client_base_options';

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
        if (!class_exists(ClientWrapper::class)) {
            return;
        }

        // Base Storage ClientOptions shared by token verification (RequestStorageClientFactory) and
        // the controller-facing StorageClientApiFactory (via StorageClientApiFactoryResolver), so a
        // request is verified with the same Connection URL / logger / user agent / tuned options the
        // controller-facing client uses. Connection URL comes from the ServiceClient; consumers tune
        // it through the storage_client_options node.
        $connectionUrl = (new Definition())
            ->setFactory([new Reference(ServiceClient::class), 'getConnectionServiceUrl']);

        $baseClientOptions = (new Definition(ClientOptions::class))
            ->setArgument('$url', $connectionUrl)
            ->setArgument('$logger', new Reference('logger'))
            ->setArgument('$userAgent', $config['app_name']);

        $storageClientOptions = $config['storage_client_options'] ?? [];
        assert(is_array($storageClientOptions));
        $this->applyStorageClientOptions($baseClientOptions, $storageClientOptions);

        $container->setDefinition(self::STORAGE_CLIENT_BASE_OPTIONS_ID, $baseClientOptions);

        // Manage API client that exchanges programmatic tokens for legacy Storage tokens. It
        // authenticates with the service's projected Kubernetes ServiceAccount JWT (read per
        // request) and calls Connection over the ServiceClient's default DNS.
        $container->register(self::STORAGE_TOKEN_RESOLVER_CLIENT_ID, ManageApiClient::class)
            ->setFactory([new Reference(ManageApiClientFactory::class), 'getClientForServiceAccountTokenPath'])
            ->setArguments([self::SERVICE_ACCOUNT_TOKEN_PATH])
        ;

        $container->register(RequestStorageClientFactory::class)
            ->setArgument('$baseClientOptions', new Reference(self::STORAGE_CLIENT_BASE_OPTIONS_ID))
        ;

        $container->register(StorageApiTokenFactory::class)
            ->setArgument('$clientFactory', new Reference(RequestStorageClientFactory::class))
            ->setArgument('$resolverClient', new Reference(self::STORAGE_TOKEN_RESOLVER_CLIENT_ID))
            ->setArgument('$logger', new Reference('logger'))
        ;

        $container->register(StorageApiTokenAuthenticator::class)
            ->setArgument('$tokenFactory', new Reference(StorageApiTokenFactory::class))
        ;
        $authenticators[StorageApiTokenAuth::class] = new Reference(StorageApiTokenAuthenticator::class);

        // StorageClientApiFactory controller-argument value resolver
        $container->register(StorageClientApiFactoryResolver::class)
            ->setArgument('$baseClientOptions', new Reference(self::STORAGE_CLIENT_BASE_OPTIONS_ID))
            ->setArgument('$tokenStorage', new Reference(TokenStorageInterface::class))
            ->addTag('controller.argument_value_resolver')
        ;
    }

    /**
     * Merge consumer-configured Storage ClientOptions onto the bundle-built base definition.
     *
     * @param array<array-key, mixed> $options
     */
    private function applyStorageClientOptions(Definition $baseClientOptions, array $options): void
    {
        if (isset($options['service'])) {
            // Merge the referenced ClientOptions' non-null values on top at instantiation.
            $service = $options['service'];
            assert(is_string($service));
            $baseClientOptions->addMethodCall('addValuesFrom', [new Reference($service)]);
            return;
        }

        $scalarArgs = [
            'backoff_max_tries' => '$backoffMaxTries',
            'aws_retries' => '$awsRetries',
            'aws_debug' => '$awsDebug',
            'retry_on_maintenance' => '$retryOnMaintenance',
            'use_branch_storage' => '$useBranchStorage',
            'user_agent' => '$userAgent',
        ];
        foreach ($scalarArgs as $key => $arg) {
            if (isset($options[$key])) {
                $baseClientOptions->setArgument($arg, $options[$key]);
            }
        }

        // logger is a service id → reference, overriding the default @logger.
        if (isset($options['logger'])) {
            $logger = $options['logger'];
            assert(is_string($logger));
            $baseClientOptions->setArgument('$logger', new Reference($logger));
        }
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
