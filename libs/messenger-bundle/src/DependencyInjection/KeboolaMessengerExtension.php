<?php

declare(strict_types=1);

namespace Keboola\MessengerBundle\DependencyInjection;

use InvalidArgumentException;
use Symfony\Component\Config\Definition\Configurator\DefinitionConfigurator;
use Symfony\Component\Config\FileLocator;
use Symfony\Component\DependencyInjection\ContainerBuilder;
use Symfony\Component\DependencyInjection\Extension\AbstractExtension;
use Symfony\Component\DependencyInjection\Loader\Configurator\ContainerConfigurator;
use Symfony\Component\DependencyInjection\Loader\PhpFileLoader;

class KeboolaMessengerExtension extends AbstractExtension
{
    private const PLATFORM_AWS = 'aws';
    private const PLATFORM_AZURE = 'azure';
    private const PLATFORM_GCP = 'gcp';

    public function configure(DefinitionConfigurator $definition): void
    {
        $definition->rootNode() // @phpstan-ignore-line - root node is always ArrayNode
            ->children()
                ->scalarNode('platform')
                    ->isRequired()
                    ->cannotBeEmpty()
                ->end()

                ->scalarNode('connection_events_queue_dsn')
                    ->isRequired()
                    ->cannotBeEmpty()
                ->end()
            ->end()
        ;
    }

    public function prependExtension(ContainerConfigurator $container, ContainerBuilder $builder): void
    {
        $configuration = $this->getConfiguration([], $builder);
        assert($configuration !== null);

        $configs = $builder->getExtensionConfig($this->getAlias());
        $config = $this->processConfiguration($configuration, $configs);

        $platform = $builder->resolveEnvPlaceholders($config['platform'] ?? '', true);
        assert(is_string($platform));

        $connectionEventsQueueDsn = $config['connection_events_queue_dsn'] ?? '';

        if ($connectionEventsQueueDsn !== '') {
            switch ($platform) {
                case self::PLATFORM_AWS:
                    $transportConfig = [
                        'dsn' => $connectionEventsQueueDsn,
                        'serializer' => 'keboola.messenger_bundle.serializer.aws',
                        'options' => [
                            'auto_setup' => false,
                        ],
                    ];
                    break;

                case self::PLATFORM_AZURE:
                    $transportConfig = [
                        'dsn' => $connectionEventsQueueDsn,
                        'serializer' => 'keboola.messenger_bundle.serializer.azure',
                        'options' => [
                            'token_expiry' => 3600,
                            'receive_mode' => 'peek-lock',
                        ],
                    ];
                    break;

                case self::PLATFORM_GCP:
                    $transportConfig = [
                        'dsn' => $connectionEventsQueueDsn,
                        'serializer' => 'keboola.messenger_bundle.serializer.gcp',
                    ];
                    break;

                default:
                    throw new InvalidArgumentException(sprintf('Unknown platform "%s".', $platform));
            };

            $builder->prependExtensionConfig('framework', [
                'messenger' => [
                    'transports' => [
                        'connection_events' => $transportConfig,
                    ],
                ],
            ]);
        }
    }

    public function loadExtension(array $config, ContainerConfigurator $container, ContainerBuilder $builder): void
    {
        $loader = new PhpFileLoader($builder, new FileLocator(__DIR__ . '/../Resources/config'));
        $loader->load('messenger.php');
    }
}
