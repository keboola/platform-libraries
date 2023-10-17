<?php

declare(strict_types=1);

namespace Keboola\MessengerBundle\DependencyInjection;

use Keboola\MessengerBundle\Platform;
use Symfony\Component\Config\Definition\Configurator\DefinitionConfigurator;
use Symfony\Component\Config\FileLocator;
use Symfony\Component\DependencyInjection\ChildDefinition;
use Symfony\Component\DependencyInjection\ContainerBuilder;
use Symfony\Component\DependencyInjection\Extension\AbstractExtension;
use Symfony\Component\DependencyInjection\Loader\Configurator\ContainerConfigurator;
use Symfony\Component\DependencyInjection\Loader\PhpFileLoader;
use Symfony\Component\DependencyInjection\Reference;

class KeboolaMessengerExtension extends AbstractExtension
{
    public function configure(DefinitionConfigurator $definition): void
    {
        $definition->rootNode() // @phpstan-ignore-line - root node is always ArrayNode
            ->children()
                ->enumNode('platform')
                    ->defaultNull()
                    ->beforeNormalization()
                        ->ifEmpty()->thenUnset()
                    ->end()
                    ->values([
                        ...array_map(fn(Platform $v) => $v->value, Platform::cases()),
                        null,
                    ])
                ->end()

                ->scalarNode('connection_events_queue_dsn')
                    ->cannotBeEmpty()
                ->end()

                ->scalarNode('connection_audit_log_queue_dsn')
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
        $configs = $builder->resolveEnvPlaceholders($configs, true);
        assert(is_array($configs));
        $config = $this->processConfiguration($configuration, $configs);

        if (!isset($config['platform'])) {
            return;
        }
        $platform = Platform::from($config['platform']);

        $this->createTransportConfig(
            $builder,
            $platform,
            $config,
            'connection_events',
            'connection_events_queue_dsn',
            'keboola.messenger_bundle.event_factory.application_events',
        );

        $this->createTransportConfig(
            $builder,
            $platform,
            $config,
            'connection_audit_log',
            'connection_audit_log_queue_dsn',
            'keboola.messenger_bundle.event_factory.audit_log',
        );
    }

    public function loadExtension(array $config, ContainerConfigurator $container, ContainerBuilder $builder): void
    {
        $loader = new PhpFileLoader($builder, new FileLocator(__DIR__ . '/../Resources/config'));
        $loader->load('messenger.php');
    }

    private function createTransportConfig(
        ContainerBuilder $builder,
        Platform $platform,
        array $config,
        string $transportName,
        string $dsnProperty,
        string $eventFactoryService,
    ): void {
        $transportDsn = $config[$dsnProperty] ?? '';
        if ($transportDsn === '') {
            return;
        }

        $serializerServiceName = sprintf('keboola.messenger_bundle.transport_serializer.%s', $transportName);
        $serializerServiceDefinition =
            (new ChildDefinition(sprintf('keboola.messenger_bundle.platform_serializer.%s', $platform->value)))
            ->setArgument('$eventFactory', new Reference($eventFactoryService))
        ;

        $builder->setDefinition($serializerServiceName, $serializerServiceDefinition);
        $builder->prependExtensionConfig('framework', [
            'messenger' => [
                'transports' => [
                    $transportName => [
                        'dsn' => $transportDsn,
                        'serializer' => $serializerServiceName,
                        'options' => $this->getTransportDefaultOptions($platform),
                    ],
                ],
            ],
        ]);
    }

    private function getTransportDefaultOptions(Platform $platform): array
    {
        return match ($platform) {
            Platform::AWS => [
                'auto_setup' => false,
            ],

            Platform::AZURE => [
                'token_expiry' => 3600,
                'receive_mode' => 'peek-lock',
            ],

            Platform::GCP => [],
        };
    }
}
