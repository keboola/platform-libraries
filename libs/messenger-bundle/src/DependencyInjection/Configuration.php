<?php

declare(strict_types=1);

namespace Keboola\MessengerBundle\DependencyInjection;

use Symfony\Component\Config\Definition\Builder\TreeBuilder;
use Symfony\Component\Config\Definition\ConfigurationInterface;

class Configuration implements ConfigurationInterface
{
    public const PLATFORM_AWS = 'aws';
    public const PLATFORM_AZURE = 'azure';

    public function getConfigTreeBuilder(): TreeBuilder
    {
        $treeBuilder = new TreeBuilder('keboola_messenger');

        $treeBuilder->getRootNode()
            ->children()
                ->enumNode('platform')
                    ->values([self::PLATFORM_AWS, self::PLATFORM_AZURE])
                ->end()

                ->scalarNode('connection_events_queue_dsn')
                    ->defaultValue('%env(CONNECTION_EVENTS_QUEUE_DSN)%')
                ->end()

                ->scalarNode('connection_events_queue_name')
                    ->defaultValue('%env(CONNECTION_EVENTS_QUEUE_NAME)%')
                ->end()
            ->end()

            ->validate()
                ->ifTrue(fn(array $config): bool =>
                    $config['platform'] === 'azure' &&
                    empty($config['connection_events_queue_name']))
                ->thenInvalid('Platform "azure" requires "connection_events_queue_name" to be set.')
            ->end()
        ;

        return $treeBuilder;
    }
}
