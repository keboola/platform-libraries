<?php

declare(strict_types=1);

namespace Keboola\ApiBundle\DependencyInjection;

use Keboola\ServiceClient\ServiceDnsType;
use Symfony\Component\Config\Definition\Builder\TreeBuilder;
use Symfony\Component\Config\Definition\ConfigurationInterface;

class Configuration implements ConfigurationInterface
{
    public function getConfigTreeBuilder(): TreeBuilder
    {
        $treeBuilder = new TreeBuilder('keboola_api');

        $treeBuilder->getRootNode()
            ->children()
                ->scalarNode('app_name')
                    ->cannotBeEmpty()
                    ->defaultValue('%app_name%')
                ->end()
                ->enumNode('default_service_dns_type')
                    ->cannotBeEmpty()
                    ->values(array_map(fn(ServiceDnsType $v) => $v->value, ServiceDnsType::cases()))
                    ->defaultValue(ServiceDnsType::PUBLIC->value)
                ->end()
                ->arrayNode('storage_client_options')
                    ->info(
                        'Extra ClientOptions for the Storage API client factory: '
                        . 'a service id (string) OR individual options.',
                    )
                    ->beforeNormalization()
                        ->ifString()
                        ->then(fn(string $v) => ['service' => $v])
                    ->end()
                    ->children()
                        ->scalarNode('service')
                            ->cannotBeEmpty()
                            ->info('Service id of a ClientOptions instance merged onto the base options.')
                        ->end()
                        ->integerNode('backoff_max_tries')->end()
                        ->integerNode('aws_retries')->end()
                        ->booleanNode('aws_debug')->end()
                        ->booleanNode('retry_on_maintenance')->end()
                        ->booleanNode('use_branch_storage')->end()
                        ->scalarNode('user_agent')
                            ->cannotBeEmpty()
                            ->info('Override the Storage client user agent (defaults to app_name).')
                        ->end()
                        ->scalarNode('logger')
                            ->cannotBeEmpty()
                            ->info('Service id of a PSR LoggerInterface to use instead of the default @logger.')
                        ->end()
                    ->end()
                    ->validate()
                        ->ifTrue(fn(array $v) => isset($v['service']) && count($v) > 1)
                        ->thenInvalid(
                            'storage_client_options: use either a service reference (string) '
                            . 'or individual options, not both.',
                        )
                    ->end()
                ->end()
            ->end()
        ;

        return $treeBuilder;
    }
}
