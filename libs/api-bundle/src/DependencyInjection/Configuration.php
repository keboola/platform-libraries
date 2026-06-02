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
                ->arrayNode('storage_token_exchange')
                    ->addDefaultsIfNotSet()
                    ->info('Exchange of Connection programmatic tokens for legacy Storage tokens.')
                    ->children()
                        ->booleanNode('enabled')
                            ->info('When true, #[StorageApiTokenAuth] also accepts programmatic tokens.')
                            ->defaultFalse()
                        ->end()
                        ->scalarNode('service_account_token_path')
                            ->cannotBeEmpty()
                            ->defaultValue('/var/run/secrets/connection.keboola.com/serviceaccount/token')
                        ->end()
                        ->scalarNode('project_id_header')
                            ->cannotBeEmpty()
                            ->defaultValue('X-KBC-ProjectId')
                        ->end()
                        ->enumNode('connection_dns_type')
                            ->cannotBeEmpty()
                            ->values(array_map(fn(ServiceDnsType $v) => $v->value, ServiceDnsType::cases()))
                            ->defaultValue(ServiceDnsType::INTERNAL->value)
                        ->end()
                    ->end()
                ->end()
            ->end()
        ;

        return $treeBuilder;
    }
}
