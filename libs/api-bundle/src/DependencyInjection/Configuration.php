<?php

declare(strict_types=1);

namespace Keboola\ApiBundle\DependencyInjection;

use Keboola\ApiBundle\ServiceClient\ServiceDnsType;
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
            ->end()
        ;

        return $treeBuilder;
    }
}
