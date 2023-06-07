<?php

declare(strict_types=1);

namespace Keboola\ApiBundle\DependencyInjection;

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
                    ->defaultValue('%app_name%')
                ->end()
                ->scalarNode('storage_api_url')
                    ->cannotBeEmpty()
                    ->defaultValue('%env(STORAGE_API_URL)%')
                ->end()
            ->end()
        ;

        return $treeBuilder;
    }
}
