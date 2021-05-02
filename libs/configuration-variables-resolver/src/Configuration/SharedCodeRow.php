<?php

namespace Keboola\ConfigurationVariablesResolver\Configuration;

use Symfony\Component\Config\Definition\Builder\TreeBuilder;

class SharedCodeRow extends AbstractConfiguration
{
    public function getConfigTreeBuilder(): TreeBuilder
    {
        $treeBuilder = new TreeBuilder('configuration');
        $rootNode = $treeBuilder->getRootNode();
        $rootNode
            ->children()
                ->scalarNode('variables_id')->end()
                ->scalarNode('code_content')->isRequired()->cannotBeEmpty()->end()
            ->end();
        return $treeBuilder;
    }
}
