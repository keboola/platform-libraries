<?php

namespace Keboola\InputMapping\Configuration;

use Symfony\Component\Config\Definition\Builder\ArrayNodeDefinition;
use Symfony\Component\Config\Definition\Builder\NodeDefinition;
use Symfony\Component\Config\Definition\Builder\TreeBuilder;

class Table extends Configuration
{
    public function getConfigTreeBuilder()
    {
        $treeBuilder = new TreeBuilder();
        $root = $treeBuilder->root("table");
        self::configureNode($root);
        return $treeBuilder;
    }

    public static function configureNode(NodeDefinition $node)
    {
        /** @var ArrayNodeDefinition $node */
        $node
            ->children()
                ->scalarNode("source")->cannotBeEmpty()->end()
                ->arrayNode("source_search")
                    ->children()
                        ->scalarNode('key')->isRequired()->cannotBeEmpty()->end()
                        ->scalarNode('value')->isRequired()->cannotBeEmpty()->end()
                    ->end()
                ->end()
                ->scalarNode("destination")->end()
                ->integerNode("days")
                    ->treatNullLike(0)
                ->end()
                ->scalarNode("changed_since")
                    ->treatNullLike("")
                ->end()
                ->arrayNode('columns')
                    ->prototype('scalar')->end()
                ->end()
                ->arrayNode('column_types')
                    ->prototype('array')
                        ->children()
                            ->scalarNode('source')->isRequired()->end()
                            ->scalarNode('type')->end()
                            ->scalarNode('destination')->end()
                            ->scalarNode('type')->end()
                            ->scalarNode('length')->end()
                            ->scalarNode('nullable')->end()
                            ->scalarNode('convert_empty_values_to_null')->end()
                            ->scalarNode('compression')->end()
                        ->end()
                    ->end()
                ->end()
                ->scalarNode("where_column")->end()
                ->integerNode("limit")->end()
                ->arrayNode("where_values")->prototype("scalar")->end()->end()
                ->scalarNode("where_operator")
                    ->defaultValue("eq")
                    ->beforeNormalization()
                        ->ifInArray(["", null])
                        ->then(function () {
                            return "eq";
                        })
                    ->end()
                    ->validate()
                        ->ifNotInArray(["eq", "ne"])
                        ->thenInvalid("Invalid operator in where_operator %s.")
                    ->end()
                ->end()
            ->end()
            ->validate()
            ->ifTrue(function ($v) {
                return empty($v['source']) && empty($v['source_search']);
            })
            ->thenInvalid('Either "source" or "source_search" must be configured.');
            ;
    }
}
