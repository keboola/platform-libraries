<?php

namespace Keboola\OutputMapping\Configuration\Table;

use Keboola\OutputMapping\Configuration\Configuration;
use Symfony\Component\Config\Definition\Builder\NodeDefinition;
use Symfony\Component\Config\Definition\Builder\TreeBuilder;

class Manifest extends Configuration
{
    public function getConfigTreeBuilder()
    {
        $treeBuilder = new TreeBuilder('table');
        $root = $treeBuilder->getRootNode();
        self::configureNode($root);
        return $treeBuilder;
    }

    public static function configureNode(NodeDefinition $node)
    {
        $node
            ->children()
                ->scalarNode("destination")->end()
                ->booleanNode("incremental")->defaultValue(false)->end()
                ->arrayNode("primary_key")
                    ->prototype("scalar")
                        // TODO: turn this on when all manifests will not produce array with an empty string
                        // ->cannotBeEmpty()
                    ->end()
                ->end()
                ->arrayNode("columns")
                    ->prototype("scalar")
                ->end()
                ->end()
                ->arrayNode("distribution_key")
                    ->prototype("scalar")->cannotBeEmpty()->end()
                ->end()
                ->scalarNode("delete_where_column")->end()
                ->arrayNode("delete_where_values")->prototype("scalar")->end()->end()
                ->scalarNode("delete_where_operator")
                    ->defaultValue("eq")
                    ->beforeNormalization()
                        ->ifInArray(["", null])
                        ->then(function () {
                            return "eq";
                        })
                    ->end()
                    ->validate()
                    ->ifNotInArray(["eq", "ne"])
                        ->thenInvalid("Invalid operator in delete_where_operator %s.")
                    ->end()
                ->end()
                ->scalarNode("delimiter")->defaultValue(",")->end()
                ->scalarNode("enclosure")->defaultValue("\"")->end()
                ->arrayNode("metadata")
                    ->prototype('array')
                        ->children()
                            ->scalarNode("key")->end()
                            ->scalarNode("value")->end()
                        ->end()
                    ->end()
                ->end()
                ->arrayNode("column_metadata")
                    ->useAttributeAsKey("name")
                    ->prototype('array')
                        ->prototype("array")
                            ->children()
                                ->scalarNode("key")->end()
                                ->scalarNode("value")->end()
                            ->end()
                        ->end()
                    ->end()
                ->end()
                ->booleanNode("write_always")->defaultValue(false)->end()
            ;
    }
}
