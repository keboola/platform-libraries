<?php

namespace Keboola\InputMapping\Configuration;

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
        $node
            ->children()
                ->scalarNode("source")->isRequired()->end()
                ->scalarNode("destination")->end()
                ->integerNode("days")
                    ->treatNullLike(0)
                ->end()
                ->arrayNode("columns")->prototype("scalar")->end()->end()
                ->scalarNode("where_column")->end()
                ->integerNode("limit")->end()
                ->arrayNode("where_values")->prototype("scalar")->end()->end()
                ->scalarNode("where_operator")
                    ->defaultValue("eq")
                    ->validate()
                    ->ifNotInArray(array("eq", "ne"))
                        ->thenInvalid("Invalid operator in where_operator %s.")
                    ->end()
                ->end()
            ->end()
            ;
    }
}
