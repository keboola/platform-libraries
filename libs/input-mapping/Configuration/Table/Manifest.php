<?php

namespace Keboola\InputMapping\Configuration\Table;

use Keboola\InputMapping\Configuration\Configuration;
use Symfony\Component\Config\Definition\Builder\NodeDefinition;
use Symfony\Component\Config\Definition\Builder\TreeBuilder;

class Manifest extends Configuration
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
                ->scalarNode("id")->isRequired()->end()
                ->scalarNode("name")->end()
                ->scalarNode("uri")->end()
                ->arrayNode("primary_key")->prototype("scalar")->end()->end()
                ->arrayNode("indexed_columns")->prototype("scalar")->end()->end()
                ->scalarNode("created")->end()
                ->scalarNode("last_import_date")->end()
                ->scalarNode("last_change_date")->end()
                ->integerNode("rows_count")->treatNullLike(0)->end()
                ->integerNode("data_size_bytes")->treatNullLike(0)->end()
                ->booleanNode("is_alias")->end()
                ->arrayNode("columns")->prototype("scalar")->end()->end()
                ->arrayNode("attributes")->prototype("array")->children()
                    ->scalarNode("name")->end()
                    ->scalarNode("value")->end()
                    ->booleanNode("protected")->end()
                ->end()->end()->end()
            ;
    }
}
