<?php

namespace Keboola\InputMapping\Configuration;

use Symfony\Component\Config\Definition\Builder\NodeDefinition;
use Symfony\Component\Config\Definition\Builder\TreeBuilder;

class File extends Configuration
{
    public function getConfigTreeBuilder()
    {
        $treeBuilder = new TreeBuilder();
        $root = $treeBuilder->root("file");
        self::configureNode($root);
        return $treeBuilder;
    }

    public static function configureNode(NodeDefinition $node)
    {
        $node
            ->children()
                ->arrayNode("tags")
                    ->prototype("scalar")->end()
                ->end()
                ->scalarNode("query")->end()
                ->booleanNode("filter_by_run_id")->end()
                ->integerNode("limit")->defaultValue(10)->end()
                ->arrayNode("processed_tags")
                    ->prototype("scalar")->end()
                ->end()
            ->end()
        ->validate()
            ->ifTrue(function ($v) {
                if ((!isset($v["tags"]) || count($v["tags"]) == 0) && !isset($v["query"])) {
                    return true;
                }
                return false;
            })
                ->thenInvalid("At least one of 'tags' or 'query' parameters must be defined.");
    }
}
