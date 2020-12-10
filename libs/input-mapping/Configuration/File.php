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
                ->arrayNode("source")
                    ->children()
                        ->arrayNode("tags")
                            ->prototype("array")
                                ->children()
                                    ->scalarNode("name")
                                        ->isRequired()
                                        ->cannotBeEmpty()
                                    ->end()
                                ->end()
                            ->end()
                        ->end()
                    ->end()
                ->end()
                ->scalarNode("query")->end()
                ->booleanNode("filter_by_run_id")->end()
                ->integerNode("limit")->end()
                ->arrayNode("processed_tags")
                    ->prototype("scalar")->end()
                ->end()
            ->end()
        ->validate()
            ->ifTrue(function ($v) {
                if ((!isset($v["tags"]) || count($v["tags"]) == 0) && !isset($v["query"]) && (!isset($v["source"]["tags"]) || count($v["source"]["tags"]) == 0)) {
                    return true;
                }
                return false;
            })
                ->thenInvalid("At least one of 'tags', 'source.tags' or 'query' parameters must be defined.")
            ->end()
            ->validate()
            ->ifTrue(function ($v) {
                if (isset($v["tags"]) && isset($v["source"]["tags"])) {
                    return true;
                }
                return false;
            })
            ->thenInvalid("Both 'tags' and 'source.tags' cannot be defined.")
            ->end()
        ;
    }
}
