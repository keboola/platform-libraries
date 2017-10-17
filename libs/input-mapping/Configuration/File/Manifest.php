<?php

namespace Keboola\InputMapping\Configuration\File;

use Keboola\InputMapping\Configuration\Configuration;
use Symfony\Component\Config\Definition\Builder\NodeDefinition;
use Symfony\Component\Config\Definition\Builder\TreeBuilder;

class Manifest extends Configuration
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
                ->integerNode("id")->isRequired()->end()
                ->scalarNode("name")->end()
                ->scalarNode("created")->end()
                ->booleanNode("is_public")->defaultValue(false)->end()
                ->booleanNode("is_encrypted")->defaultValue(false)->end()
                ->booleanNode("is_sliced")->defaultValue(false)->end()
                ->arrayNode("tags")->prototype("scalar")->end()->end()
                ->integerNode("max_age_days")->treatNullLike(0)->end()
                ->integerNode("size_bytes")->end()
            ;
    }
}
