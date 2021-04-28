<?php

namespace Keboola\OutputMapping\Configuration;

use Symfony\Component\Config\Definition\Builder\NodeDefinition;
use Symfony\Component\Config\Definition\Builder\TreeBuilder;

class TableFile extends Configuration
{
    public function getConfigTreeBuilder()
    {
        $treeBuilder = new TreeBuilder();
        $root = $treeBuilder->root("tableFile");
        self::configureNode($root);
        return $treeBuilder;
    }

    public static function configureNode(NodeDefinition $node)
    {
        $node
            ->children()
            ->arrayNode("tags")->prototype("scalar")->end()->end()
            ->booleanNode("is_permanent")->defaultValue(true)->end()
        ;
    }
}

