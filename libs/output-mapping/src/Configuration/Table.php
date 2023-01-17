<?php

declare(strict_types=1);

namespace Keboola\OutputMapping\Configuration;

use Symfony\Component\Config\Definition\Builder\ArrayNodeDefinition;
use Symfony\Component\Config\Definition\Builder\TreeBuilder;

class Table extends Configuration
{
    public function getConfigTreeBuilder(): TreeBuilder
    {
        $treeBuilder = new TreeBuilder('table');
        $root = $treeBuilder->getRootNode();
        self::configureNode($root);
        return $treeBuilder;
    }

    public static function configureNode(ArrayNodeDefinition $node): void
    {
        Table\Manifest::configureNode($node);
        $node->children()->scalarNode('source')->isRequired()->end();
    }
}
