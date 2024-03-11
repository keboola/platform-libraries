<?php

declare(strict_types=1);

namespace Keboola\OutputMapping\Configuration\Table;

use Symfony\Component\Config\Definition\Builder\ArrayNodeDefinition;
use Symfony\Component\Config\Definition\Builder\TreeBuilder;

class Configuration extends Manifest
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
        parent::configureNode($node);
        $node->children()->scalarNode('source')->end()->end();
    }
}
