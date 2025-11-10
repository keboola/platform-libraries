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
        $node->children()->scalarNode('source')->end();
        $node->validate()
            ->ifTrue(function ($values) {
                $isDirectGrant = isset($values['unload_strategy']) &&
                    $values['unload_strategy'] === 'direct-grant';
                return !$isDirectGrant && !isset($values['source']);
            })
            ->thenInvalid('The child config "source" under "table" must be configured.')
            ->end();
    }
}
