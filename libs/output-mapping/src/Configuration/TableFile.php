<?php

declare(strict_types=1);

namespace Keboola\OutputMapping\Configuration;

use Symfony\Component\Config\Definition\Builder\ArrayNodeDefinition;
use Symfony\Component\Config\Definition\Builder\TreeBuilder;

class TableFile extends Configuration
{
    public function getConfigTreeBuilder(): TreeBuilder
    {
        $treeBuilder = new TreeBuilder('tableFile');
        $root = $treeBuilder->getRootNode();
        self::configureNode($root);
        return $treeBuilder;
    }

    public static function configureNode(ArrayNodeDefinition $node): void
    {
        // BEFORE MODIFICATION OF THIS CONFIGURATION, READ AND UNDERSTAND
        // https://keboola.atlassian.net/wiki/spaces/ENGG/pages/3283910830/Job+configuration+validation
        $node
            ->children()
            ->arrayNode('tags')->prototype('scalar')->end()->end()
            ->booleanNode('is_permanent')->defaultValue(true)->end()
        ;
    }
}
