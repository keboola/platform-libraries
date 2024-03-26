<?php

declare(strict_types=1);

namespace Keboola\InputMapping\Configuration\File;

use Keboola\InputMapping\Configuration\Configuration;
use Symfony\Component\Config\Definition\Builder\ArrayNodeDefinition;
use Symfony\Component\Config\Definition\Builder\TreeBuilder;

class Manifest extends Configuration
{
    public function getConfigTreeBuilder(): TreeBuilder
    {
        $treeBuilder = new TreeBuilder('file');
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
                ->integerNode('id')->isRequired()->end()
                ->scalarNode('name')->end()
                ->scalarNode('created')->end()
                ->booleanNode('is_public')->defaultValue(false)->end()
                ->booleanNode('is_encrypted')->defaultValue(false)->end()
                ->booleanNode('is_sliced')->defaultValue(false)->end()
                ->arrayNode('tags')->prototype('scalar')->end()->end()
                ->integerNode('max_age_days')->treatNullLike(0)->end()
                ->integerNode('size_bytes')->end()
            ;
        // BEFORE MODIFICATION OF THIS CONFIGURATION, READ AND UNDERSTAND
        // https://keboola.atlassian.net/wiki/spaces/ENGG/pages/3283910830/Job+configuration+validation
    }
}
