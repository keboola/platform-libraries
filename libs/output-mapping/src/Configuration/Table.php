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
        $node
            ->children()
            ->scalarNode('destination')->end()
            ->booleanNode('incremental')->defaultValue(false)->end()
            ->arrayNode('primary_key')
            ->prototype('scalar')
            // TODO: turn this on when all manifests will not produce array with an empty string
            // ->cannotBeEmpty()
            ->end()
            ->end()


            ->end()
            ->end()
            ->arrayNode('distribution_key')
            ->prototype('scalar')->cannotBeEmpty()->end()
            ->end()
            ->scalarNode('delete_where_column')->end()
            ->arrayNode('delete_where_values')->prototype('scalar')->end()->end()
            ->scalarNode('delete_where_operator')
            ->defaultValue('eq')
            ->beforeNormalization()
            ->ifInArray(['', null])
            ->then(function () {
                return 'eq';
            })
            ->end()
            ->validate()
            ->ifNotInArray(['eq', 'ne'])
            ->thenInvalid('Invalid operator in delete_where_operator %s.')
            ->end()
            ->end()

            ->arrayNode('metadata')
            ->prototype('array')
            ->children()
            ->scalarNode('key')->end()
            ->scalarNode('value')->end()
            ->end()
            ->end()
            ->end()
            ->arrayNode('column_metadata')
            ->useAttributeAsKey('name')
            ->prototype('array')
            ->prototype('array')
            ->children()
            ->scalarNode('key')->end()
            ->scalarNode('value')->end()
            ->end()
            ->end()
            ->end()
            ->end()
            ->booleanNode('write_always')->defaultValue(false)->end()
        ;
    }
        $node->children()->scalarNode('source')->isRequired()->end();
    }
}
