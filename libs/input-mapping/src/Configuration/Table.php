<?php

declare(strict_types=1);

namespace Keboola\InputMapping\Configuration;

use Symfony\Component\Config\Definition\Builder\ArrayNodeDefinition;
use Symfony\Component\Config\Definition\Builder\NodeDefinition;
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

    public static function configureNode(NodeDefinition $node): void
    {
        // BEFORE MODIFICATION OF THIS CONFIGURATION, READ AND UNDERSTAND
        // https://keboola.atlassian.net/wiki/spaces/ENGG/pages/3283910830/Job+configuration+validation
        /** @var ArrayNodeDefinition $node */
        $node
            ->children()
                ->scalarNode('source')->cannotBeEmpty()->end()
                ->scalarNode('source_branch_id')->end()
                ->arrayNode('source_search')
                    ->children()
                        ->scalarNode('key')->isRequired()->cannotBeEmpty()->end()
                        ->scalarNode('value')->isRequired()->cannotBeEmpty()->end()
                    ->end()
                ->end()
                ->scalarNode('destination')->end()
                ->integerNode('days')
                    ->treatNullLike(0)
                ->end()
                ->scalarNode('changed_since')
                    ->treatNullLike('')
                ->end()
                ->arrayNode('columns')
                    ->prototype('scalar')->end()
                ->end()
                ->arrayNode('column_types')
                    ->prototype('array')
                        ->children()
                            ->scalarNode('source')->isRequired()->end()
                            ->scalarNode('type')->end()
                            ->scalarNode('destination')->end()
                            ->scalarNode('length')->end()
                            ->scalarNode('nullable')->end()
                            ->scalarNode('convert_empty_values_to_null')->end()
                            ->scalarNode('compression')->end()
                        ->end()
                    ->end()
                ->end()
                ->scalarNode('where_column')->end()
                ->integerNode('limit')->end()
                ->arrayNode('where_values')->prototype('scalar')->end()->end()
                ->scalarNode('where_operator')
                    ->defaultValue('eq')
                    ->beforeNormalization()
                        ->ifInArray(['', null])
                        ->then(function () {
                            return 'eq';
                        })
                    ->end()
                    ->validate()
                        ->ifNotInArray(['eq', 'ne'])
                        ->thenInvalid('Invalid operator in where_operator %s.')
                    ->end()
                ->end()
                ->booleanNode('overwrite')->defaultValue(false)->end()
                ->booleanNode('use_view')->defaultValue(false)->end()
                ->booleanNode('keep_internal_timestamp_column')->defaultValue(true)->end()
            ->end()
            ->validate()
            ->ifTrue(function ($v) {
                return empty($v['source']) && empty($v['source_search']);
            })
            ->thenInvalid('Either "source" or "source_search" must be configured.');
        // BEFORE MODIFICATION OF THIS CONFIGURATION, READ AND UNDERSTAND
        // https://keboola.atlassian.net/wiki/spaces/ENGG/pages/3283910830/Job+configuration+validation
    }
}
