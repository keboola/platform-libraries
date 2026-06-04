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
                // Legacy flag kept for BC; converted to "load_type" in the validate() callback below and never
                // emitted in the parsed output. No default so it cannot be re-added after being unset.
                ->booleanNode('use_view')->end()
                ->enumNode('load_type')
                    ->values(['COPY', 'CLONE', 'VIEW', 'AUTO'])
                ->end()
                ->booleanNode('keep_internal_timestamp_column')->defaultValue(true)->end()
                ->scalarNode('file_type')->end()
            ->end()
            ->validate()
            ->always(function ($v) {
                if (empty($v['file_type'])) {
                    unset($v['file_type']);
                }
                // Convert the legacy "use_view" flag to the "load_type" attribute. An explicit "load_type"
                // always wins over the legacy flag, so the flag only takes effect when "load_type" is unset.
                // The flag is then dropped so it never leaks downstream of the normalizer.
                if (array_key_exists('use_view', $v)) {
                    if (($v['use_view'] === true) && !isset($v['load_type'])) {
                        $v['load_type'] = 'VIEW';
                    }
                    unset($v['use_view']);
                }
                return $v;
            })
            ->end()
            ->validate()
                ->ifTrue(function ($v) {
                    return empty($v['source']) && empty($v['source_search']);
                })
                ->thenInvalid('Either "source" or "source_search" must be configured.')
            ->end()
            ->validate()
                ->ifTrue(fn($v) =>
                    isset($v['where_column']) &&
                    strlen(trim($v['where_column'])) !== 0 &&
                    count($v['where_values']) === 0)
                ->thenInvalid('When "where_column" is set, "where_values" must be provided.')
            ->end()
            ->validate()
                ->ifTrue(fn($v) =>
                    count($v['where_values']) > 0 &&
                    (!isset($v['where_column']) || strlen(trim($v['where_column'])) === 0))
                ->thenInvalid('When "where_values" is set, non-empty string in "where_column" must be provided.')
            ->end()
        ;
        // BEFORE MODIFICATION OF THIS CONFIGURATION, READ AND UNDERSTAND
        // https://keboola.atlassian.net/wiki/spaces/ENGG/pages/3283910830/Job+configuration+validation
    }
}
