<?php

namespace Keboola\InputMapping\Configuration;

use Keboola\InputMapping\Table\Options\InputTableOptions;
use Symfony\Component\Config\Definition\Builder\NodeDefinition;
use Symfony\Component\Config\Definition\Builder\TreeBuilder;

class File extends Configuration
{
    public function getConfigTreeBuilder()
    {
        $treeBuilder = new TreeBuilder();
        $root = $treeBuilder->root('file');
        self::configureNode($root);
        return $treeBuilder;
    }

    public static function configureNode(NodeDefinition $node)
    {
        $node
            ->children()
                ->arrayNode('tags')
                    ->prototype('scalar')->end()
                ->end()
                ->arrayNode('source')
                    ->children()
                        ->arrayNode('tags')
                            ->prototype('array')
                                ->children()
                                    ->scalarNode('name')
                                        ->isRequired()
                                        ->cannotBeEmpty()
                                    ->end()
                                    ->scalarNode('match')
                                        ->defaultValue('include')
                                        ->validate()
                                            ->ifNotInArray(['include', 'exclude'])
                                            ->thenInvalid('Invalid match type "%s", allowed values are: "include", "exclude".')
                                        ->end()
                                    ->end()
                                ->end()
                            ->end()
                        ->end()
                    ->end()
                ->end()
                ->scalarNode('query')->end()
                ->booleanNode('filter_by_run_id')->end()
                ->integerNode('limit')->end()
                ->arrayNode('processed_tags')
                    ->prototype('scalar')->end()
                ->end()
                ->scalarNode('changed_since')->end()
            ->end()
            ->validate()
                ->always(function ($v) {
                    if (empty($v['tags'])) {
                        unset($v['tags']);
                    }
                    if (empty($v['query'])) {
                        unset($v['query']);
                    }
                    if (empty($v['processed_tags'])) {
                        unset($v['processed_tags']);
                    }
                    return $v;
                })
            ->end()
            ->validate()
            ->ifTrue(function ($v) {
                if ((!isset($v['tags']) || count($v['tags']) == 0) && !isset($v['query']) &&
                    (!isset($v['source']['tags']) || count($v['source']['tags']) == 0)) {
                    return true;
                }
                return false;
            })
                ->thenInvalid('At least one of "tags", "source.tags" or "query" parameters must be defined.')
            ->end()
            ->validate()
            ->ifTrue(function ($v) {
                if (isset($v['tags']) && isset($v['source']['tags'])) {
                    return true;
                }
                return false;
            })
            ->thenInvalid('Both "tags" and "source.tags" cannot be defined.')
            ->end()
            ->validate()
            ->ifTrue(function ($v) {
                if (isset($v['query']) && isset($v['changed_since']) && $v['changed_since'] !== InputTableOptions::ADAPTIVE_INPUT_MAPPING_VALUE) {
                    return true;
                }
                return false;
            })
                ->thenInvalid('Adaptive inputs is not supported for query configurations')
            ->end()
            ->validate()
            ->ifTrue(function ($v) {
                if (isset($v['changed_since'])
                    && $v['changed_since'] !== InputTableOptions::ADAPTIVE_INPUT_MAPPING_VALUE
                    && strtotime($v['changed_since']) === false) {
                    return true;
                }
                return false;
            })
                ->thenInvalid('The value provided for changed_since could not be converted to a timestamp')
            ->end()
        ;
    }
}
