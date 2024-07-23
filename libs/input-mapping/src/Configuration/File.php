<?php

declare(strict_types=1);

namespace Keboola\InputMapping\Configuration;

use Keboola\InputMapping\Table\Options\InputTableOptions;
use Symfony\Component\Config\Definition\Builder\ArrayNodeDefinition;
use Symfony\Component\Config\Definition\Builder\TreeBuilder;

class File extends Configuration
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
                ->arrayNode('file_ids')
                    ->prototype('scalar')->end()
                ->end()
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
                                            // phpcs:ignore Generic.Files.LineLength
                                            ->thenInvalid('Invalid match type "%s", allowed values are: "include", "exclude".')
                                        ->end()
                                    ->end()
                                ->end()
                            ->end()
                        ->end()
                    ->end()
                ->end()
                ->scalarNode('query')->end()
                ->integerNode('limit')->end()
                ->booleanNode('overwrite')->defaultValue(true)->end()
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
                    if (empty($v['file_ids'])) {
                        unset($v['file_ids']);
                    }
                    return $v;
                })
            ->end()
            ->validate()
            ->ifTrue(function ($v) {
                $hasTags = isset($v['tags']) && count($v['tags']) > 0;
                $hasQuery = isset($v['query']);
                $hasSourceTags = isset($v['source']['tags']) && count($v['source']['tags']) > 0;
                $hasFileIds = isset($v['file_ids']) && count($v['file_ids']) > 0;
                if (!($hasTags || $hasQuery || $hasSourceTags || $hasFileIds)) {
                    return true;
                }
                return false;
            })
                ->thenInvalid(
                    'At least one of "tags", "source.tags", "query" or "file_ids" parameters must be defined.',
                )
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
                if (isset($v['query']) && isset($v['changed_since'])) {
                    return true;
                }
                return false;
            })
                ->thenInvalid('The changed_since parameter is not supported for query configurations')
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
            ->validate()
            ->ifTrue(function ($v) {
                unset($v['processed_tags']);
                unset($v['overwrite']);
                $items = array_keys($v);
                if (in_array('file_ids', $items, true) && count($v) > 1) {
                    return true;
                }
                return false;
            })
                ->thenInvalid('The file_ids filter can be combined only with overwrite flag and processed_tags')
            ->end()
        ;
        // BEFORE MODIFICATION OF THIS CONFIGURATION, READ AND UNDERSTAND
        // https://keboola.atlassian.net/wiki/spaces/ENGG/pages/3283910830/Job+configuration+validation
    }
}
