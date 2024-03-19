<?php

declare(strict_types=1);

namespace Keboola\OutputMapping\Configuration\Table;

use Keboola\OutputMapping\Configuration\Configuration;
use Symfony\Component\Config\Definition\Builder\ArrayNodeDefinition;
use Symfony\Component\Config\Definition\Builder\TreeBuilder;

class Manifest extends Configuration
{
    public const DEFAULT_DELIMITER = ',';
    public const DEFAULT_ENCLOSURE = '"';

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
                ->arrayNode('columns')
                    ->prototype('scalar')
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
                ->scalarNode('delimiter')->defaultValue(self::DEFAULT_DELIMITER)->end()
                ->scalarNode('enclosure')->defaultValue(self::DEFAULT_ENCLOSURE)->end()
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
            ->end()
            ->validate()
                ->ifTrue(fn($values) =>
                    isset($values['delete_where_column']) && $values['delete_where_column'] !== '' &&
                    isset($values['delete_where_values']) && count($values['delete_where_values']) === 0)
                ->thenInvalid('When "delete_where_column" option is set, then the "delete_where_values" is required.')
            ->end()
            ;
    }
}
