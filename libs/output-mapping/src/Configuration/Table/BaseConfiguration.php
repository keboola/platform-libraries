<?php

declare(strict_types=1);

namespace Keboola\OutputMapping\Configuration\Table;

use Keboola\OutputMapping\Configuration\Configuration;
use Symfony\Component\Config\Definition\Builder\ArrayNodeDefinition;
use Symfony\Component\Config\Definition\Exception\InvalidConfigurationException;

abstract class BaseConfiguration extends Configuration
{
    public const DEFAULT_DELIMITER = ',';
    public const DEFAULT_ENCLOSURE = '"';

    private const ALLOWED_DATA_TYPES_BACKEND = [
        'base',
        'redshift',
        'snowflake',
        'synapse',
        'bigquery',
        'exasol',
    ];

    public static function configureNode(ArrayNodeDefinition $node): void
    {
        // BEFORE MODIFICATION OF THIS CONFIGURATION, READ AND UNDERSTAND
        // https://keboola.atlassian.net/wiki/spaces/ENGG/pages/3283910830/Job+configuration+validation
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
                ->arrayNode('tags')->prototype('scalar')->cannotBeEmpty()->end()->end()
                ->scalarNode('manifest_type')->end()
                ->booleanNode('has_header')->end()
                ->scalarNode('description')->end()
                ->variableNode('table_metadata')->end()
                ->arrayNode('schema')
                    ->prototype('array')
                        ->children()
                            ->scalarNode('name')->isRequired()->cannotBeEmpty()->end()
                            ->arrayNode('data_type')->isRequired()
                                ->ignoreExtraKeys(false)
                                ->children()
                                    ->arrayNode('base')->isRequired()
                                        ->children()
                                            ->scalarNode('type')->isRequired()->cannotBeEmpty()->end()
                                            ->scalarNode('length')->end()
                                            ->scalarNode('default')->end()
                                        ->end()
                                    ->end()
                                ->end()
                                ->validate()->always(function ($v) {
                                    foreach ($v as $item => $itemValues) {
                                        if (!in_array($item, self::ALLOWED_DATA_TYPES_BACKEND, true)) {
                                            throw new InvalidConfigurationException(sprintf(
                                                'The "%s" data type is not supported.',
                                                $item,
                                            ));
                                        }
                                        if (!isset($itemValues['type'])) {
                                            throw new InvalidConfigurationException(sprintf(
                                                'The "type" is required for the "%s" data type.',
                                                $item,
                                            ));
                                        }
                                    }
                                    return $v;
                                })->end()
                            ->end()
                            ->booleanNode('nullable')->defaultTrue()->end()
                            ->booleanNode('primary_key')->defaultFalse()->end()
                            ->scalarNode('description')->end()
                            ->arrayNode('metadata')
                                ->ignoreExtraKeys(false)
                            ->end()
                        ->end()
                        ->validate()
                            ->ifTrue(fn($values) => isset($values['description']) &&
                                isset($values['metadata']['KBC.description']))
                            ->thenInvalid('Only one of "description" or "metadata.KBC.description" can be defined.')
                        ->end()
                    ->end()
                ->end()
            ->end()
            ->validate()
                ->ifTrue(fn($values) =>
                    isset($values['delete_where_column']) && $values['delete_where_column'] !== '' &&
                    isset($values['delete_where_values']) && count($values['delete_where_values']) === 0)
                ->thenInvalid('When "delete_where_column" option is set, then the "delete_where_values" is required.')
            ->end()
            ->validate()
                ->ifTrue(fn($values) => !empty($values['schema']) && !empty($values['metadata']))
                ->thenInvalid('Only one of "schema" or "metadata" can be defined.')
            ->end()
            ->validate()
                ->ifTrue(fn($values) => !empty($values['schema']) && !empty($values['column_metadata']))
                ->thenInvalid('Only one of "schema" or "column_metadata" can be defined.')
            ->end()
            ->validate()
                ->ifTrue(
                    fn($values) => isset($values['description']) && isset($values['table_metadata']['KBC.description']),
                )
                ->thenInvalid('Only one of "description" or "table_metadata.KBC.description" can be defined.')
            ->end()
            ->validate()->always(function ($v) {
                if (!empty($v['schema']) && !empty($v['primary_key'])) {
                    throw new InvalidConfigurationException(
                        'Only one of "primary_key" or "schema[].primary_key" can be defined.',
                    );
                }
                return $v;
            })->end()
            ;
        // BEFORE MODIFICATION OF THIS CONFIGURATION, READ AND UNDERSTAND
        // https://keboola.atlassian.net/wiki/spaces/ENGG/pages/3283910830/Job+configuration+validation
    }
}
