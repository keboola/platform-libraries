<?php

declare(strict_types=1);

namespace Keboola\OutputMapping\Configuration\Table;

use Closure;
use Keboola\OutputMapping\Configuration\Configuration;
use Symfony\Component\Config\Definition\Builder\ArrayNodeDefinition;
use Symfony\Component\Config\Definition\Exception\InvalidConfigurationException;

abstract class BaseConfiguration extends Configuration
{
    public const DEFAULT_DELIMITER = ',';
    public const DEFAULT_ENCLOSURE = '"';
    public const FIELD_DEDUPLICATION_STRATEGY = 'deduplication_strategy';

    private const DEFAULT_DELETE_WHERE_OPERATOR = 'eq';

    private const ALLOWED_DATA_TYPES_BACKEND = [
        'base',
        'snowflake',
        'bigquery',
    ];

    private const ALLOWED_DELETE_WHERE_OPERATORS = ['eq', 'ne'];

    public static function configureNode(ArrayNodeDefinition $node): void
    {
        // BEFORE MODIFICATION OF THIS CONFIGURATION, READ AND UNDERSTAND
        // https://keboola.atlassian.net/wiki/spaces/ENGG/pages/3283910830/Job+configuration+validation
        $node
            ->children()
                ->scalarNode('destination')->end()
                ->booleanNode('incremental')->defaultValue(false)->end()
                ->enumNode('unload_strategy')
                    ->values(['direct-grant'])
                ->end()
                ->enumNode(self::FIELD_DEDUPLICATION_STRATEGY)
                    ->values([
                        DeduplicationStrategy::INSERT->value,
                        DeduplicationStrategy::UPSERT->value,
                    ])
                ->end()
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
                    ->defaultValue(self::DEFAULT_DELETE_WHERE_OPERATOR)
                    ->beforeNormalization()
                        ->ifInArray(['', null])
                        ->then(function () {
                            return self::DEFAULT_DELETE_WHERE_OPERATOR;
                        })
                    ->end()
                    ->validate()
                    ->ifNotInArray(self::ALLOWED_DELETE_WHERE_OPERATORS)
                        ->thenInvalid('Invalid operator in delete_where_operator %s.')
                    ->end()
                ->end()
                ->arrayNode('delete_where')
                    ->prototype('array')
                        ->children()
                            ->scalarNode('changed_since')->end()
                            ->scalarNode('changed_until')->end()
                            ->arrayNode('where_filters')
                                ->prototype('array')
                                    ->children()
                                        ->scalarNode('column')->isRequired()->cannotBeEmpty()->end()
                                        ->scalarNode('operator')
                                            ->defaultValue(self::DEFAULT_DELETE_WHERE_OPERATOR)
                                            ->validate()
                                                ->ifNotInArray(self::ALLOWED_DELETE_WHERE_OPERATORS)
                                                ->thenInvalid(
                                                    'Invalid operator %s. Valid values are "' .
                                                        implode('" or "', self::ALLOWED_DELETE_WHERE_OPERATORS) . '".',
                                                )
                                            ->end()
                                        ->end()
                                        ->arrayNode('values_from_set')
                                            ->prototype('scalar')->end()
                                        ->end()
                                        ->arrayNode('values_from_workspace')
                                            ->children()
                                                ->scalarNode('workspace_id')->end()
                                                ->scalarNode('table')->isRequired()->cannotBeEmpty()->end()
                                                ->scalarNode('column')->end()
                                            ->end()
                                        ->end()
                                        ->arrayNode('values_from_storage')
                                            ->children()
                                                ->scalarNode('bucket_id')->isRequired()->cannotBeEmpty()->end()
                                                ->scalarNode('table')->isRequired()->cannotBeEmpty()->end()
                                                ->scalarNode('column')->end()
                                            ->end()
                                        ->end()
                                    ->end()
                                    ->validate()
                                        ->always(function ($v) {
                                            if (empty($v['values_from_set'])) {
                                                unset($v['values_from_set']);
                                            }
                                            return $v;
                                        })
                                    ->end()
                                    ->validate()
                                        ->ifTrue(function ($v) {
                                            $valuesSources = array_filter([
                                                !empty($v['values_from_set']),
                                                !empty($v['values_from_workspace']),
                                                !empty($v['values_from_storage']),
                                            ]);
                                            return count($valuesSources) > 1;
                                        })
                                        ->thenInvalid(
                                            'Only one of "values_from_set", "values_from_workspace", ' .
                                            'or "values_from_storage" can be defined.',
                                        )
                                    ->end()
                                    ->validate()
                                        ->ifTrue(function ($v) {
                                            $valuesSources = array_filter([
                                                !empty($v['values_from_set']),
                                                !empty($v['values_from_workspace']),
                                                !empty($v['values_from_storage']),
                                            ]);
                                            return count($valuesSources) === 0;
                                        })
                                        ->thenInvalid(
                                            'One of "values_from_set", "values_from_workspace", ' .
                                            'or "values_from_storage" must be defined.',
                                        )
                                    ->end()
                                ->end()
                            ->end()
                        ->end()
                        ->validate()
                            ->always(function ($v) {
                                if (empty($v['where_filters'])) {
                                    unset($v['where_filters']);
                                }
                                return $v;
                            })
                        ->end()
                        ->validate()
                            ->ifTrue(function ($v) {
                                return empty($v['changed_since']) && empty($v['changed_until']) &&
                                    empty($v['where_filters']);
                            })
                            ->thenInvalid(
                                'At least one of "changed_since", "changed_until", or "where_filters" ' .
                                'must be defined.',
                            )
                        ->end()
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
                    ->normalizeKeys(false)
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
                            ->arrayNode('data_type')
                                ->ignoreExtraKeys(false)
                                ->children()
                                    ->arrayNode('base')->isRequired()
                                        ->children()
                                            ->scalarNode('type')->isRequired()->cannotBeEmpty()->end()
                                            ->scalarNode('length')
                                                ->beforeNormalization()->always(self::getStringNormalizer())->end()
                                            ->end()
                                            ->scalarNode('default')
                                                ->beforeNormalization()->always(self::getStringNormalizer())->end()
                                            ->end()
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
                                        if (isset($itemValues['default'])) {
                                            $v[$item]['default'] = self::getStringNormalizer()($itemValues['default']);
                                        }
                                        if (isset($itemValues['length'])) {
                                            $v[$item]['length'] = self::getStringNormalizer()($itemValues['length']);
                                        }
                                    }
                                    return $v;
                                })->end()
                            ->end()
                            ->booleanNode('nullable')->defaultTrue()->end()
                            ->booleanNode('primary_key')->defaultFalse()->end()
                            ->booleanNode('distribution_key')->defaultFalse()->end()
                            ->scalarNode('description')->end()
                            ->variableNode('metadata')->end()
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
                ->ifTrue(fn($values) => !empty($values['schema']) && !empty($values['columns']))
                ->thenInvalid('Only one of "schema" or "columns" can be defined.')
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
                if (!empty($v['schema']) && !empty($v['distribution_key'])) {
                    throw new InvalidConfigurationException(
                        'Only one of "distribution_key" or "schema[].distribution_key" can be defined.',
                    );
                }
                return $v;
            })->end()
            ->validate()
            ->always(function ($v) {
                if (empty($v['delete_where'])) {
                    unset($v['delete_where']);
                }
                return $v;
            })
            ->end()
            ->validate()
            ->ifTrue(fn($values) =>
                isset($values['delete_where_column']) && $values['delete_where_column'] !== '' &&
                isset($values['delete_where']))
            ->thenInvalid('Only one of "delete_where_column" or "delete_where" can be defined.')
            ->end()
            ;
        // BEFORE MODIFICATION OF THIS CONFIGURATION, READ AND UNDERSTAND
        // https://keboola.atlassian.net/wiki/spaces/ENGG/pages/3283910830/Job+configuration+validation
    }

    private static function getStringNormalizer(): Closure
    {
        return function ($v) {
            if (is_bool($v)) {
                return $v ? 'true' : 'false';
            }
            if (is_scalar($v)) {
                return (string) $v;
            }
            return $v;
        };
    }
}
