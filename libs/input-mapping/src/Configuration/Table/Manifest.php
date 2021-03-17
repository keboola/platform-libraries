<?php

namespace Keboola\InputMapping\Configuration\Table;

use Keboola\InputMapping\Configuration\Configuration;
use Symfony\Component\Config\Definition\Builder\NodeDefinition;
use Symfony\Component\Config\Definition\Builder\TreeBuilder;

class Manifest extends Configuration
{
    public function getConfigTreeBuilder()
    {
        $treeBuilder = new TreeBuilder();
        $root = $treeBuilder->root("table");
        self::configureNode($root);
        return $treeBuilder;
    }

    public static function configureNode(NodeDefinition $node)
    {
        $node
            ->children()
                ->scalarNode("id")->isRequired()->end()
                ->scalarNode("name")->end()
                ->scalarNode("uri")->end()
                ->arrayNode("primary_key")->prototype("scalar")->end()->end()
                ->arrayNode("distribution_key")->prototype("scalar")->end()->end()
                ->scalarNode("created")->end()
                ->scalarNode("last_import_date")->end()
                ->scalarNode("last_change_date")->end()
                ->arrayNode("columns")->prototype("scalar")->end()->end()
                ->arrayNode("s3")->children()
                    ->booleanNode("isSliced")->end()
                    ->scalarNode("region")->end()
                    ->scalarNode("bucket")->end()
                    ->scalarNode("key")->end()
                    ->arrayNode("credentials")->children()
                        ->scalarNode("access_key_id")->end()
                        ->scalarNode("secret_access_key")->end()
                        ->scalarNode("session_token")->end()
                    ->end()->end()
                ->end()->end()
                ->arrayNode("abs")->children()
                    ->booleanNode("is_sliced")->end()
                    ->scalarNode("region")->end()
                    ->scalarNode("container")->end()
                    ->scalarNode("name")->end()
                    ->arrayNode("credentials")->children()
                        ->scalarNode("sas_connection_string")->end()
                        ->scalarNode("expiration")->end()
                    ->end()->end()
                ->end()->end()
                ->arrayNode("metadata")
                    ->prototype('array')
                        ->children()
                            ->scalarNode("key")->isRequired()->end()
                            ->scalarNode("value")->isRequired()->end()
                            ->scalarNode("id")->isRequired()->end()
                            ->scalarNode("provider")->isRequired()->end()
                            ->scalarNode("timestamp")->isRequired()->end()
                        ->end()
                    ->end()
                    ->defaultValue([])
                ->end()
                ->arrayNode("column_metadata")
                    ->useAttributeAsKey("name")
                    ->prototype('array')
                        ->prototype("array")
                            ->children()
                                ->scalarNode("key")->end()
                                ->scalarNode("value")->end()
                                ->scalarNode("id")->end()
                                ->scalarNode("provider")->end()
                                ->scalarNode("timestamp")->end()
                            ->end()
                        ->end()
                    ->end()
                    ->defaultValue([])
                ->end()
            ->end()
        ;
    }
}
