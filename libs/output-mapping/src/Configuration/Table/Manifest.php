<?php

declare(strict_types=1);

namespace Keboola\OutputMapping\Configuration\Table;

use Keboola\OutputMapping\Configuration\Configuration;
use Symfony\Component\Config\Definition\Builder\ArrayNodeDefinition;
use Symfony\Component\Config\Definition\Builder\TreeBuilder;
use Symfony\Component\Config\Definition\Exception\InvalidConfigurationException;

class Manifest extends BaseConfiguration
{
    public static function configureNode(ArrayNodeDefinition $node): void
    {
        // BEFORE MODIFICATION OF THIS CONFIGURATION, READ AND UNDERSTAND
        // https://keboola.atlassian.net/wiki/spaces/ENGG/pages/3283910830/Job+configuration+validation
        parent::configureNode($node);
        $node->children()->scalarNode('manifest_type')->end()->end();
    }

    public function getConfigTreeBuilder(): TreeBuilder
    {
        $treeBuilder = new TreeBuilder('table');
        $root = $treeBuilder->getRootNode();
        self::configureNode($root);
        return $treeBuilder;
    }
}
