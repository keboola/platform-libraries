<?php

declare(strict_types=1);

namespace Keboola\OutputMapping\Configuration\Table;

use Symfony\Component\Config\Definition\Builder\ArrayNodeDefinition;
use Symfony\Component\Config\Definition\Builder\TreeBuilder;

class Manifest extends BaseConfiguration
{
    public static function configureNode(ArrayNodeDefinition $node): void
    {
        // BEFORE MODIFICATION OF THIS CONFIGURATION, READ AND UNDERSTAND
        // https://github.com/keboola/platform-libraries/blob/main/docs/job-configuration.md
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
