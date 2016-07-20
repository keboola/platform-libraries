<?php

namespace Keboola\InputMapping\Configuration;

use Symfony\Component\Config\Definition\ConfigurationInterface;
use Symfony\Component\Config\Definition\Builder\TreeBuilder;
use Symfony\Component\Config\Definition\Processor;

class Configuration implements ConfigurationInterface
{

    /**
     * @return TreeBuilder
     */
    public function getConfigTreeBuilder()
    {
        $treeBuilder = new TreeBuilder();
        $treeBuilder->root("configuration");
        return $treeBuilder;
    }

    /**
     *
     * Shortcut method for processing configurations
     *
     * @param $configurations
     * @return array
     */
    public function parse($configurations)
    {
        $processor = new Processor();
        $definition = new static();
        return $processor->processConfiguration($definition, $configurations);
    }
}
