<?php

declare(strict_types=1);

namespace Keboola\OutputMapping\Configuration;

use Symfony\Component\Config\Definition\ConfigurationInterface;
use Symfony\Component\Config\Definition\Processor;

abstract class Configuration implements ConfigurationInterface
{
    final public function __construct()
    {
    }

    public function parse(array $configurations): array
    {
        $processor = new Processor();
        $definition = new static();
        return $processor->processConfiguration($definition, $configurations);
    }
}
