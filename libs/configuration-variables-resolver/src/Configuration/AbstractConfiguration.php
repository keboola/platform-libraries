<?php

declare(strict_types=1);

namespace Keboola\ConfigurationVariablesResolver\Configuration;

use Symfony\Component\Config\Definition\ConfigurationInterface;
use Symfony\Component\Config\Definition\Processor;

abstract class AbstractConfiguration implements ConfigurationInterface
{
    public function process(array $jobData): array
    {
        $processor = new Processor();
        return $processor->processConfiguration(new static(), [$jobData]);
    }
}
