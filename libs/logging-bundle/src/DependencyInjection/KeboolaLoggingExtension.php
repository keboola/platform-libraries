<?php

declare(strict_types=1);

namespace Keboola\LoggingBundle\DependencyInjection;

use Keboola\LoggingBundle\Monolog\DataDogContextProcessor;
use Symfony\Component\DependencyInjection\ContainerBuilder;
use Symfony\Component\DependencyInjection\Definition;
use Symfony\Component\HttpKernel\DependencyInjection\Extension;

class KeboolaLoggingExtension extends Extension
{
    public function load(array $configs, ContainerBuilder $container): void
    {
        $this->configureDataDogProcessor($container);
    }

    private function configureDataDogProcessor(ContainerBuilder $container): void
    {
        if (!function_exists('DDTrace\current_context')) {
            return;
        }

        $definition = new Definition(DataDogContextProcessor::class);
        $definition->addTag('monolog.processor');

        $container->setDefinition('keboola.logging.datadog_context_processor', $definition);
    }
}
