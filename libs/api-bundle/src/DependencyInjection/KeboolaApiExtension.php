<?php

declare(strict_types=1);

namespace Keboola\ApiBundle\DependencyInjection;

use Keboola\ApiBundle\Security\AttributeAuthenticator;
use PHPStan\BetterReflection\Reflection\Adapter\ReflectionClass;
use Symfony\Component\DependencyInjection\ContainerBuilder;
use Symfony\Component\DependencyInjection\Definition;
use Symfony\Component\HttpKernel\DependencyInjection\Extension;
use Symfony\Component\Routing\Route;

class KeboolaApiExtension extends Extension
{
    public function load(array $configs, ContainerBuilder $container): void
    {
        $this->configureDataDogProcessor($container);
    }

    private function configureDataDogProcessor(ContainerBuilder $container): void
    {
        foreach ($container->getServiceIds() as $serviceId) {
            $reflectionClass = new ReflectionClass($container->get($serviceId));
            $attributes = $reflectionClass->getAttributes();
            $hasRoute = false;
            foreach ($attributes as $attribute) {
                if ($attribute->getName() === Route::class) {
                    $hasRoute = true;
                }
            }
            $hasAuth = false;
            if ($hasRoute) {
                foreach ($attributes as $attribute) {
                    if ($attribute->getName() === AttributeAuthenticator::class) {
                        $hasAuth = true;
                    }
                }
            }
            if (!$hasAuth) {
                throw new \Exception('Invalid ctrl auth' . $serviceId);
            }
        }

        //$definition = new Definition(DataDogContextProcessor::class);
        //$definition->addTag('monolog.processor');

        //$container->setDefinition('keboola.logging.datadog_context_processor', $definition);
    }
}
