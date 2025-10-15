<?php

declare(strict_types=1);

namespace Keboola\PhpTestUtils\Tests\Fixtures;

use Monolog\Logger;
use Symfony\Bundle\FrameworkBundle\FrameworkBundle;
use Symfony\Bundle\FrameworkBundle\Kernel\MicroKernelTrait;
use Symfony\Component\DependencyInjection\ContainerBuilder;
use Symfony\Component\HttpKernel\DependencyInjection\LazyLoadingValueHolderFactory;
use Symfony\Component\HttpKernel\Kernel;
use Symfony\Component\HttpKernel\KernelInterface;
use Symfony\Component\Routing\Loader\Configurator\RoutingConfigurator;

class TestKernel extends Kernel implements KernelInterface
{
    use MicroKernelTrait;

    public function registerBundles(): iterable
    {
        return [
            new FrameworkBundle(),
        ];
    }

    protected function configureContainer(ContainerBuilder $container): void
    {
        // minimal framework configuration
        $container->loadFromExtension('framework', [
            'secret' => 'S0ME_SECRET',
            'http_method_override' => false,
            'test' => true,
            'router' => [
                'utf8' => true,
            ],
            'session' => [
                'enabled' => false,
            ],
        ]);

        // simple logger service used by FixtureAwareTestCase
        $container->register('monolog.logger', Logger::class)
            ->addArgument('test')
            ->setPublic(true);
    }

    protected function configureRoutes(RoutingConfigurator $routes): void
    {
        // no routes needed for these tests
    }
}
